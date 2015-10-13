#!/usr/bin/env python

from elasticsearch import Elasticsearch
import threading
from threading import Thread
import subprocess, Queue, os, sys, time

def max_throughput(time_a, time_b, mean_packet_loss):
    # Expected TCP segment size limit: 1500 octets
    mean_segment_size = 1500
    round_trip_time = time_a + time_b
    # Formula given by https://en.wikipedia.org/wiki/TCP_tuning#Packet_loss
    return mean_segment_size / (round_trip_time * math.sqrt(mean_packet_loss))


num_threads = 1
lock = threading.lock()
queue = Queue.Queue()

nw_index = "network_weather-2015-10-11"
usrc = {
    "size": 0,
    "aggregations": {
       "unique_vals": {
          "terms": {
             "field": "@message.src",
             "size":1000
          }
       }
    }
}
udest = {
    "size": 0,
    "aggregations": {
       "unique_vals": {
          "terms": {
             "field": "@message.dest",
             "size":1000
          }
       }
    }
}
usrcs = []
udests = []
es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])
print "documents to look into:"
print es.count(index=nw_index)

res = es.search(index="network_weather-2015-10-11", body=usrc, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    usrcs.append(tag['key'])

res = es.search(index="network_weather-2015-10-11", body=udest, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    udests.append(tag['key'])

print "unique sources: ", len(usrcs)
print "unique destinations: ", len(udests)

# Dictionary of source IP - destination IP pairs
sd_dict = {}
# Put the sources and destinations in the queue
for s in usrcs:
    for d in udests:
        if s == d: continue
        print "source: ", s
        print "destination: ", d
        st={
        "query": {
                "filtered":{
                    "query": {
                        "match_all": {}
                    },
                    "filter":{
                        "and": [
                            {
                                "term":{ "@message.src":s }
                            },
                            {
                                "term":{ "@message.dest":d }
                            }
                        ]
                    }
                }
            }
        }

        st_rev={
        "query": {
                "filtered":{
                    "query": {
                        "match_all": {}
                    },
                    "filter":{
                        "and": [
                            {
                                "term":{ "@message.src":d }
                            },
                            {
                                "term":{ "@message.dest":s }
                            }
                        ]
                    }
                }
            }
        }

        sd_dict[s] = d
        sd_dict[d] = s
        queue.put([st, st_rev])


def get_all_throughputs():
    while True:
        st_pair = queue.get()
        st = st_pair[0]
        st_rev = st_pair[1]

        res = es.search(index=nw_index, body=st, size=1000)
        res_rev = es.search(index=nw_index, body=st_rev, size=1000)

        if res and res_rev and (not sd_dict[s]) and (not sd_dict[d]):
            # Both source-dest and dest-source result dictionaries are
            # nonempty and the source-destination pairs are not already
            # in the dictionary

            num_sd_delay = 0
            tot_sd_delay = 0
            num_ds_delay = 0
            tot_ds_delay = 0
            num_pl = 0
            tot_pl = 0

            # Calculate simple averages
            for sd_hit in res['hits']['hits']:
                if sd_hit['_type'] == 'packet_loss_rate':
                    num_pl += 1
                    tot_pl += sd_hit['_source']['@message']['packet_loss']
                elif sd_hit['_type'] == 'latency':
                    num_sd_delay += 1
                    tot_sd_delay += sd_hit['_source']['@message']['delay_mean']

            for ds_hit in res_rev['hits']['hits']:
                if ds_hit['_type'] == 'packet_loss_rate':
                    num_pl += 1
                    tot_pl += ds_hit['_source']['@message']['packet_loss']
                elif sd_hit['_type'] == 'latency':
                    num_ds_delay += 1
                    tot_ds_delay += ds_hit['_source']['@message']['delay_mean']

            avg_sd_delay = tot_sd_delay / num_sd_delay
            avg_ds_delay = tot_ds_delay / num_ds_delay
            avg_pl = tot_pl / num_pl

            tp = max_throughput(avg_sd_delay, avg_ds_delay, avg_pl)
            print 'max throughput for source-destination pair (%s - %s):\t %f' % (s, d, tp)

            queue.task_done()

for i in range(num_threads):
    thread = Thread(target = get_all_throughputs)
    thread.daemon = True
    thread.start()

queue.join()

print "All done."
