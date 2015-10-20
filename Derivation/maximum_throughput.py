#!/usr/bin/env python

from elasticsearch import Elasticsearch
import threading
from threading import Thread
import subprocess, Queue, os, sys, time
import math

if len(sys.argv) == 2:
    debug = (1 if sys.argv[1] == "d" else 0)
else: debug = 0

def max_throughput(time_a, time_b, mean_packet_loss):
    # Expected TCP segment size limit: 1500 octets
    mean_segment_size = 1500
    round_trip_time = time_a + time_b
    if mean_packet_loss == 0:
        mean_packet_loss = 1 # Assume no packet loss
    # Formula given by https://en.wikipedia.org/wiki/TCP_tuning#Packet_loss
    return mean_segment_size / (round_trip_time * math.sqrt(mean_packet_loss))


import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')

num_threads = 1
# lock = threading.Lock()
queue = Queue.Queue()

nw_index = "network_weather-2015-10-19"
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

res = es.search(index=nw_index, body=usrc, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    usrcs.append(tag['key'])

res = es.search(index=nw_index, body=udest, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    udests.append(tag['key'])

print "unique sources: ", len(usrcs)
print "unique destinations: ", len(udests)

# Dictionary of source IP - destination IP pairs
sd_dict = {}
# Put the sources and destinations in the queue
for s in usrcs[:40]:
    for d in udests[:40]:
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

        queue.put([st, st_rev, s, d])


def predict_all_throughputs():
    while True:
        st_data = queue.get()
        st = st_data[0]
        st_rev = st_data[1]
        s = st_data[2]
        d = st_data[3]

        res = es.search(index=nw_index, body=st, size=1000)
        res_rev = es.search(index=nw_index, body=st_rev, size=1000)

        if res and res_rev and (s not in sd_dict) and (d not in sd_dict):
            # Both source-dest and dest-source result dictionaries are
            # nonempty and the source-destination pairs are not already
            # in the dictionary

            num_sd_delay = 0
            tot_sd_delay = 0
            num_ds_delay = 0
            tot_ds_delay = 0
            num_pl = 0
            tot_pl = 0

            # Print debug info
            if debug:
                print "res: " + res.__str__()
                print "res keys: " + res.keys().__str__()
                print "res values: " + res.values().__str__() + "\n"
                print "res_rev: " + res_rev.__str__()
                print "res_rev keys: " + res_rev.keys().__str__()
                print "res_rev values: " + res_rev.values().__str__() + "\n\n"

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

            if num_sd_delay > 0:
                avg_sd_delay = tot_sd_delay / num_sd_delay
                if debug: print "average source-dest latency for pair (%s - %s):\t %f" % (s, d, avg_sd_delay)

            if num_ds_delay > 0:
                avg_ds_delay = tot_ds_delay / num_ds_delay
                if debug: print "average dest-source latency for pair (%s - %s):\t %f" % (s, d, avg_ds_delay)

            if num_pl > 0:
                avg_pl = tot_pl / num_pl
                if debug: print "average packet loss for pair (%s - %s):\t %f" % (s, d, avg_pl)

            if num_sd_delay > 0 and num_ds_delay > 0 and num_pl > 0:
                avg_sd_delay = tot_sd_delay / num_sd_delay
                avg_ds_delay = tot_ds_delay / num_ds_delay
                avg_pl = tot_pl / num_pl

                tp = max_throughput(avg_sd_delay, avg_ds_delay, avg_pl)
                print "max throughput for (%s - %s):\t %f" % (s, d, tp)
                print "\t(actual throughput = __)"
            else:
                print "no delay or packet loss data for source-destination pair (%s - %s)" % (s, d)

            sd_dict[s] = d
            sd_dict[d] = s
            queue.task_done()

for i in range(num_threads):
    thread = Thread(target = predict_all_throughputs)
    thread.daemon = True
    thread.start()

queue.join()

print "All done."
