#!/usr/bin/env python

from elasticsearch import Elasticsearch
import threading
from threading import Thread
import subprocess, Queue, os, sys, time
import math

debug = 0
if len(sys.argv) == 2:
    nw_index = "network_weather-2015-10-%d" % int(sys.argv[1])
else:
    nw_index = "network_weather-2015-10-19"
# if len(sys.argv) == 2:
#     debug = (1 if sys.argv[1] == "d" else 0)
# else: debug = 0

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

# nw_indices = []
# i = 19
# while i < 26:
#     nw_indices.append("network_weather-2015-10-%d" % i)
#     i += 1

# print nw_indices

# nw_index = "network_weather-2015-10-19"
usrc = {
    "size": 0,
    "aggregations": {
       "unique_vals": {
          "terms": {
             "field": "@message.srcSite",
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
             "field": "@message.destSite",
             "size":1000
          }
       }
    }
}
usrcs = []
udests = []
es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])
# print "documents to look into:"
# print es.count(index=nw_index)

res = es.search(index=nw_index, body=usrc, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    if tag['key'] == "WT2":# or tag['key'] == "BNL-ATLAS":
        usrcs.append(tag['key'])

res = es.search(index=nw_index, body=udest, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    if tag['key'] == "BNL-ATLAS":# or tag['key'] == "WT2
        udests.append(tag['key'])

# print "unique sources: ", len(usrcs)
# print "unique destinations: ", len(udests)

# Dictionary of source IP - destination IP pairs
sd_dict = {}
# Put the sources and destinations in the queue
for s_name in usrcs[:40]:
    for d_name in udests[:40]:
        if s_name == d_name: continue
        # print "source: ", s_name
        # print "destination: ", d_name
        st={
        "query": {
                "filtered":{
                    "query": {
                        "match_all": {}
                    },
                    "filter":{
                        "and": [
                            {
                                "term":{ "@message.srcSite":s_name }
                            },
                            {
                                "term":{ "@message.destSite":d_name }
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
                                "term":{ "@message.srcSite":d_name }
                            },
                            {
                                "term":{ "@message.destSite":s_name }
                            }
                        ]
                    }
                }
            }
        }

        queue.put([st, st_rev, s_name, d_name])


node_table = {}

def get_throughputs():
    while True:
        global node_table
        st_data = queue.get()
        st = st_data[0]
        st_rev = st_data[1]
        s_name = st_data[2]
        d_name = st_data[3]
        res = es.search(index=nw_index, body=st, size=1000)
        res_rev = es.search(index=nw_index, body=st_rev, size=1000)
        # print "source: %s\tdest: %s" % (s_name, d_name)

        table_index = "%s <--> %s" % (s_name, d_name)
        node_table[table_index] = {}
        node_table[table_index]['packet_loss'] = {}
        node_table[table_index]['latency'] = {}
        node_table[table_index]['throughput'] = {}

        num_sd_delay = 0
        num_ds_delay = 0
        num_pl = 0
        num_tp = 0

        tot_sd_delay = 0
        tot_ds_delay = 0
        tot_pl = 0
        tot_tp = 0

        for hit in res['hits']['hits']:
            src = hit['_source']['@message']['src']
            dst = hit['_source']['@message']['dest']

            if hit['_type'] == 'packet_loss_rate':
                node_table[table_index]['packet_loss'][src] = dst
                num_pl += 1
                tot_pl += hit['_source']['@message']['packet_loss']
                # print "packet_loss\t\t(%s  -  %s)" % (src, dst)
            if hit['_type'] == 'latency':
                node_table[table_index]['latency'][src] = dst
                num_sd_delay += 1
                tot_sd_delay += hit['_source']['@message']['delay_mean']
                # print "latency\t\t(%s  -  %s)" % (src, dst)
            if hit['_type'] == 'throughput':
                node_table[table_index]['throughput'][src] = dst
                num_tp += 1
                tot_tp += hit['_source']['@message']['throughput']
                # print "throughput\t\t(%s  -  %s)" % (src, dst)

        # print "node_table[%s]:" % table_index
        # print "\tpacket_loss: %s" % node_table[table_index]['packet_loss']
        # print "\tlatency: %s" % node_table[table_index]['latency']
        # print "\tthroughput: %s" % node_table[table_index]['throughput']
        # print "\n"

        table_index = "%s <--> %s" % (d_name, s_name)
        node_table[table_index] = {}
        node_table[table_index]['packet_loss'] = {}
        node_table[table_index]['latency'] = {}
        node_table[table_index]['throughput'] = {}

        for hit in res_rev['hits']['hits']:
            src = hit['_source']['@message']['src']
            dst = hit['_source']['@message']['dest']

            if hit['_type'] == 'packet_loss_rate':
                node_table[table_index]['packet_loss'][src] = dst
                num_pl += 1
                tot_pl += hit['_source']['@message']['packet_loss']
                # print "packet_loss\t\t(%s  -  %s)" % (src, dst)
            if hit['_type'] == 'latency':
                node_table[table_index]['latency'][src] = dst
                num_ds_delay += 1
                tot_ds_delay += hit['_source']['@message']['delay_mean']
                # print "latency\t\t(%s  -  %s)" % (src, dst)
            if hit['_type'] == 'throughput':
                node_table[table_index]['throughput'][src] = dst
                num_tp += 1
                tot_tp += hit['_source']['@message']['throughput']
                # print "throughput\t\t(%s  -  %s)" % (src, dst)

        # NOTE: Uses the same code we use in maximum_throughput.py
        # avg_sd_delay = 0
        # avg_ds_delay = 0
        # avg_pl = 0
        # avg_tp = 0

        if num_sd_delay > 0: avg_sd_delay = tot_sd_delay / num_sd_delay
        if num_ds_delay > 0: avg_ds_delay = tot_ds_delay / num_ds_delay
        if num_pl > 0: avg_pl = tot_pl / num_pl
        if num_tp > 0: avg_tp = tot_tp / num_tp

        # print "node_table[%s]:" % table_index
        # print "\tpacket_loss: %s" % node_table[table_index]['packet_loss']
        # print "\tlatency: %s" % node_table[table_index]['latency']
        # print "\tthroughput: %s" % node_table[table_index]['throughput']
        # print "\n"

        # print "[%s]" % table_index
        # if num_pl > 0: print "\tpacket_loss: %f" % avg_pl
        # if num_sd_delay > 0 and num_ds_delay > 0: print "\tlatency: %f" % (avg_sd_delay + avg_ds_delay)
        # if num_tp > 0: print "\tthroughput: %f" % avg_tp

        if num_sd_delay > 0 and num_ds_delay > 0 and num_pl > 0:
            pre_tp = max_throughput(avg_sd_delay, avg_ds_delay, avg_pl)
            # print "predirected throughput: %f" % pre_tp
            if num_tp > 0:
                print "[%s %s] ratio of actual to predicted: %f" % (nw_index, table_index, (avg_tp / pre_tp))
                # print "Done with %s" % nw_index
                sys.stdout.flush()
                os._exit(1)


for i in range(num_threads):
    thread = Thread(target = get_throughputs)
    thread.daemon = True
    thread.start()

queue.join()

print "All done."
