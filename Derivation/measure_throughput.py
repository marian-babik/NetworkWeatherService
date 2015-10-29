#!/usr/bin/env python

from elasticsearch import Elasticsearch
import threading
from threading import Thread
import subprocess, Queue, os, sys, time
import math

debug = 0
if len(sys.argv) == 2:
    date = "2015-10-%d" % int(sys.argv[1])
else:
    date = "2015-10-19"
nw_index = "network_weather-" + date

def max_throughput(time_a, time_b, mean_packet_loss):
    # Expected TCP segment size limit: 1500 octets
    mean_segment_size = 1500
    round_trip_time = time_a + time_b
    if mean_packet_loss == 0:
        mean_packet_loss = 0.000001 # Assume no packet loss
    # Formula given by https://en.wikipedia.org/wiki/TCP_tuning#Packet_loss
    print "\tRTT = %f" % round_trip_time
    return mean_segment_size / (round_trip_time * math.sqrt(mean_packet_loss))

import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')

num_threads = 1
queue = Queue.Queue()

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

res = es.search(index=nw_index, body=usrc, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    if tag['key'] == "WT2":
        usrcs.append(tag['key'])

res = es.search(index=nw_index, body=udest, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    if tag['key'] == "BNL-ATLAS":# or tag['key'] == "WT2
        udests.append(tag['key'])

# Dictionary of source IP - destination IP pairs
sd_dict = {}
# Put the sources and destinations in the queue
for s_name in usrcs[:40]:
    for d_name in udests[:40]:
        if s_name == d_name: continue
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

            if hit['_type'] == 'latency':
                node_table[table_index]['latency'][src] = dst
                num_sd_delay += 1
                tot_sd_delay += hit['_source']['@message']['delay_mean']

            if hit['_type'] == 'throughput':
                node_table[table_index]['throughput'][src] = dst
                num_tp += 1
                tot_tp += hit['_source']['@message']['throughput']

        table_index = "%s <--> %s" % (d_name, s_name)
        node_table[table_index] = {}
        node_table[table_index]['packet_loss'] = {}
        node_table[table_index]['latency'] = {}
        node_table[table_index]['throughput'] = {}

        for hit in res_rev['hits']['hits']:
            src = hit['_source']['@message']['src']
            dst = hit['_source']['@message']['dest']

            # if hit['_type'] == 'packet_loss_rate':
            #     node_table[table_index]['packet_loss'][src] = dst
            #     num_pl += 1
            #     tot_pl += hit['_source']['@message']['packet_loss']

            if hit['_type'] == 'latency':
                node_table[table_index]['latency'][src] = dst
                num_ds_delay += 1
                tot_ds_delay += hit['_source']['@message']['delay_mean']

            # if hit['_type'] == 'throughput':
            #     node_table[table_index]['throughput'][src] = dst
            #     num_tp += 1
            #     tot_tp += hit['_source']['@message']['throughput']

        if num_sd_delay > 0: avg_sd_delay = tot_sd_delay / num_sd_delay
        if num_ds_delay > 0: avg_ds_delay = tot_ds_delay / num_ds_delay
        if num_pl > 0: avg_pl = tot_pl / num_pl
        if num_tp > 0: avg_tp = tot_tp / num_tp

        if num_sd_delay > 0 and num_ds_delay > 0 and num_pl > 0:

            print "[%s %s]" % (date, table_index)
            pre_tp = max_throughput(avg_sd_delay, avg_ds_delay, avg_pl)

            if num_tp > 0:
                print "\t(%f\t / %f)\t =\t %f" % (avg_tp, pre_tp, (avg_tp / pre_tp))
                sys.stdout.flush()
                os._exit(1)


for i in range(num_threads):
    thread = Thread(target = get_throughputs)
    thread.daemon = True
    thread.start()

queue.join()

print "All done."
