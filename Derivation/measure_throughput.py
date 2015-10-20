#!/usr/bin/env python

from elasticsearch import Elasticsearch
import threading
from threading import Thread
import subprocess, Queue, os, sys, time
import math

if len(sys.argv) == 2:
    debug = (1 if sys.argv[1] == "d" else 0)
else: debug = 0

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
print "documents to look into:"
print es.count(index=nw_index)

res = es.search(index=nw_index, body=usrc, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    if tag['key'] == "WT2":# or tag['key'] == "BNL-ATLAS":
        usrcs.append(tag['key'])

res = es.search(index=nw_index, body=udest, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    if tag['key'] == "BNL-ATLAS":# or tag['key'] == "WT2
        udests.append(tag['key'])

print "unique sources: ", len(usrcs)
print "unique destinations: ", len(udests)

# Dictionary of source IP - destination IP pairs
sd_dict = {}
# Put the sources and destinations in the queue
for s_name in usrcs[:40]:
    for d_name in udests[:40]:
        if s_name == d_name: continue
        print "source: ", s_name
        print "destination: ", d_name
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

        queue.put([st, s_name, d_name])


def get_throughputs():
    while True:
        st_data = queue.get()
        st = st_data[0]
        s_name = st_data[1]
        d_name = st_data[2]
        res = es.search(index=nw_index, body=st, size=1000)
        print "source: %s\tdest: %s" % (s_name, d_name)

        # Print the IP addresses of these as well

        for hit in res['hits']['hits']:
            src = hit['_source']['@message']['src']
            dst = hit['_source']['@message']['dest']

            if hit['_type'] == 'packet_loss_rate':
                print "packet_loss (%s-%s)" % (src, dst)
            if hit['_type'] == 'latency':
                print "latency (%s-%s)" % (src, dst)
            if hit['_type'] == 'throughput':
                print "throughput (%s-%s)" % (src, dst)


for i in range(num_threads):
    thread = Thread(target = get_throughputs)
    thread.daemon = True
    thread.start()

queue.join()

print "All done."
