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
                        "or": [
                            {
                                "term":{ "@message.srcSite":s }
                            },
                            {
                                "term":{ "@message.destSite":d }
                            }
                        ]
                    }
                }
            }
        }

        queue.put([st, s, d])


def get_throughputs():
    while True:
        st_data = queue.get()
        st = st_data[0]
        s = st_data[1]
        d = st_data[2]

        res = es.search(index=nw_index, body=st, size=1000)

        print res


for i in range(num_threads):
    thread = Thread(target = get_throughputs)
    thread.daemon = True
    thread.start()

queue.join()

print "All done."
