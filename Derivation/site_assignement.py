#!/usr/bin/env python

# 1 stream               22.87s user 3.27s system 4% cpu 9:39.41 total
# 10 streams             25.15s user 4.63s system 47% cpu 1:02.48 total 7480 requests/second
from datetime import datetime
from elasticsearch import Elasticsearch

import threading
from threading import Thread
import  subprocess, Queue, os, sys,time

import agisFunctions

mapping = agisFunctions.getATLAShosts()

nThreads=1
lock = threading.Lock()
totr=0

def worker():
    global totr
    while True:
        st=q.get()
        res = es.search(index="network_weather-2015-10-11", body=st, size=1000)
        recs=res['hits']['total']
        lock.acquire()
        totr+=recs
        lock.release()
        for rec in res['hits']['hits']:
            es.update(index=rec['_index'],doc_type=rec['_type'],id=rec['_id'], body={"doc": {"srcSite": mapping[rec['@message']['src']],"destSite": mapping[rec['@message']['dest']] }})
        print "records:",recs, "\t remaining:",q.qsize(), "\ttotal rec:",totr
        q.task_done()

print "make sure we are connected right."
import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')
print(res.content)

es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])

print "documents to look into:"
print es.count(index="network_weather-2015-10-11")

usrc={
    "size": 0,
    "aggregations": {
       "unique_vals": {
          "terms": {
             "field": "@message.src",
             "size":5000
          }
       }
    }
}
udest={
    "size": 0,
    "aggregations": {
       "unique_vals": {
          "terms": {
             "field": "@message.dest",
             "size":5000
          }
       }
    }
}
usrcs=[]
udests=[]

res = es.search(index="network_weather-2015-10-11", body=usrc, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    usrcs.append(tag['key'])

res = es.search(index="network_weather-2015-10-11", body=udest, size=10000)
for tag in res['aggregations']['unique_vals']['buckets']:
    udests.append(tag['key'])

print "unique sources: ", len(usrcs)
print "unique destinations: ", len(udests)

q=Queue.Queue()
for i in range(nThreads):
    t = Thread(target=worker)
    t.daemon = True
    t.start()

for s in usrcs:
    for d in udests:
        if not s in mapping and not d in mapping: continue
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
        q.put(st)


q.join()

print 'All done.'
