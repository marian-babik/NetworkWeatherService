#!/usr/bin/env python

import siteMapping

import Queue, os, sys, time
import threading
from threading import Thread
import urllib2

import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

import stomp

allhosts=[]
allhosts.append([('128.142.36.204',61513)])
allhosts.append([('188.185.227.50',61513)])
topic = '/topic/perfsonar.throughput'

siteMapping.reload()

class MyListener(object):
    def on_error(self, headers, message):
        print 'received an error %s' % message
    def on_message(self, headers, message):
        # print message
        q.put(message)

def eventCreator():
    aLotOfData=[]
    while(True):
        d=q.get()
        m=json.loads(d)
        
        d = datetime.now()
        ind="network_weather_2-"+str(d.year)+"."+str(d.month)+"."+str(d.day)
        data = {
            '_index': ind,
            '_type': 'throughput'
        }
        
        source=m['meta']['source']
        destination=m['meta']['destination']
        data['MA']=m['meta']['measurement_agent']
        data['src']=source
        data['dest']=destination
        so=siteMapping.getPS(source)
        de=siteMapping.getPS(destination)
        if so!= None:
            data['srcSite']=so[0]
            data['srcVO']=so[1]
        if de!= None:
            data['destSite']=de[0]
            data['destVO']=de[1]
        data['srcProduction']=siteMapping.isProductionThroughput(source)
        data['destProduction']=siteMapping.isProductionThroughput(destination)
        if not 'datapoints'in m:
            print 'no datapoints in this message!'
            q.task_done()
            continue
        su=m['datapoints']
        for ts, th in su.iteritems():
            data['timestamp']=datetime.utcfromtimestamp(int(ts)).isoformat()
            data['throughput']=th
            #print data
            aLotOfData.append(data)
        q.task_done()
        if len(aLotOfData)>100:
            res = helpers.bulk(es, aLotOfData)
            print res
            aLotOfData=[]

passfile = open('/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/.passfile')
passwd=passfile.read()



print "make sure we are connected right..."
import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')
print(res.content)

es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])


q=Queue.Queue()
#start eventCreator threads
for i in range(1):
     t = Thread(target=eventCreator)
     t.daemon = True
     t.start()

for host in allhosts:
    conn = stomp.Connection(host, user='psatlflume', passcode=passwd.strip() )
    conn.set_listener('MyConsumer', MyListener())
    conn.start()
    conn.connect()
    conn.subscribe(destination = topic, ack = 'auto', id="1", headers = {})

while(True):
    print "qsize:", q.qsize()
    time.sleep(60)
