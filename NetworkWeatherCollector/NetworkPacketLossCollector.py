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
topic = '/topic/perfsonar.packet-loss-rate'

siteMapping.reload()

class MyListener(object):
    def on_error(self, headers, message):
        print 'received an error %s' % message
    def on_message(self, headers, message):
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
            '_type': 'packet_loss_rate'
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
        data['srcProduction']=siteMapping.isProductionLatency(source)
        data['destProduction']=siteMapping.isProductionLatency(destination)
        if not 'summaries'in m: 
            q.task_done()
            print threading.current_thread().name, "no summaries found in the message"
            continue
        su=m['summaries']
        # print su
        for s in su:
            if s['summary_window']=='300':
                results = s['summary_data']
                for r in results:
                    data['timestamp']=datetime.utcfromtimestamp(r[0]).isoformat()
                    data['packet_loss']=r[1]
                    # print data
                    aLotOfData.append(data)
        q.task_done()
        if len(aLotOfData)>100:
            res = helpers.bulk(es, aLotOfData)
            print threading.current_thread().name, "\t inserted:",res[0], '\tErrors:',res[1]
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
for i in range(3):
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
