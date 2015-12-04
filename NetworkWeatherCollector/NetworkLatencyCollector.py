#!/usr/bin/env python

import sys, time
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

import stomp

from pprint import pprint

global messages
messages=[]

allhosts=[]
allhosts.append([('128.142.36.204',61513)])
allhosts.append([('188.185.227.50',61513)])
topic = '/topic/perfsonar.histogram-owdelay'


#MAKE on_message add the message to a queue and make queue 10 deep
#MAKE a separate thread that uploads ???


class MyListener(object):
    def on_error(self, headers, message):
        print 'received an error %s' % message
    def on_message(self, headers, message):
        #print 'received a message %s' % message
        messages.append(message)

passfile = open('/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/.passfile')
passwd=passfile.read()



print "make sure we are connected right..."
import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')
print(res.content)

es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])





while(True):
    aLotOfData=[]
    messages=[]
    d = datetime.now()
    ind="network_weather_dev-"+str(d.year)+"."+str(d.month)+"."+str(d.day)
    
    data = {
        '_index': ind,
        '_type': 'latency'
    }
    
    for host in allhosts:
        try:
            conn = stomp.Connection(host, user='psatlflume', passcode=passwd.strip() )
            conn.set_listener('MyConsumer', MyListener())
            conn.start()
            conn.connect()
            conn.subscribe(destination = topic, ack = 'auto', id="1", headers = {})
            time.sleep(2)
            conn.disconnect()
        finally:
            print len(messages)
            for message in messages:
                m=json.loads(message)
                source=m['meta']['source']
                destination=m['meta']['destination']
                data['MA']=m['meta']['measurement_agent']
                data['src']=source
                data['dest']=destination
                data['srcSite']=source
                data['srcVO']=source
                data['destSite']=destination
                data['destVO']=destination
                su=m['summaries']
                for s in su:
                    if s['summary_window']=='300' and s['summary_type']=='statistics':
                        results=s['summary_data']
                        # print results
                        for r in results:
                            data['timestamp']=r[0]*1000
                            data['delay_mean']=r[1]['mean']
                            data['delay_median']=r[1]['median']
                            data['delay_sd']=r[1]['standard-deviation']
                            aLotOfData.append(data)
                            # pprint(data)                
            # res = helpers.bulk(es, aLotOfData)
            print "events: ", len(aLotOfData), 
            #print(res)
            aLotOfData=[]
            messages=[]
