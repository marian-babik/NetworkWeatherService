#!/usr/bin/env python

import siteMapping

import Queue, os, sys, time
import threading
from threading import Thread
import requests
import copy
import json
from datetime import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers

import stomp

allhosts=[]
allhosts.append([('128.142.36.204',61513)])
allhosts.append([('188.185.227.50',61513)])
# topic = '/topic/perfsonar.packet-trace'
topic = '/topic/perfsonar.raw.packet-trace'

siteMapping.reload()

lastReconnectionTime=0

class MyListener(object):
    def on_error(self, headers, message):
        print('received an error %s' % message)
    def on_message(self, headers, message):
        q.put(message)


def GetESConnection(lastReconnectionTime):
    if ( time.time()-lastReconnectionTime < 60 ): 
        return
    lastReconnectionTime=time.time()
    print("make sure we are connected right...")
    res = requests.get('http://cl-analytics.mwt2.org:9200')
    print(res.content)
    
    es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])
    return es

def eventCreator():
    tries=0
    aLotOfData=[]
    while(True):
        d=q.get()
        m=json.loads(d)
        
        data = {
            '_type': 'traceroute'
        }
        print(m)
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
        if not 'datapoints' in m: 
            q.task_done()
            print(threading.current_thread().name, "no datapoints found in the message")
            continue
        dp=m['datapoints']
        # print(su)
        for ts in dp:
            dati=datetime.utcfromtimestamp(r[0]).isoformat()
            data['_index']="network_weather_2-"+str(dati.year)+"."+str(dati.month)+"."+str(dati.day)
            data['timestamp']=dati
            data['hops']=[]
            hops = dp[ts]
            for hop in hops:
                if hop['ip'] == None: continue
                if hop['ip'] not in data['hops']:
					data['hops'].append(hop['ip'])
                    # print(data)
            data['hash']=hash("".join(data['hops']))
            aLotOfData.append(copy.copy(data))
        q.task_done()
        

        if tries%10==1:
            es = GetESConnection(lastReconnectionTime)
            
        if len(aLotOfData)>500:
            tries += 1
            try:
                res = helpers.bulk(es, aLotOfData, raise_on_exception=False,request_timeout=60)
                print(threading.current_thread().name, "\t inserted:",res[0], '\tErrors:',res[1])
                aLotOfData=[]
                tries = 0
            except es_exceptions.ConnectionError as e:
                print('ConnectionError ', e)
            except es_exceptions.TransportError as e:
                print('TransportError ', e)
            except helpers.BulkIndexError as e:
                print(e[0])
                # for i in e[1]:
                    # print(i)
            except:
                print('Something seriously wrong happened.')

passfile = open('/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/.passfile')
passwd=passfile.read()

es = GetESConnection(lastReconnectionTime)
while (not es):
    es = GetESConnection(lastReconnectionTime)

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
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"qsize:", q.qsize()) 
    time.sleep(60)
