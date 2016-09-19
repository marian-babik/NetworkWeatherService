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
topic = '/topic/perfsonar.summary.histogram-owdelay'
#topic = '/topic/perfsonar.histogram-owdelay'

siteMapping.reload()

conns = []

lastReconnectionTime=0

#mids={}
class MyListener(object):
    def on_message(self, headers, message):
        q.put(message)
    def on_error(self, headers, message):
        print('received an error %s' % message)
    def on_heartbeat_timeout(self):
        print ('lost heartbeat. Needs a reconnect!')
        conn.disconnect()
    def on_disconnected(self):
        print ('no connection. Needs a reconnect!')
#        id=headers['message-id']
#        if id in mids:
#            print (headers, message)
#        else:
#            mids[id]=True

def connectToAMQ(conns):
    for conn in conns:
        if conn:
            conn.disconnect()
    conns=[]
    for host in allhosts:
        conn = stomp.Connection(host, user='psatlflume', passcode=passwd.strip() )
        conn.set_listener('MyConsumer', MyListener())
        conn.start()
        conn.connect()
        conn.subscribe(destination = topic, ack = 'auto', id="1", headers = {})    
        conns.append(conn)

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
    aLotOfData=[]
    tries=0
    while(True):
        d=q.get()
        m=json.loads(d)
        
        data = {
            '_type': 'latency'
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
            print(threading.current_thread().name, "no summaries found in the message")
            continue
        su=m['summaries']
        for s in su:
            if s['summary_window']=='300' and s['summary_type']=='statistics':
                results=s['summary_data']
                #print(results)
                for r in results:
                    dati=datetime.utcfromtimestamp(float(r[0]))
                    data['_index']="network_weather_2-"+str(dati.year)+"."+str(dati.month)+"."+str(dati.day)
                    data['timestamp']=r[0]*1000
                    data['delay_mean']=r[1]['mean']
                    data['delay_median']=r[1]['median']
                    data['delay_sd']=r[1]['standard-deviation']
                    #print(data)
                    aLotOfData.append(copy.copy(data))
        q.task_done()
        if tries%10==1:
            es = GetESConnection(lastReconnectionTime)
        if len(aLotOfData)>500:
            # print('writing out data...')
            tries += 1
            try:
                res = helpers.bulk(es, aLotOfData, raise_on_exception=True,request_timeout=60)
                print(threading.current_thread().name, "\t inserted:",res[0], '\tErrors:',res[1])
                aLotOfData=[]
                tries=0
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

connectToAMQ(conns)

q=Queue.Queue()
#start eventCreator threads
for i in range(3):
     t = Thread(target=eventCreator)
     t.daemon = True
     t.start()



while(True):
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "qsize:", q.qsize())
    for conn in conns:
        if not conn.is_connected():
            print ('problem with connection. try reconnecting...')
            connectToAMQ(conns)
            break
    time.sleep(60)
