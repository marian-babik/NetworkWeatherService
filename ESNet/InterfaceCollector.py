#!/usr/bin/env python

import os, sys, time
import threading
from threading import Thread
import requests

import json
from datetime import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers

lastReconnectionTime=0

class interface:
    def __init__(self, name, has_flow, tags):
        self.name=name
        self.has_flow=has_flow
        self.tags=tags
        self.lastupdate=int(time.time()*1000)
    def prnt(self):
        print ('interface: ', self.name, '\tflow: ', self.has_flow, '\t tags:', self.tags)
        
def getInterfaces():
    interfaces=[]
    print("Getting interfaces...")
    try:
        req = requests.get("https://my.es.net/api/v1/network_entity/?format=json")
        if (req.status_code>299):
            print ("problem in getting interfaces. status code: ", req.status_code)
        else: 
            res=req.json()
            for s in res["objects"]:
                #print(s["short_name"], s["has_flow"], s["tags"])
                interfaces.append(interface(s["short_name"],s["has_flow"],s["tags"]))
    except:
        print ("Unexpected error:", sys.exc_info()[0])
    print ("Done.")
    for i in interfaces:
        i.prnt()
    return interfaces
    
def getData(i):
    print ("Loading data for: ",i.name)
    currenttime=int(time.time()*1000)
    link="https://my.es.net/api/v1/network_entity_interface/"
    link+=i.name+'/?'
    link+="end_time="+str(currenttime) + '&'
    link+="start_time="+str(i.lastupdate)
    link+='&frequency=30000&format=json'
    i.lastupdate = currenttime
    res=[]
    try:
        #print(link)
        req = requests.get(link)
        print (i.name, req.status_code)
        j=req.json()
        
        d = datetime.now()
        ind="esnet-"+str(d.year)+"."+str(d.month)+"."+str(d.day)
        data = {
            '_index': ind,
            '_type': 'interface',
            'site': i.name.replace("ATLAS-","")
        }

        for s in j["objects"]:
            data['device']=s["device"]
            data['interface']=s["interface"]
            data['description']=s["description"]
            chin=s["channels"]["in"]["samples"]
            for sample in chin:
                data['timestamp']=sample[0]
                data['direction']='in'
                data['rate']=sample[1]
                res.append(data.copy())
            chout=s["channels"]["out"]["samples"]
            for sample in chout:
                data['timestamp']=sample[0]
                data['direction']='out'
                data['rate']=sample[1]
                res.append(data.copy())
        return res
            
    except:
        print ("Unexpected error:", sys.exc_info()[0])
    return res 

def GetESConnection(lastReconnectionTime):
    if ( time.time()-lastReconnectionTime < 60 ): 
        return
    lastReconnectionTime=time.time()
    print "make sure we are connected right..."
    res = requests.get('http://cl-analytics.mwt2.org:9200')
    print(res.content)
    
    es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])
    return es


def loader(i):
    print ("starting a thread for ", i.name)
    while(True):
        aLotOfData=getData(i)
        try:
            res = helpers.bulk(es, aLotOfData, raise_on_exception=True)
            print threading.current_thread().name, "\t inserted:",res[0], '\tErrors:',res[1]
            aLotOfData=[]
        except es_exceptions.ConnectionError as e:
            print 'ConnectionError ', e
        except es_exceptions.TransportError as e:
            print 'TransportError ', e
        except helpers.BulkIndexError as e:
            print e[0]
            for i in e[1]:
                print i
        except:
            print 'Something seriously wrong happened. '
        time.sleep(900)

es = GetESConnection(lastReconnectionTime)
while (not es):
    es = GetESConnection(lastReconnectionTime)

interfaces=getInterfaces()
#staggered start loaders threads
for i in interfaces:
     time.sleep(20)
     t = Thread(target=loader,args=(i,))
     t.daemon = True
     t.start()

while(True):
    print "All OK ..."
    time.sleep(900)