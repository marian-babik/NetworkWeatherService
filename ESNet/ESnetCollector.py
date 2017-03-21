#!/usr/bin/python

import os, sys, time
import threading
from threading import Thread
import requests

import json
from datetime import datetime
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers

lastReconnectionTime = 0

ESserver = sys.argv[1]
ESport = int(sys.argv[2])
APIkey = sys.argv[3]
url = 'https://my.es.net/graphql_token'
auth = None
timeout=10


if len(sys.argv)>=6:
	auth = (sys.argv[4], sys.argv[5]) #es-atlas, pass
if len(sys.argv)==7:
	timeout = sys.argv[6] #600

class interface:
    def __init__(self, name, has_flow=True, tags=[]):
        self.name=name
        self.has_flow=has_flow
        self.tags=tags
        self.lastInterfaceUpdate = datetime.utcnow()
        self.lastFlowUpdate = datetime.utcnow()
    def prnt(self):
        print ('interface: ', self.name, 'flow: ', self.has_flow, 'tags:', self.tags)
        
def getInterfaces():
    interfaces=[]
    print("Getting interfaces...")
    entities_q = """ query { networkEntities(entityType:"LHCONE") { shortName hasFlow tags } }  """
    
    try:
    	r = requests.get(url, dict(query=entities_q), headers=dict(Authorization='Token ' + APIkey))

    	if r.status_code == 200:
            entities = r.json()
            # print(entities)
	    for e in entities['data']['networkEntities']:
                interfaces.append(interface(e['shortName'],e['hasFlow'],e['tags']))
    	else:
        	print 'got status {0}: {1}'.format(r.status_code, r.content)
    except:
        print ("Unexpected error in getting Interfaces:", sys.exc_info()[0])
	
    print ("Done.")
    for i in interfaces:
        i.prnt()
    return interfaces
    
def getInterfaceData(i):
    #print ("Loading interface data for: ",i.name)
    currenttime = datetime.utcnow()
    
    res=[]

    interface_q = """
    query {
      networkEntity(shortName: "%s", entityType: "LHCONE") {
        interfaces (beginTime: "%s", endTime:"%s") { device interface traffic }
      }
    }""" % (i.name, i.lastInterfaceUpdate.isoformat(), currenttime.isoformat())

    try:
    	r = requests.get(url, dict(query=interface_q), headers=dict(Authorization='Token ' + APIkey))

    	if r.status_code != 200:
	    print 'got status {0}: {1}'.format(r.status_code, r.content)
            return res

    	dat = r.json()
        ins = dat['data']['networkEntity']['interfaces']
    	#print(ins)

        d = datetime.utcnow()
        ind="esnet_"+str(d.year)+"-"+str(d.month)
        data = {
            '_index': ind,
            '_type': 'interface',
            'site': i.name
        }

        for s in ins:
            data['device'] = s["device"]
            data['interface'] = s["interface"]
            st = json.loads(s['traffic'])
            traf = st["points"]
            data['description'] = st["name"]
            for sample in traf:
                data['timestamp'] = sample[0]
                data['rateIn'] = long(sample[1])
                data['rateOut'] = long(sample[2])
                res.append(data.copy())
        print(i.name,'got',len(res),"interface results.")
        i.lastInterfaceUpdate = currenttime
        return res
            
    except:
        print ("Unexpected error:", sys.exc_info()[0])
    
    return res 

def getFlowData(i):
    #print ("Loading flow data for: ",i.name)
    currenttime = datetime.utcnow()
    res=[]

    try:
        
	flow_q = """
    	query {
      	networkEntity(shortName:"%s", entityType:"LHCONE") {
        flow(breakdown: "vpnsite" beginTime: "%s", endTime: "%s") { name traffic }
        }}""" % (i.name, i.lastFlowUpdate.isoformat(), currenttime.isoformat())

    	r = requests.get(url, dict(query=flow_q), headers=dict(Authorization='Token ' + APIkey))

    	if r.status_code != 200:
   	    print 'flow got status {0}: {1}'.format(r.status_code, r.content)
            return res

	dat = r.json()
 	flows= dat['data']['networkEntity']['flow']
        #print(flows)
        
        d = datetime.now()
        ind="esnet_"+str(d.year)+"-"+str(d.month)
        data = {
            '_index': ind,
            '_type': 'flow',
            'site1': i.name
        }

        for f in flows:
            data['site2']=f["name"].split("(")[0]
            st=json.loads(f['traffic'])
            traf=st["points"]
            for sample in traf:
                data['timestamp']=sample[0]
                data['rateIn']=sample[1]
                data['rateOut']=sample[2]
                res.append(data.copy())
	print(i.name,'got',len(res),"flow results.")
        i.lastFlowUpdate = currenttime
        return res

    except:
        print ("Unexpected error in flow data parsing: ", sys.exc_info()[0])
    return res 

def GetESConnection(lastReconnectionTime):
    if ( time.time()-lastReconnectionTime < 60 ): 
        return
    lastReconnectionTime=time.time()
    print ("make sure we are connected right...")
    res = requests.get('http://' + ESserver + ':' + str(ESport))
      #sys.exit(0)
    print(res.content)
    
    es = Elasticsearch([{'host': ESserver, 'port': ESport} ],http_auth=auth,timeout=timeout)
    return es

    

def loader(i):
    print ("starting a thread for ", i.name)
    while(True):
        aLotOfData=getInterfaceData(i)
        aLotOfData.extend(getFlowData(i))
        try:
            res = helpers.bulk(es, aLotOfData, raise_on_exception=True)
            aLotOfData=[]
            print (i.name, "inserted:",res[0], 'Errors:',res[1])
        except es_exceptions.ConnectionError as e:
            print ('ConnectionError ', e)
        except es_exceptions.TransportError as e:
            print ('TransportError ', e)
        except helpers.BulkIndexError as e:
            print (e)
            # for i in e[1]:
                # print i
        except:
            print ('Something seriously wrong happened indexing. ', sys.exc_info()[0])
        
        time.sleep(900)

es = GetESConnection(lastReconnectionTime)

def main():
    print('starting collection')
    global es
    while (not es):
        es = GetESConnection(lastReconnectionTime)

    interfaces=getInterfaces()

    # staggered start loaders threads
    for i in interfaces:
	time.sleep(20)
	t = Thread(target=loader,args=(i,))
	t.daemon = True
	t.start()

    while(True):
	at=threading.active_count()
    	print ("Active threads: ", at)
    	while (not es):
        	es = GetESConnection(lastReconnectionTime)
	time.sleep(900)

    sys.exit()


if __name__ == "__main__":
    main()

