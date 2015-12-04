# maps ips to sites

import urllib2, sys
import socket
import time

try: import simplejson as json
except ImportError: import json

ot=0
PerfSonars={}
throughputHosts=[]
latencyHosts=[]

class ps:
    hostname=''
    sitename=''
    VO=''
    ip=''
    flavor=''
    def prnt(self):
        print 'ip:',self.ip,'\thost:',self.hostname,'\tVO:',self.VO,'\tflavor:',self.flavor

def getIP(host):
    ip='unknown'
    try:
        ip=socket.gethostbyname(host)
    except:
        print "Could not get ip for", host
    return ip


def reload():
    global ot
    global throughputHosts
    global latencyHosts
    
    ot=time.time()
    try:
        req = urllib2.Request("http://atlas-agis-api.cern.ch/request/site/query/list/?json&vo_name=atlas&state=ACTIVE", None)
        opener = urllib2.build_opener()
        f = opener.open(req)
        res = json.load(f)
        sites=[]
        for s in res:
            sites.append(s["rc_site"])
        # print res
        print 'Sites reloaded.'
    except:
        print "Could not get sites from AGIS. Exiting..."
        print "Unexpected error: ", str(sys.exc_info()[0])

        
    try:
        req = urllib2.Request("http://atlas-agis-api.cern.ch/request/service/query/list/?json&state=ACTIVE&type=PerfSonar", None)
        opener = urllib2.build_opener()
        f = opener.open(req)
        res = json.load(f)
        for s in res:
            p=ps()
            p.hostname=s['endpoint']
            p.ip=getIP(p.hostname)
            if s['status']=='production': p.production=True
            p.flavor=s['flavour']
            p.sitename=s['rc_site']
            if p.sitename in sites: p.VO="ATLAS";
            sites.append(s["rc_site"])
            PerfSonars[p.ip]=p
            p.prnt()
        print 'Perfsonars reloaded.'
    except:
        print "Could not get perfsonars from AGIS. Exiting..."
        print "Unexpected error: ", str(sys.exc_info()[0])
    
    try:
        req = urllib2.Request("https://myosg.grid.iu.edu/psmesh/json/name/wlcg-all", None)
        opener = urllib2.build_opener()
        f = opener.open(req)
        res = json.load(f)
        throughputHosts=[]
        for o in res['organizations']:
            for s in o['sites']:
                for h in s['hosts']:
                    for a in h['addresses']:
                        print a
                        ip=getIP(a)
                        if ip!='unknown': throughputHosts.append(ip)
        print throughputHosts
        print 'throughputHosts reloaded.'
    except:
        print "Could not get perfsonar throughput hosts. Exiting..."
        print "Unexpected error: ", str(sys.exc_info()[0])
        
    try:
        req = urllib2.Request("https://myosg.grid.iu.edu/psmesh/json/name/wlcg-latency-all", None)
        opener = urllib2.build_opener()
        f = opener.open(req)
        res = json.load(f)
        latencyHosts=[]
        for o in res['organizations']:
            for s in o['sites']:
                for h in s['hosts']:
                    for a in h['addresses']:
                        print a
                        ip=getIP(a)
                        if ip!='unknown': latencyHosts.append(ip)
        print latencyHosts
        print 'latencyHosts reloaded.'
    except:
        print "Could not get perfsonar latency hosts. Exiting..."
        print "Unexpected error: ", str(sys.exc_info()[0])
    

def getPS(ip):
    global ot
    if (time.time()-ot)>86400: 
        print ot
        reload()
    if ip in PerfSonars:
        return [PerfSonars[ip].sitename,PerfSonars[ip].VO]

def isProductionLatency(ip):
    if ip in latencyHosts: 
        return True
    return False 
          
def isProductionThroughput(ip):
    if ip in throughputHosts: 
        return True
    return False  