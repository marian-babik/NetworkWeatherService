# maps ips to sites

import urllib2, sys
import socket
import time

try: import simplejson as json
except ImportError: import json

global ot
PerfSonars={}
ot=0

class ps:
    hostname=''
    sitename=''
    VO=''
    production=False
    ip=''
    flavor=''

def getIP(host):
    ip='unknown'
    try:
        ip=socket.gethostbyname(host)
    except:
        print "Could not get ip for", host
    return ip


def reload():
    ot=time.time()
    sites=[]
    try:
        req = urllib2.Request("http://atlas-agis-api.cern.ch/request/site/query/list/?json&vo_name=atlas&state=ACTIVE", None)
        opener = urllib2.build_opener()
        f = opener.open(req)
        res = json.load(f)
        for s in res:
            sites.append(s["rc_site"])
        # print res
        print 'Sites reloaded.'
    except:
        print "Could not get sites from AGIS. Exiting..."
        print "Unexpected error: ", str(sys.exc_info()[0])
        sys.exit(1)
        
    try:
        PerfSonars.clear()
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
        # print res
        print 'Perfsonars reloaded.'
    except:
        print "Could not get perfsonars from AGIS. Exiting..."
        print "Unexpected error: ", str(sys.exc_info()[0])
        sys.exit(1)
        
def getPS(ip):
    if (time.time()-ot)>600: 
        reload()
    if ip in PerfSonars:
        return [PerfSonars[ip].sitename,PerfSonars[ip].VO,PerfSonars[ip].production]

        
        