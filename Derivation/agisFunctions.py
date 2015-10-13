import socket, json
import urllib, urllib2

def getATLAShosts(tipe=''):
    mapping={}
    req = urllib2.Request('http://atlas-agis-api.cern.ch/request/service/query/list/?json&state=ANY&type=PerfSonar', None)
    opener = urllib2.build_opener()
    f = opener.open(req)
    res=json.load(f)
    for r in res:
        # print r
        if r['endpoint']:
            hostname = r['endpoint']
        if r['rc_site']:
            site = r['rc_site']
        else:
            print "no rc_site!"
        if r['flavour']:
            flavour = r['flavour']
        else:
            print 'no flavour!'
        if tipe=='' or tipe==flavour:
            try:
                addr = socket.gethostbyname(hostname)
                print 'site: ', site, '\tflavour:',flavour, '\taddr:', addr,"\thost:",hostname
                mapping[addr]=site
            except:
                print 'problem'
    return mapping