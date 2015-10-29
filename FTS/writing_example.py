#!/usr/bin/env python

import json
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import helpers

from pprint import pprint

aLotOfData=[]

d = datetime.now()
ind="fts-"+str(d.year)+"."+str(d.month)+"."+str(d.day)

data = {
    '_index': ind,
    '_type': 'transfer',
    'activity': 'Express',
    'atlas_site_dst': 'CERN-PROD',
    'atlas_site_src': 'BNL-ATLAS',
    'bytes': 33123516,
    'fts_ended_at': 1446045441000,
    'fts_started_at': 1446045442000,
    'submitted_at':1446045402000
}

aLotOfData.append(data)

pprint(data)

print "make sure we are connected right..."
import requests
res = requests.get('http://cl-analytics.mwt2.org:9200')
print(res.content)

es = Elasticsearch([{'host':'cl-analytics.mwt2.org', 'port':9200}])

res = helpers.bulk(es, aLotOfData)
print(res)