#!/usr/bin/env python

import Queue
import socket
import time
import threading
import copy
import json
from datetime import datetime

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers
import stomp

import siteMapping

topic = '/topic/perfsonar.summary.status'
es = None

siteMapping.reload()

conns = []


class MyListener(object):
    def on_message(self, headers, message):
        q.put(message)

    def on_error(self, headers, message):
        print('received an error %s' % message)

    def on_heartbeat_timeout(self):
        print ('AMQ - lost heartbeat. Needs a reconnect!')
        connectToAMQ()

    def on_disconnected(self):
        print ('AMQ - no connection. Needs a reconnect!')
        connectToAMQ()


def connectToAMQ():
    global conns
    for conn in conns:
        if conn.is_connected():
            print('disconnecting first ...')
            conn.disconnect()
    conns = []

    addresses = socket.getaddrinfo('netmon-mb.cern.ch', 61513)
    ips = set()
    for a in addresses:
        ips.add(a[4][0])
    allhosts = []
    for ip in ips:
        allhosts.append([(ip, 61513)])

    for host in allhosts:
        conn = stomp.Connection(host, user='psatlflume', passcode=passwd.strip())
        conn.set_listener('MyConsumer', MyListener())
        conn.start()
        conn.connect()
        conn.subscribe(destination=topic, ack='auto', id="1", headers={})
        conns.append(conn)


def GetESConnection():
    print("make sure we are connected right...")
    try:
        es = Elasticsearch([{'host': 'cl-analytics.mwt2.org', 'port': 9200}])
        print ("connected OK!")
    except es_exceptions.ConnectionError as e:
        print('ConnectionError in GetESConnection: ', e)
    except:
        print('Something seriously wrong happened.')
    else:
        return es

    time.sleep(70)
    GetESConnection()


def eventCreator():
    aLotOfData = []
    while True:
        d = q.get()
        m = json.loads(d)
        data = {
            '_type': 'ps_perf'
        }

        metrics = ['perfSONAR services: ntp', 'perfSONAR esmond freshness', 'OSG datastore freshness',
                   'perfSONAR services: pscheduler']
        if not any(pattern in m['metric'] for pattern in metrics):
            q.task_done()
            continue
        if 'perf_metrics' in m.keys() and not m['perf_metrics']:
            q.task_done()
            continue
        data['host'] = m['host']
        prefix = m['metric'].replace("perfSONAR", "ps").replace(":", "").replace(" ", "_").lower()
        for k in m['perf_metrics'].keys():
            data[prefix+"_"+k] = m['perf_metrics'][k]
        dati = datetime.utcfromtimestamp(float(m['timestamp']))
        data['_index'] = "network_weather_2-" + str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
        data['timestamp'] = int(float(m['timestamp']) * 1000)
        #print(data)
        aLotOfData.append(copy.copy(data))
        q.task_done()

        if len(aLotOfData) > 100:
            reconnect = True
            # print('writing out data...')
            try:
                res = helpers.bulk(es, aLotOfData, raise_on_exception=True, request_timeout=60)
                print(threading.current_thread().name, "\t inserted:", res[0], '\tErrors:', res[1])
                aLotOfData = []
                reconnect = False
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
            if reconnect:
                es = GetESConnection()


passfile = open('/afs/cern.ch/user/i/ivukotic/ATLAS-Hadoop/.passfile')
passwd = passfile.read()

connectToAMQ()

q = Queue.Queue()
# start eventCreator threads
for i in range(3):
    t = threading.Thread(target=eventCreator)
    t.daemon = True
    t.start()

while True:
    time.sleep(60)
    print(datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "qsize:", q.qsize())
    for conn in conns:
        if not conn.is_connected():
            print ('problem with connection. try reconnecting...')
            connectToAMQ()
            break