#!/usr/bin/env python

import Queue
import socket
import time
import threading
import copy
import math
import json
from datetime import datetime

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers
import stomp

import siteMapping

topic = '/topic/perfsonar.raw.histogram-owdelay'
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
            '_type': 'latency'
        }

        source = m['meta']['source']
        destination = m['meta']['destination']
        data['MA'] = m['meta']['measurement_agent']
        data['src'] = source
        data['dest'] = destination
        so = siteMapping.getPS(source)
        de = siteMapping.getPS(destination)
        if so is not None:
            data['srcSite'] = so[0]
            data['srcVO'] = so[1]
        if de is not None:
            data['destSite'] = de[0]
            data['destVO'] = de[1]
        data['srcProduction'] = siteMapping.isProductionLatency(source)
        data['destProduction'] = siteMapping.isProductionLatency(destination)
        su = m['datapoints']
        for ts, th in su.iteritems():
            dati = datetime.utcfromtimestamp(float(ts))
            data['_index'] = "network_weather-test-" + str(dati.year) + "." + str(dati.month) + "." + str(dati.day)
            data['timestamp'] = int(float(ts) * 1000)
            th_fl = dict((float(k), v) for (k, v) in th.items())

            # mean
            th_mean = sum(k*v for k, v in th_fl.items())/600
            data['delay_mean'] = th_mean
            # std dev
            data['delay_sd'] = math.sqrt(sum((k - th_mean) ** 2 * v for k, v in th_fl.items()) / 600)
            # median
            csum = 0
            ordered_th = [(k, v) for k, v in sorted(th_fl.items())]
            for index, entry in enumerate(ordered_th):
                csum += entry[1]
                if csum > 301:
                    data['delay_median'] = entry[0]
                    break
                elif csum == 300:
                    data['delay_median'] = entry[0] + ordered_th[index+1][0] / 2
                    break
                elif csum == 301 and index == 0:
                    data['delay_median'] = entry[0]
                    break
                elif csum == 301 and index > 0:
                    data['delay_median'] = entry[0] + ordered_th[index-1][0] / 2
                    break
            #print(data)
            aLotOfData.append(copy.copy(data))
        q.task_done()
        if len(aLotOfData) > 500:
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
