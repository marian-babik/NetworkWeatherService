from elasticsearch import Elasticsearch, exceptions as es_exceptions
from elasticsearch import helpers

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


def bulk_index(data , es=None, thread_name=''):
	reconnect = True
    if es is None:
        es = GetESConnection()
    try:
        res = helpers.bulk(es, data, raise_on_exception=True, request_timeout=60)
        print(thread_name, "inserted:", res[0], 'errors:', res[1])
        data = []
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
