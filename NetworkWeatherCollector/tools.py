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

