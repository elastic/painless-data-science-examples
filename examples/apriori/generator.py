from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import random

IP_NOT_REACHABLE = "IP_NOT_REACHABLE"
IP_REACHABLE = "IP_REACHABLE"
RESPONSE = "RESPONSE"
NO_RESPONSE = "NO_RESPONSE"
MISMATCH_REQUEST_RESPONSE = "MISMATCH_REQUEST_RESPONSE"
vPAS_FAILURE = "vPAS-Failure"
POM_FAILURE = "POM-Failure"
PAD_FAILURE = "PAD-Failure"
RELAY_LINK_STATUS = "RELAY_LINK_STATUS"
DIAMETER_PEER_GROUP_DOWN = "DIAMETER_PEER_GROUP_DOWN"
NO_PEER_GROUP_MEMBER_AVAILABLE = "NO_PEER_GROUP_MEMBER_AVAILABLE"
DIAMETER_PEER_GROUP_DOWN_RX = "DIAMETER_PEER_GROUP_DOWN_RX"
DIAMETER_PEER_GROUP_DOWN_GX = "DIAMETER_PEER_GROUP_DOWN_GX"
DIAMETER_PEER_GROUP_DOWN_TX = "DIAMETER_PEER_GROUP_DOWN_TX"
DIAMETER_PEER_GROUP_UP_RX = "DIAMETER_PEER_GROUP_UP_RX"
DIAMETER_PEER_GROUP_UP_GX = "DIAMETER_PEER_GROUP_UP_GX"
DIAMETER_PEER_GROUP_UP_TX = "DIAMETER_PEER_GROUP_UP_TX"
PROCESS_STATE = "PROCESS_STATE"
NO_PROCESS_STATE = "NO_PROCESS_STATE"

ALL = [
    [IP_NOT_REACHABLE, IP_REACHABLE],
    [RESPONSE, NO_RESPONSE, MISMATCH_REQUEST_RESPONSE],
    [vPAS_FAILURE, POM_FAILURE, PAD_FAILURE],
    [RELAY_LINK_STATUS],
    [DIAMETER_PEER_GROUP_DOWN, NO_PEER_GROUP_MEMBER_AVAILABLE],
    [DIAMETER_PEER_GROUP_DOWN_RX, DIAMETER_PEER_GROUP_DOWN_GX, DIAMETER_PEER_GROUP_DOWN_TX,
     DIAMETER_PEER_GROUP_UP_RX, DIAMETER_PEER_GROUP_UP_GX, DIAMETER_PEER_GROUP_UP_TX],
    [PROCESS_STATE, NO_PROCESS_STATE]
]

RULES = [
    [IP_NOT_REACHABLE, RELAY_LINK_STATUS, NO_PEER_GROUP_MEMBER_AVAILABLE, PROCESS_STATE],
    [IP_NOT_REACHABLE, NO_PEER_GROUP_MEMBER_AVAILABLE, DIAMETER_PEER_GROUP_DOWN_RX],
    [NO_RESPONSE, IP_REACHABLE, vPAS_FAILURE, DIAMETER_PEER_GROUP_UP_RX],
    [NO_RESPONSE, IP_REACHABLE, POM_FAILURE, DIAMETER_PEER_GROUP_UP_TX],
    [DIAMETER_PEER_GROUP_DOWN, DIAMETER_PEER_GROUP_DOWN_GX, NO_PROCESS_STATE],
    [MISMATCH_REQUEST_RESPONSE, IP_REACHABLE, RELAY_LINK_STATUS, PROCESS_STATE, PAD_FAILURE]
]

def rule_generator(index_name, number):
    # Generates approximately 'number' of each rule

    time = ((datetime.now() - datetime(1970,1,1)).total_seconds() - 86400 * 28) * 1000.0

    for _ in range(len(RULES) * number):
        rule = random.choice(RULES)
        doc = {'_index': index_name, '@timestamp': int(time)}
        time = time + random.expovariate(1 / 10000)
        for i in range(0, len(rule)):
            doc['f' + str(i + 1)] = rule[i]
        yield doc

def rand_generator(index_name, number):
    # Generates 'number' of random events

    time = ((datetime.now() - datetime(1970,1,1)).total_seconds() - 86400 * 28) * 1000.0

    for _ in range(number):
        doc = {'_index': index_name, '@timestamp': int(time)}
        number_conds = int(random.uniform(0, 6))
        for i in range(0, number_conds + 1):
            doc['f' + str(i + 1)] = random.choice(ALL[i])
        yield doc
        time = time + random.expovariate(1 / (10000 * len(RULES)))

def recreate_index(es, index_name):
    mappings = {
        'mappings':{
            'properties':{
                '@timestamp':{
                    'type':'date',
                    'format':'epoch_millis'
                },
                'f1':{ 'type': 'keyword' },
                'f2':{ 'type': 'keyword' },
                'f3':{ 'type': 'keyword' },
                'f4':{ 'type': 'keyword' },
                'f5':{ 'type': 'keyword' },
                'f6':{ 'type': 'keyword' }
            }
        }
    }
    es.indices.delete(index=index_name, ignore=[400, 404])
    es.indices.create(index=index_name, ignore=400, body=mappings)

def report_progress(counter, number_docs, last_progress):
    if counter / number_docs > last_progress + 0.05:
        print(counter,'/', number_docs)
        last_progress = last_progress + 0.05
    return counter + 1, last_progress

def generate_and_index_data(user_name = '', password = '', number = 35000):

    if user_name != '' and password != '':
        es = Elasticsearch(http_auth=(user_name, password))
    else:
        es = Elasticsearch()

    index_name = 'apriori_demo'
    recreate_index(es, index_name)

    number = int(number / (len(RULES) + 1))

    counter = 0
    last_progress = 0
    stream = rule_generator(index_name, number)
    for ok, response in streaming_bulk(es, actions=stream, chunk_size=number):
        if not ok:
            print(response)
        counter,last_progress = report_progress(counter, number * (len(RULES) + 1), last_progress)

    stream = rand_generator(index_name, number)
    for ok, response in streaming_bulk(es, actions=stream, chunk_size=10000):
        if not ok:
            print(response)
        counter,last_progress = report_progress(counter, number * (len(RULES) + 1), last_progress)
