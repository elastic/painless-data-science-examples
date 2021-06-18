from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import random

class Generator:
    INDEX_NAME = 'apriori_demo'
    IP_NOT_REACHABLE = 'IP_NOT_REACHABLE'
    IP_REACHABLE = 'IP_REACHABLE'
    RESPONSE = 'RESPONSE'
    NO_RESPONSE = 'NO_RESPONSE'
    MISMATCH_REQUEST_RESPONSE = 'MISMATCH_REQUEST_RESPONSE'
    vPAS_FAILURE = 'vPAS-Failure'
    POM_FAILURE = 'POM-Failure'
    PAD_FAILURE = 'PAD-Failure'
    RELAY_LINK_STATUS = 'RELAY_LINK_STATUS'
    DIAMETER_PEER_GROUP_DOWN = 'DIAMETER_PEER_GROUP_DOWN'
    NO_PEER_GROUP_MEMBER_AVAILABLE = 'NO_PEER_GROUP_MEMBER_AVAILABLE'
    DIAMETER_PEER_GROUP_DOWN_RX = 'DIAMETER_PEER_GROUP_DOWN_RX'
    DIAMETER_PEER_GROUP_DOWN_GX = 'DIAMETER_PEER_GROUP_DOWN_GX'
    DIAMETER_PEER_GROUP_DOWN_TX = 'DIAMETER_PEER_GROUP_DOWN_TX'
    DIAMETER_PEER_GROUP_UP_RX = 'DIAMETER_PEER_GROUP_UP_RX'
    DIAMETER_PEER_GROUP_UP_GX = 'DIAMETER_PEER_GROUP_UP_GX'
    DIAMETER_PEER_GROUP_UP_TX = 'DIAMETER_PEER_GROUP_UP_TX'
    PROCESS_STATE = 'PROCESS_STATE'
    NO_PROCESS_STATE = 'NO_PROCESS_STATE'

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

    def __init__(self, user_name: str = '', password: str = ''):
        if user_name != '' and password != '':
            self.es = Elasticsearch(http_auth=(user_name, password))
        else:
            self.es = Elasticsearch()

    def es_client(self):
        return self.es

    def generate_and_index_demo_data(self,
                                     number: int = 35000,
                                     report_progress: bool = True):
        '''
        Generate and index some demo data.
        '''
        self.recreate_index()

        number = int(number / (len(Generator.RULES) + 1))

        counter = 0
        last_progress = 0
        stream = self.__rule_generator(number)
        for ok, response in streaming_bulk(self.es, actions=stream, chunk_size=number):
            if not ok:
                print(response)
            if report_progress:
                counter,last_progress = self.__report_progress(counter, number * (len(Generator.RULES) + 1), last_progress)

        stream = self.__rand_generator(number)
        for ok, response in streaming_bulk(self.es, actions=stream, chunk_size=10000):
            if not ok:
                print(response)
            if report_progress:
                counter,last_progress = self.__report_progress(counter, number * (len(Generator.RULES) + 1), last_progress)

    def recreate_index(self):
        '''
        Recreate the index containing the demo data.
        '''
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
        self.es.indices.delete(index=Generator.INDEX_NAME, ignore=[400, 404])
        self.es.indices.create(index=Generator.INDEX_NAME, ignore=400, body=mappings)

    def __rule_generator(self, number: int):
        # Generates approximately 'number' of each rule

        time = ((datetime.now() - datetime(1970,1,1)).total_seconds() - 86400 * 28) * 1000.0

        for _ in range(len(Generator.RULES) * number):
            rule = random.choice(Generator.RULES)
            doc = {'_index': Generator.INDEX_NAME, '@timestamp': int(time)}
            time = time + random.expovariate(1 / 10000)
            for i in range(0, len(rule)):
                doc['f' + str(i + 1)] = rule[i]
            yield doc

    def __rand_generator(self, number: int):
        # Generates 'number' of random events

        time = ((datetime.now() - datetime(1970,1,1)).total_seconds() - 86400 * 28) * 1000.0

        for _ in range(number):
            doc = {'_index': Generator.INDEX_NAME, '@timestamp': int(time)}
            number_conds = int(random.uniform(0, 6))
            for i in range(0, number_conds + 1):
                doc['f' + str(i + 1)] = random.choice(Generator.ALL[i])
            yield doc
            time = time + random.expovariate(1 / (10000 * len(Generator.RULES)))

    def __report_progress(self, counter, number_docs, last_progress):
        if counter / number_docs > last_progress + 0.05:
            print(counter,'/', number_docs)
            last_progress = last_progress + 0.05
        return counter + 1, last_progress
