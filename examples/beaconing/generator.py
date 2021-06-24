from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import random

class Generator:
    INDEX_NAME = 'beaconing_demo'
    # 2021-06-01 00:00:00 in epoch milliseconds
    START_TIME = 1622505600000

    def __init__(self, user_name: str = '', password: str = ''):
        if user_name != '' and password != '':
            self.es = Elasticsearch(http_auth=(user_name, password))
        else:
            self.es = Elasticsearch()

    def es_client(self):
        return self.es

    def generate_and_index_demo_data(self):
        '''
        Generate and index some demo data.
        '''
        self.recreate_index()
        self.generate_and_index_beacon('beacon_1m', [60000], 0.01)
        self.generate_and_index_beacon('beacon_5m', [300000], 0.05)
        self.generate_and_index_beacon('beacon_10m', [600000], 0.05)
        self.generate_and_index_beacon('beacon_irregular', [300000, 180000], 0.01)
        for i in range(1, 5):
            self.generate_and_index_poisson_process('poisson_' + str(i), 300000)
        for i in range(5, 10):
            self.generate_and_index_poisson_process('poisson_' + str(i), 10000, 6000)

    def recreate_index(self):
        '''
        Recreate the index containing the demo data.
        '''
        mappings = {
            'mappings': {
                'properties': {
                    '@timestamp': {
                        'type':'date',
                        'format':'epoch_millis'
                    },
                    'tag': { 'type': 'keyword' },
                }
            }
        }

        self.es.indices.delete(index=Generator.INDEX_NAME, ignore=[400, 404])
        self.es.indices.create(index=Generator.INDEX_NAME, ignore=400, body=mappings)

    def generate_and_index_beacon(self,
                                  tag: str,
                                  period: list[int],
                                  jitter: float,
                                  number: int = 1000,
                                  report_progress: bool = False):
        '''
        Generate documents which are periodic in time and uniform random jitter.

        @param: jitter The jitter is expressed as a fraction of the period which
        should be in the range [0, 1].
        @param number The approximate number of documents to create.
        '''
        counter = 0
        last_progress = 0

        stream = self.__periodic_with_jitter_generator(tag, period, jitter, number)
        for ok, response in streaming_bulk(self.es, actions=stream, chunk_size=number):
            if not ok:
                print(response)
            if report_progress:
                counter,last_progress = self.__report_progress(counter, number, last_progress)

    def generate_and_index_poisson_process(self,
                                           tag: str,
                                           mean_interval: float,
                                           number: int = 1000,
                                           report_progress: bool = False):
        '''
        Generate documents according to a Poisson process.
        
        @param: mean_interval The inverse rate in milliseconds.
        '''
        counter = 0
        last_progress = 0
        stream = self.__poisson_process_generator(tag, mean_interval, number)

        for ok, response in streaming_bulk(self.es, actions=stream, chunk_size=number):
            if not ok:
                print(response)
            if report_progress:
                counter,last_progress = self.__report_progress(counter, number, last_progress)

    def __poisson_process_generator(self,
                                    tag: str,
                                    mean_interval: float,
                                    number: int):
        time = Generator.START_TIME
        for _ in range(number):
            yield {
                '_index': Generator.INDEX_NAME,
                'tag': tag,
                '@timestamp': time
            }
            time = time + int(random.expovariate(1 / mean_interval))

    def __periodic_with_jitter_generator(self,
                                         tag: str,
                                         period: list[int],
                                         jitter: float,
                                         number: int):
        i = 0
        time = Generator.START_TIME
        for _ in range(number):
            yield {
                '_index': Generator.INDEX_NAME,
                'tag': tag,
                '@timestamp': time
            }
            time = time + int(period[i] + random.uniform(-jitter, jitter) * period[i] + 0.5)
            i = (i + 1) % len(period)

    def __report_progress(self,
                          counter: int,
                          number_docs: int,
                          last_progress: int):
        if counter / number_docs > last_progress + 0.05:
            print(counter,'/', number_docs)
            last_progress = last_progress + 0.05
        return counter + 1, last_progress
