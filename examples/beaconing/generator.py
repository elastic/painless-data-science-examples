from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
import random

class Generator:
    INDEX_NAME = 'beaconing_demo'
    # 2021-06-01 00:00:00 in epoch milliseconds
    START_TIME = 1622505600000

    @staticmethod
    def recreate_index(es: Elasticsearch):
        '''
        Recreate the index containing the test data
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

        es.indices.delete(index=Generator.INDEX_NAME, ignore=[400, 404])
        es.indices.create(index=Generator.INDEX_NAME, ignore=400, body=mappings)

    @staticmethod
    def generate_and_index_beacon(tag: str,
                                  period: int,
                                  jitter: float,
                                  es: Elasticsearch,
                                  number: int = 1000):
        '''
        Generate documents which are periodic in time and uniform random jitter.

        @param: jitter The jitter is expressed as a fraction of the period which
        should be in the range [0, 1].
        @param number The approximate number of documents to create.
        '''
        counter = 0
        last_progress = 0

        stream = Generator.__periodic_documents_with_jitter_generator(tag, period, jitter, number)
        for ok, response in streaming_bulk(es, actions=stream, chunk_size=number):
            if not ok:
                print(response)
            counter,last_progress = Generator.__report_progress(counter, number, last_progress)


    @staticmethod
    def generate_and_index_poisson_process(tag: str,
                                           mean_interval: float,
                                           es: Elasticsearch,
                                           number: int = 1000):
        '''
        Generate documents according to a Poisson process.
        
        @param: mean_interval The inverse rate in milliseconds.
        '''
        counter = 0
        last_progress = 0
        stream = Generator.__generate_poisson_process(tag, mean_interval, number)

        for ok, response in streaming_bulk(es, actions=stream, chunk_size=number):
            if not ok:
                print(response)
            counter,last_progress = Generator.__report_progress(counter, number, last_progress)


    @staticmethod
    def __generate_poisson_process(tag: str,
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

    @staticmethod
    def __periodic_documents_with_jitter_generator(tag: str,
                                                   period: float,
                                                   jitter: float,
                                                   number: int):
        time = Generator.START_TIME
        for _ in range(number):
            yield {
                '_index': Generator.INDEX_NAME,
                'tag': tag,
                '@timestamp': time
            }
            time = time + int(period + random.uniform(-jitter, jitter) * period + 0.5)

    @staticmethod
    def __report_progress(counter: int,
                          number_docs: int,
                          last_progress: int):
        if counter / number_docs > last_progress + 0.05:
            print(counter,'/', number_docs)
            last_progress = last_progress + 0.05
        return counter + 1, last_progress
