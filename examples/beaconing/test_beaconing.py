from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from examples.beaconing.generator import Generator
import math
import numpy as np

# TODO
#import utils.read_scripted_metric as read_scripted_metric

class Test:

    @staticmethod
    def setup(user_name = '', password = ''):
        '''
        Generate some data used for testing. 

        @param user_name The Elasticsearch client name.
        @param password The password for user_name.
        '''
        es = Test.__es_client(user_name, password)
        Generator.recreate_index(es)
        Generator.generate_and_index_beacon('beacon_1m', 60000, 0.01, es)
        Generator.generate_and_index_beacon('beacon_5m', 300000, 0.05, es)
        Generator.generate_and_index_beacon('beacon_10m', 600000, 0.05, es)
        for i in range(1, 5):
            Generator.generate_and_index_poisson_process('poisson_' + str(i), 300000, es)
        for i in range(5, 10):
            Generator.generate_and_index_poisson_process('poisson_' + str(i), 10000, es, 6000)

    @staticmethod
    def test_generated(user_name = '', password = ''):
        '''
        Test the beaconing detection scipted metric aggregation on a toy generated
        data set vs a python reference implementation.

        @param user_name The Elasticsearch client name.
        @param password The password for user_name.
        '''
        es = Test.__es_client(user_name, password)

        terms_date_histogram_result = Search.from_dict({
            "size": 0,
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": "now-6h",
                        "lt": "now"
                    }
                }
            },
            "aggs": {
                "counts": {
                    "terms": {
                        "field": "tag",
                        "size": 12
                    },
                    "aggs": {
                        "time_buckets": {
                            "date_histogram": {
                                "field": "@timestamp",
                                "fixed_interval": "1m"
                            }
                        }
                    }
                }
            }
        }).using(es).index(Generator.INDEX_NAME).execute()

        expected_beaconing = {}
        for bucket in terms_date_histogram_result.aggregations.counts.buckets:
            tag = bucket['key']
            counts = []
            for count in bucket['time_buckets']:
                counts.append(count['doc_count'])
            expected_beaconing[tag] = Test.__is_beaconing(counts)

        # TODO
        #scripted_metric_query_body = read_scripted_metric.read('examples/beaconing/scripted_metric_beacons.txt')
        # scripted_metric_result = Search.from_dict(scripted_metric_query_body).using(es).index('beaconing_demo').execute()


    @staticmethod
    def __is_beaconing(counts: list):
        '''
        Check if a signal appears to be beaconing.

        For signals which are significantly higher frequency than the bucket interval
        we check that they are much more regular than a Poisson process.

        For signals which are sparse on the bucket length we check their autocovariance
        are high.
        '''
        # Drop zero counts at start and end and first and last partial buckets to allow
        # for signals which are intermittent.
        a,_ = next(filter(lambda x: x[1] > 0, enumerate(counts)))
        b,_ = next(filter(lambda x: x[1] > 0, enumerate(reversed(counts))))
        b = len(counts) - b
        if b - a <= 2:
            return 0
        counts = counts[a+1:b-1]

        mean = np.mean(counts)
        if mean == 0:
            return False

        variance = np.var(counts)

        # For Poisson process we expect var equals the mean so this condition implies that
        # the signal is much more regular than a Poisson process.
        if variance < 0.1 * mean:
            return True

        print(sum(1 if count == 0 else 0 for count in counts))
        print(len(counts))

        if 2 * sum(1 if count == 0 else 0 for count in counts) < len(counts):
            return False

        # If the signal is sparse on the bucket length it's variance will be high. However,
        # in this case we can directly check if the values arrive periodically.

        max_period = int(len(counts) / 4)

        autocovariances = [0] * (max_period - 1)

        for period in range(2, max_period + 1):

            # Allow for jitter <= 10% of period.
            max_jitter = int(0.1 * period)

            n = 0
            for i in range(0, len(counts) - 2 * period - max_jitter + 1, period):

                autocovariances_i = []

                for j in range(-max_jitter, max_jitter + 1):
                    for k in range(i, i + period):
                        autocovariances_i.append((counts[k] - mean) * (counts[k + period + j] - mean))

                autocovariances[period - 2] = autocovariances[period - 2] + max(autocovariances_i)
                n = n + period

            autocovariances[period - 2] = autocovariances[period - 2] / n

        # We use the fact that if a signal is periodic with period p it will have high
        # autocovariance for any shift i * p for integer i. So we average over the
        # autocovariance for multiples of the period. This works around the fact that
        # smoothly varying signals will have high autocovariance for small shifts.
        for i in range(0, len(autocovariances)):
            np.mean(autocovariances[i:len(autocovariances):i + 2])

        pearson_corr = max(autocovariances) / variance
        
        return True if pearson_corr > 0.6 else False

    @staticmethod
    def __es_client(user_name: str = '',
                    password: str = ''):
        if user_name != '' and password != '':
            return Elasticsearch(http_auth=(user_name, password))
        return Elasticsearch()
