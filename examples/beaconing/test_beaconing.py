from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from examples.beaconing.generator import Generator
import numpy as np
import utils.read_scripted_metric as read_scripted_metric

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
                        "gte": Generator.START_TIME,
                        "lt":  Generator.START_TIME + 6 * 3600 * 1000
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
            print(tag)
            counts = []
            for count in bucket['time_buckets']:
                counts.append(count['doc_count'])
            expected_beaconing[tag] = Test.__is_beaconing(counts)

        print(expected_beaconing)

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
        a = a + 1
        b,_ = next(filter(lambda x: x[1] > 0, enumerate(reversed(counts))))
        b = len(counts) - b - 1

        # There are too few buckets to be confident in the test statistics.
        if b - a < 16:
            return 0

        counts = counts[a+1:b-1]
        mean = np.mean(counts)
        variance = np.var(counts)

        # If the period less than the bucket interval then we expect to see low variation
        # in the count per bucket. For Poisson process we expect the variance to be equal
        # to the mean so this condition implies that the signal is much more regular than
        # a Poisson process.
        if variance < 0.1 * mean:
            return True

        # If the period is greater than the buckt interval we can check for a periodic
        # pattern in the buckt counts. We do this by lookig for high values of the
        # autocovariance function.

        max_period = int(len(counts) / 4)

        autocovariances = [0] * (max_period - 1)

        for period in range(2, max_period + 1):

            # Allow for jitter <= 10% of period.
            jitter = int(0.1 * period)

            n = 0
            for i in range(0, len(counts) - 2 * period - jitter + 1, period):

                autocovariances_i = [0] * (2 * jitter + 1)

                for j in range(-jitter, jitter + 1):
                    for k in range(i, i + period):
                        autocovariances_i[jitter + j] = \
                            autocovariances_i[jitter + j] + \
                            (counts[k] - mean) * (counts[k + period + j] - mean)

                autocovariances[period - 2] = autocovariances[period - 2] + max(autocovariances_i)
                n = n + period

            autocovariances[period - 2] = autocovariances[period - 2] / n

        # We use the fact that if a signal is periodic with period p it will have high
        # autocovariance for any shift i * p for integer i. So we average over the
        # autocovariance for multiples of the period. This works around the fact that
        # smoothly varying signals will have high autocovariance for small shifts.
        for i in range(0, len(autocovariances)):
            np.mean(autocovariances[i:len(autocovariances):i + 2])

        pearson = max(autocovariances) / variance
        print('pearson', pearson)

        return pearson >= 0.75

    @staticmethod
    def __es_client(user_name: str = '',
                    password: str = ''):
        if user_name != '' and password != '':
            return Elasticsearch(http_auth=(user_name, password))
        return Elasticsearch()
