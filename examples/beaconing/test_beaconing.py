from elasticsearch_dsl import Search
from examples.beaconing.generator import Generator
import numpy as np
import utils.read_scripted_metric as read_scripted_metric

class Test:
    def __init__(self,
                 user_name: str = '',
                 password: str = ''):
        self.generator = Generator(user_name, password)

    def es_client(self):
        return self.generator.es_client()

    def setup_generated(self):
        '''
        Generate some data used for testing.

        This isn't run as part setup because it relatively heavyweight.
        '''
        self.generator.generate_and_index_demo_data()

    def test_generated(self):
        '''
        Test the beaconing detection scipted metric aggregation on the data set
        generated by setup_generated vs a python reference implementation.
        '''
        es = self.es_client()

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
                        "size": 20
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

        expected_results = {}
        for bucket in terms_date_histogram_result.aggregations.counts.buckets:
            counts = []
            for count in bucket['time_buckets']:
                counts.append(count['doc_count'])
            expected_results[bucket['key']] = self.__is_beaconing(counts)

        scripted_metric_query_body = read_scripted_metric.read('examples/beaconing/scripted_metric_beacons.txt')
        actual_results = Search.from_dict(scripted_metric_query_body).using(es).index('beaconing_demo').execute()

        failed = False

        for bucket in actual_results.aggregations.process.buckets:
            expected_result = expected_results[bucket.key]
            if (self.__assert_equal(bucket.beacon_stats.value.is_beaconing, expected_result['is_beaconing']) or
                self.__assert_equal(bucket.beacon_stats.value.non_empty_buckets, expected_result['non_empty_buckets']) or
                self.__assert_close(bucket.beacon_stats.value.mean, expected_result['mean'], 1e-4) or
                self.__assert_close(bucket.beacon_stats.value.variance, expected_result['variance'], 1e-4) or
                ('pearson' in expected_result and
                 self.__assert_close(bucket.beacon_stats.value.pearson, expected_result['pearson'], 1e-4))):
                print('mismatch:\n', expected_result, '\nvs\n', bucket.beacon_stats.value.to_dict())
                failed = True
                break

        print('TEST', 'FAILED' if failed else 'PASSED')

    def __is_beaconing(self, counts: list):
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
            return {"is_beaconing": False, "non_empty_buckets": b - a}

        counts = counts[a:b]
        mean = np.mean(counts)
        variance = np.var(counts)

        # If the period less than the bucket interval then we expect to see low variation
        # in the count per bucket. For Poisson process we expect the variance to be equal
        # to the mean so this condition implies that the signal is much more regular than
        # a Poisson process.
        if variance < 0.1 * abs(mean):
            return {"is_beaconing": True, "non_empty_buckets": b - a, "mean": mean, "variance": variance}

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
                        autocovariances_i[jitter + j] += \
                            (counts[k] - mean) * (counts[k + period + j] - mean)

                autocovariances[period - 2] += max(autocovariances_i)
                n += period

            autocovariances[period - 2] = autocovariances[period - 2] / n

        # We use the fact that if a signal is periodic with period p it will have high
        # autocovariance for any shift i * p for integer i. So we average over the
        # autocovariance for multiples of the period. This works around the fact that
        # smoothly varying signals will have high autocovariance for small shifts.
        for i in range(0, len(autocovariances)):
            autocovariances[i] = np.mean(autocovariances[i:len(autocovariances):i + 2])

        pearson = min(max(autocovariances) / variance, 1)

        return {"is_beaconing": pearson >= 0.7,
                "non_empty_buckets": b - a,
                "mean": mean,
                "variance": variance,
                "pearson": pearson}

    def __assert_equal(self, lhs, rhs):
        return lhs != rhs

    def __assert_close(self, lhs: float, rhs: float, tolerance: float):
        return abs(rhs - lhs) > tolerance
