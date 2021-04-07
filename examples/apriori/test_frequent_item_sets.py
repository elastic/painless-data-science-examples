from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import utils.read_scripted_metric as read_scripted_metric

def test_frequent_sets(user_name = '', password = ''):
    # Test that the sccripted metric produces similar results to the python implementation.
    # The results are not identical because of the random sampling.

    if user_name != '' and password != '':
        es = Elasticsearch(http_auth=(user_name, password))
    else:
        es = Elasticsearch()

    query_body = read_scripted_metric.read('examples/apriori/scripted_metric_frequent_sets.txt')

    result = Search.from_dict(query_body).using(es).index('apriori_demo').execute()

    size = 1
    for rulesk in result.aggregations.random_sample.frequent_sets.value:
        print('FREQUENT_SETS(size=' + str(size) + ')')
        for key,support in rulesk.to_dict().items():
            print('  ', key, 'support', support)
        size = size + 1
