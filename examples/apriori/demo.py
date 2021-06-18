from elasticsearch_dsl import Search
from examples.apriori.generator import Generator
import utils.read_scripted_metric as read_scripted_metric

class Demo:
    def __init__(self,
                 user_name: str = '',
                 password: str = ''):
        self.generator = Generator(user_name, password)

    def setup(self):
        '''
        Setup a mixture of random and periodically arriving documents.
        '''
        print('GENERATING DEMO DATA...')

        self.generator.generate_and_index_demo_data()

    def run(self):
        '''
        Run the aggregation to find periodic beacons.
        '''
        print('FINDING FREQUENT ITEM SETS...')

        es = self.generator.es_client()

        scripted_metric_query_body = read_scripted_metric.read('examples/apriori/scripted_metric_frequent_sets.txt')

        results = Search.from_dict(scripted_metric_query_body).using(es).index('apriori_demo').execute()

        size = 1
        for rules in results.aggregations.random_sample.frequent_sets.value:
            print('FREQUENT_SETS(size=' + str(size) + ')')
            for key,support in rules.to_dict().items():
                print('  ', key, '/ support =', support)
            size = size + 1