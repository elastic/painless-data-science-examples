from elasticsearch_dsl import Search
from examples.beaconing.generator import Generator
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
        print('FIND BEACONS...')

        es = self.generator.es_client()
        
        scripted_metric_query_body = read_scripted_metric.read('examples/beaconing/scripted_metric_beacons.txt')
        results = Search.from_dict(scripted_metric_query_body).using(es).index('beaconing_demo').execute()

        for bucket in results.aggregations.process.buckets:
            print(bucket.key, 'is_beaconing:', bucket.beacon_stats.value.is_beaconing)
