import json, requests, os
from requests.exceptions import RequestException
class ElasticsearchConnector:

    def __init__(self):
        self.elasticsearch_url = os.getenv('ELASTICSEARCH_URL')
        self.index_pattern_url = '_index_template/search_documents'
        self.HEADERS = {
            'Content-type': 'application/json',
            'Accept': 'application/json'
        }

    def put_index_pattern(self, index_pattern, logger):
        try:
            requests.put(
                f"http://{self.elasticsearch_url}/{self.index_pattern_url }",
                json=index_pattern,
                headers=self.HEADERS
            )
        except requests.exceptions.RequestException as e:
            logger.critical(e)


    
