import json, requests, os
from requests.exceptions import RequestException
class KafkaConnectClient:

    def __init__(self):
        self.connect_url = os.getenv('CONNECT_URL')
        self.NEW_CONNECTOR_PATH = f"{self.connect_url}/connectors/"
        self.HEADERS = {
            'Content-type': 'application/json'
        }

    def post_new_connector(self, logger, connector_config):
        logger.debug(connector_config)
        logger.debug(f"{self.NEW_CONNECTOR_PATH }")
        response = requests.post(
            self.NEW_CONNECTOR_PATH,
            json=connector_config,
            headers=self.HEADERS
        )
        if response.status_code == 400:
            logger.error(response.json())
            raise RequestException