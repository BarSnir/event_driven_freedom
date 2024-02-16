import json, requests, os
class KafkaConnectConnector:

    def __init__(self):
        pass

    def post_new_connector(self, logger, connector_config_file):
        connector_config = json.load(connector_config_file)
        headers={
            'Content-type': 'application/json'
        }
        logger.debug(connector_config)
        logger.debug(f"{os.getenv('CONNECT_URL')}/connectors")
        return requests.post(
            f"{os.getenv('CONNECT_URL')}/connectors/",
            json=connector_config,
            headers=headers
        )