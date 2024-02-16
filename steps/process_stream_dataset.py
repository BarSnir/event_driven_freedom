import os
from libs.connectors.kafka_connect import KafkaConnectConnector
from requests.exceptions import RequestException
from libs.utils.logger import ColorLogger

MODULE_MESSAGE = 'Step C || Generating Debezium & Apache Flink stream operations!'

def process(logger):
    ColorLogger.log_new_step_dashes(logger)
    logger.info(MODULE_MESSAGE)
    ColorLogger.log_new_step_dashes(logger)
    kafka_connect_connector = KafkaConnectConnector()
    try:
        with open(f"{os.getenv('DEBEZIUM_CONFIG_FILE_PATH')}") as debezium_config_file:
            response = kafka_connect_connector.post_new_connector(logger, debezium_config_file)
            if response.status_code == 400:
                logger.error(response.json())
                raise RequestException
    except RequestException as req_exception:
        logger.error("Pay attention to connector request.")
    # Wakeup flink stream clusters.
    # Run flink stream workflow.