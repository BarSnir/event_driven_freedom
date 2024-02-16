import json, requests,os
from libs.utils.logger import ColorLogger

MODULE_MESSAGE = 'Step C || Generating Debezium & Apache Flink stream operations!'

def process(logger):
    ColorLogger.log_new_step_dashes(logger)
    logger.info(MODULE_MESSAGE)
    ColorLogger.log_new_step_dashes(logger)
    try:
        with open(f"{os.getenv('CONNECTOR_CONFIG_PATH')}") as connector_config_file:
            connector_config = json.load(connector_config_file)
            headers={
                'Content-type': 'application/json'
            }
            logger.debug(connector_config)
            logger.debug(f"{os.getenv('CONNECT_URL')}/connectors")
            requests.post(
                f"{os.getenv('CONNECT_URL')}/connectors/",
                json=connector_config,
                headers=headers
            )
    except:
        pass
    # Wakeup flink stream clusters.
    # Run flink stream workflow.