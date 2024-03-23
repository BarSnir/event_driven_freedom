from requests.exceptions import RequestException
from libs.utils.list import ListUtils
from libs.utils.files import FileUtils
from libs.utils.logger import ColorLogger
from libs.connectors.kafka_connect import KafkaConnectClient
from libs.connectors.kafka_admin import KafkaAdminClientWrap

MODULE_MESSAGE = 'Step F || Generating Elasticsearch sink connector'
FILE_PATH = '/opt/flink/project/configs/minio_sink.json'

def process(logger):
    ColorLogger.log_new_step_dashes(logger)
    logger.info(MODULE_MESSAGE)
    ColorLogger.log_new_step_dashes(logger)
    kafka_admin_client = KafkaAdminClientWrap(logger)
    kafka_connect_client = KafkaConnectClient()
    try:
        config = FileUtils.get_json_file(FILE_PATH)
        logger.debug(config)
        topic_list = ListUtils.str_to_list(
            config.get('config').get('topics')
        )
        logger.debug(topic_list)
        kafka_admin_client.find_topics(topic_list)
        kafka_connect_client.post_new_connector(
            logger, config
        )
        logger.info(f"Done!")
    except RequestException:
        logger.error("Pay attention to connector request.")
    except Exception as e:
        logger.error(e)