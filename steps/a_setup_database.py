from libs.connectors.mysql import MySqlConnector
from libs.utils.logger import ColorLogger

MODULE_MESSAGE = 'Step A | Starting to setup MySQL database'
LOGGER_NAME = 'mysql_setup'

def process():
    # Step 1: Create Connector's handler
    logger = ColorLogger(LOGGER_NAME).get_logger()
    logger.info(MODULE_MESSAGE)
    mysql_connector = MySqlConnector(logger)
    mysql_connector.create_database()
    mysql_connector.create_tables()
