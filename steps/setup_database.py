from libs.connectors.mysql import MySqlConnector
from libs.utils.logger import ColorLogger
MODULE_MESSAGE = 'Step A || Creating databases & tables'

def process(logger):
    ColorLogger.log_new_step_dashes(logger)
    logger.info(MODULE_MESSAGE)
    ColorLogger.log_new_step_dashes(logger)
    mysql_connector = MySqlConnector(logger)
    mysql_connector.create_database()
    mysql_connector.create_tables()