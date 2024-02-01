from libs.connectors.mysql import MySqlConnector

MODULE_MESSAGE = 'Step A || Creating databases & tables'
LOGGER_NAME = 'mysql_setup'

def process(logger):
    logger.info(MODULE_MESSAGE)
    mysql_connector = MySqlConnector(logger)
    mysql_connector.create_database()
    mysql_connector.create_tables()