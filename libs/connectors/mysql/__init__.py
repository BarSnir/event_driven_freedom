import mysql.connector, os
from logging import Logger
from mysql.connector.cursor import CursorBase
class MySqlConnector:

    def __init__(self, logger: Logger):
        self._connector = self._get_connector()
        self._cursor = self._get_cursor()
        self.logger = logger

    def _get_connector(self)-> mysql.connector.MySQLConnection:
        return mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
    
    def _get_cursor(self)-> CursorBase:
        return self._connector.cursor()
    
    def create_database(self)-> None:
        try:
            self._cursor.execute(f"CREATE DATABASE {os.getenv('DATABSE')}")
            self.logger.info(f"Database {os.getenv('DATABSE')} created!")
        except Exception as e:
            self.logger.warn(e)