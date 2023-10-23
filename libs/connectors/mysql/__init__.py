import mysql.connector, os, json
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

    def create_tables(self)-> None:
        self._cursor.execute(f"USE {os.getenv('DATABSE')}")
        with open("configs/database.json") as databse_config:
            for table in json.load(databse_config).get('tables'):
                table_name = table.get('name')
                columns = self._construct_columns_statement(table.get('columns'))
                try:
                    self._cursor.execute(f"CREATE TABLE {table_name} ({columns})")
                    self.logger.info(f"Table {table_name} created!")
                except Exception as e:
                    self.logger.warn(e)
                    if os.getenv('LOG_LEVEL') == 'DEBUG':
                        self.logger.debug(f"SQL STATEMENT: CREATE TABLE {table_name} ({columns})")
                    continue
                
    def _construct_columns_statement(self, columns: list)-> str:
        column_statement = ""
        for column in columns:
            column_statement = column_statement + column
        return column_statement