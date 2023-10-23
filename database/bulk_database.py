from dotenv import load_dotenv
from libs.connectors.mysql import MySqlConnector

def process():
    # Step 1: Create Connector's handler
    mysql_connector = MySqlConnector()


if __name__ == "__main__":
    load_dotenv()
    process()