import os
from dotenv import load_dotenv
from steps import (
    a_setup_database,
)

if __name__ == "__main__":
    if os.getenv('ENV', None) == None:
        load_dotenv()
    a_setup_database.process()