import argparse, sys, os, logging
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table import expressions as F

def get_jars_path():
    path = os.getcwd()
    return f'file:///Users/barsnir/Desktop/flink_playground/flink-1.17.1/opt/'

def get_env(key:str, default:str) -> str: 
  return os.getenv(key, default)

def get_jars_full_path() -> str:
  jars_path = get_jars_path()
  jars = [
    'flink-connector-jdbc-3.1.0-1.17.jar;',
    'mysql-connector-java-5.1.9.jar'
  ]
  full_str = ''
  for jar in jars:
    full_str = f'{full_str}{jars_path}{jar}'
  return full_str

def process():
    environment_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(environment_settings)
    t_env.get_config().set('pipeline.jars',get_jars_full_path())
    source_ddl = """
        CREATE TABLE customers (
            CustomerID INT,
            LastName VARCHAR,
            FirstName VARCHAR,
            Address VARCHAR,
            City VARCHAR,
            PRIMARY KEY (CustomerID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/db',
            'table-name' = 'customers_test',
            'username'='root',
            'password'='password'
        );"""
    t_env.execute_sql(source_ddl)
    t_env.from_path('customers').execute().print()
    
if __name__ == '__main__':
    process()