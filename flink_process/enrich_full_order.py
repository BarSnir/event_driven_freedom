import os
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table import expressions as F

def get_jars_path():
    return f'file:///Users/barsnir/Desktop/projects/event_driven_freedom/jars/'

def get_env(key:str, default:str) -> str: 
  return os.getenv(key, default)

def get_jars_full_path() -> str:
  jars_path = get_jars_path()
  jars = [
    'flink-sql-connector-kafka-1.17.1.jar;',
    'flink-sql-avro-1.17.1.jar;',
    'flink-sql-avro-confluent-registry-1.17.1.jar'
  ]
  full_str = ''
  for jar in jars:
    full_str = f'{full_str}{jars_path}{jar}'
  return full_str

def log_processing():
    kafka_source_ddl = """
        CREATE TABLE orders (
            `OrderId` VARCHAR,
            `op` VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'Orders',
            'properties.bootstrap.servers' = 'localhost:9092',
            'value.format' = 'debezium-avro-confluent',
            'value.debezium-avro-confluent.url' = 'http://localhost:8082',
            'properties.group.id'='orders_v1',
            'properties.max.message.bytes'='3000000',
            'scan.startup.mode'='earliest-offset'
        )
    """
    env_settings = EnvironmentSettings.new_instance() \
      .in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set('pipeline.jars',get_jars_full_path()) \
    .set("parallelism.default", get_env('PARALLELISM', '1'))


    t_env.execute_sql(kafka_source_ddl)
    order_table = t_env.from_path('orders')
    order_table.execute().print()
if __name__ == '__main__':
    log_processing()