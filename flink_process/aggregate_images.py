import os
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table import expressions as F 
from pyflink.table.expression import DataTypes

def get_jars_path():
    return f'file:///opt/flink/opt/'

def get_env(key:str, default:str) -> str: 
  return os.getenv(key, default)

def get_jars_full_path() -> str:
  jars_path = get_jars_path()
  jars = [
    'flink-sql-connector-kafka-3.0.0-1.17.jar;',
    'flink-sql-avro-1.17.1.jar;',
    'flink-sql-avro-confluent-registry-1.17.1.jar'
  ]
  full_str = ''
  for jar in jars:
    full_str = f'{full_str}{jars_path}{jar}'
  return full_str


def log_processing():
    kafka_images_ddl = """
        CREATE TABLE images (
            `ImageId` VARCHAR,
            `OrderId` VARCHAR,
            `Url` VARCHAR,
            `Priority` INT,
            `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
            PRIMARY KEY (ImageId) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'Images',
            'properties.bootstrap.servers' = 'broker:29092',
            'value.format' = 'debezium-avro-confluent',
            'value.debezium-avro-confluent.url' = 'http://schema-registry:8082',
            'properties.group.id'='one_consumer_v1',
            'properties.max.message.bytes'='3000000',
            'scan.startup.mode'='earliest-offset'
        )
    """
    kafka_agg_images_ddl = """
        CREATE TABLE aggregate_images (
            `images_order_id` VARCHAR,
            `images_count` BIGINT,
            `images_urls` VARCHAR,
            PRIMARY KEY (images_order_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'key.format' = 'raw',
            'topic' = 'aggregate_images',
            'properties.bootstrap.servers' = 'broker:29092',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = 'http://schema-registry:8082',
            'sink.parallelism' = '1',
            'properties.auto.register.schemas'= 'true',
            'properties.use.latest.version'= 'true',
            'properties.max.block.ms' = '600000',
            'sink.buffer-flush.interval' = '10000',
            'sink.buffer-flush.max-rows' = '220000'
        )
    """

    env_settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=env_settings)
    t_env.get_config().get_configuration().set_integer("parallelism.default", 1)
    t_env.get_config().get_configuration().set_boolean("pipeline.operator-chaining.chain-operators-with-different-max-parallelism", False)
    t_env.get_config().set('pipeline.jars',get_jars_full_path()) \
    .set("table.display.max-column-width", '2000')

    t_env.execute_sql(kafka_images_ddl)
    t_env.execute_sql(kafka_agg_images_ddl)

    images_table = t_env.from_path('images') \
    .rename_columns(F.col('OrderId').alias('ImageOrderId')) \
    .add_columns(F.col('Priority').cast(DataTypes.STRING()).alias('ImagePriority')) \
    .add_columns(F.concat(F.col('Url'), '_', F.col('ImagePriority')).alias    ('ImagePriorityAgg')) \
    .drop_columns(F.col('Priority'), F.col('Url'), F.col('ImageId')) \
    .group_by(F.col('ImageOrderId')) \
    .select(
      F.col('ImageOrderId').alias('images_order_id'),
      F.col('ImagePriorityAgg').count.alias('images_count'),
      F.col('ImagePriorityAgg').list_agg(',').alias('images_urls')
    )
    images_table.execute_insert('aggregate_images').wait()
if __name__ == '__main__':
    log_processing()