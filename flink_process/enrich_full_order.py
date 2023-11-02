import os
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table import expressions as F 
from pyflink.table.expression import JsonOnNull, DataTypes
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
    kafka_orders_ddl = """
        CREATE TABLE orders (
            `OrderId` VARCHAR,
            `CustomerId` INT,
            `SiteToken` VARCHAR,
            `StatusId` INT,
            `Price` INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'Orders',
            'properties.bootstrap.servers' = 'localhost:9092',
            'value.format' = 'debezium-avro-confluent',
            'value.debezium-avro-confluent.url' = 'http://localhost:8082',
            'properties.group.id'='one_consumer_v1',
            'properties.max.message.bytes'='3000000',
            'scan.startup.mode'='earliest-offset'
        )
    """
    kafka_images_ddl = """
        CREATE TABLE images (
            `ImageId` VARCHAR,
            `OrderId` VARCHAR,
            `Url` VARCHAR,
            `Priority` INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'Images',
            'properties.bootstrap.servers' = 'localhost:9092',
            'value.format' = 'debezium-avro-confluent',
            'value.debezium-avro-confluent.url' = 'http://localhost:8082',
            'properties.group.id'='one_consumer_v1',
            'properties.max.message.bytes'='3000000',
            'scan.startup.mode'='earliest-offset'
        )
    """
    kafka_customers_ddl = """
        CREATE TABLE Customers (
            `CustomerId` INT,
            `FirstName` VARCHAR,
            `LastName` VARCHAR,
            `Email` VARCHAR,
            `CustomerTypeId` INT,
            `CustomerTypeText` VARCHAR,
            `JoinDate` DATE,
            `ProfileImage` VARCHAR,
            `IsSuspended` INT,
            `SuspendedReasonId` INT,
            `SuspendedReasonText` VARCHAR,
            `AuthTypeId` INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'Customers',
            'properties.bootstrap.servers' = 'localhost:9092',
            'value.format' = 'debezium-avro-confluent',
            'value.debezium-avro-confluent.url' = 'http://localhost:8082',
            'properties.group.id'='one_consumer_v1',
            'properties.max.message.bytes'='3000000',
            'scan.startup.mode'='earliest-offset'
        )
    """
    kafka_full_order_ddl = """
        CREATE TABLE full_orders (
            `order_id` VARCHAR,
            `customer_id` INT,
            `site_token` VARCHAR,
            `status_id` INT,
            `price` INT,
            `images_urls` VARCHAR,
            `images_count` BIGINT,
            `first_name` VARCHAR,
            `last_name` VARCHAR,
            `email` VARCHAR,
            `customer_type_id` INT,
            `customer_type_text` VARCHAR,
            `join_date` DATE,
            `profile_image` VARCHAR,
            `is_suspended` INT,
            `suspended_reason_id` INT,
            `suspended_reason_text` VARCHAR,
            `auth_type_id` INT,
            PRIMARY KEY (order_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'key.format' = 'raw',
            'topic' = 'full_orders',
            'properties.bootstrap.servers' = 'localhost:9092',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = 'http://localhost:8082',
            'sink.parallelism' = '1',
            'properties.auto.register.schemas'= 'true',
            'properties.use.latest.version'= 'true'
        )
    """
    env_settings = EnvironmentSettings.new_instance() \
      .in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set('pipeline.jars',get_jars_full_path()) \
    .set("parallelism.default", get_env('PARALLELISM', '1'))


    t_env.execute_sql(kafka_orders_ddl)
    t_env.execute_sql(kafka_images_ddl)
    t_env.execute_sql(kafka_customers_ddl)
    t_env.execute_sql(kafka_full_order_ddl)

    order_table = t_env.from_path('orders')
    images_table = t_env.from_path('images') \
    .rename_columns(F.col('OrderId').alias('ImageOrderId')) \
    .add_columns(F.col('Priority').cast(DataTypes.STRING()).alias('ImagePriority'))
    images_count = images_table.group_by(F.col('ImageOrderId')) \
    .select(
      F.col('ImageOrderId'),
      F.col('Url').count.alias('images_count')
    )
    images_urls = images_table.rename_columns(F.col('ImageOrderId').alias('images_order_id_urls')) \
    .group_by(F.col('images_order_id_urls')) \
    .select(
      F.col('images_order_id_urls'),
      F.json_object_agg(
        JsonOnNull.NULL,
        F.col('ImagePriority'),
        F.col('Url')
      ).alias('images_urls')
    )
    images_table = images_urls.join(images_count) \
    .where(F.col('ImageOrderId') == F.col('images_order_id_urls')) \
    .drop_columns(F.col('images_order_id_urls')) \
    .select(
        F.col('ImageOrderId').alias('images_order_id'),
        F.col('images_urls'),
        F.col('images_count')
    )
    customers = t_env.from_path('Customers').select(
       F.col('CustomerId').alias('customer_id_table'),
        F.col('FirstName').alias('first_name'),
        F.col('LastName').alias('last_name'),
        F.col('Email').alias('email'),
        F.col('CustomerTypeId').alias('customer_type_id'),
        F.col('CustomerTypeText').alias('customer_type_text'),
        F.col('JoinDate').alias('join_date'),
        F.col('ProfileImage').alias('profile_image'),
        F.col('IsSuspended').alias('is_suspended'),
        F.col('SuspendedReasonId').alias('suspended_reason_id'),
        F.col('SuspendedReasonText').alias('suspended_reason_text'),
        F.col('AuthTypeId').alias('auth_type_id')
    )
    order_table.join(images_table).where(F.col('OrderId') == F.col('images_order_id')) \
    .drop_columns(F.col('images_order_id')) \
    .select(
       F.col('OrderId').alias('order_id'),
        F.col('CustomerId').alias('customer_id'),
        F.col('SiteToken').alias('site_token'),
        F.col('StatusId').alias('status_id'),
        F.col('Price').alias('price'),
        F.col('images_urls'),
        F.col('images_count')
    ).join(customers).where(F.col('customer_id') == F.col('customer_id_table')) \
    .drop_columns(F.col('customer_id_table')) \
    .execute_insert('full_orders').wait()
if __name__ == '__main__':
    log_processing()