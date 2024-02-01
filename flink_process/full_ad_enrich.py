import os
from pyflink.table import TableEnvironment, EnvironmentSettings
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
    print(f'Flink version:')
    kafka_full_orders_ddl = """
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
            `auth_type_id` INT
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'full_orders',
            'properties.bootstrap.servers' = 'broker:29092',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = 'http://schema-registry:8082',
            'properties.group.id'='full_orders_consumer_v1',
            'properties.max.message.bytes'='3000000',
            'scan.startup.mode'='earliest-offset'
        )
    """
    kafka_full_vehicles_ddl = """
        CREATE TABLE full_vehicles (
            `vehicle_id` VARCHAR,
            `km` INT,
            `prev_owner_number` TINYINT,
            `vehicle_order_id` VARCHAR,
            `market_info_id` VARCHAR,
            `media_type_id` INT,
            `year_on_road` INT,
            `test_date` DATE,
            `improve_id` INT,
            `air_bags` INT,
            `sun_roof` TINYINT,
            `magnesium_wheels` TINYINT,
            `revers_sensors` TINYINT,
            `abs` TINYINT,
            `hybrid` TINYINT,
            `doors` TINYINT,
            `environment_friendly_level` INT,
            `security_test_level` INT,
            `manufacturer_id` INT,
            `manufacturer_text` VARCHAR,
            `model_id` INT,
            `model_text` VARCHAR,
            `submodel_id` INT,
            `submodel_text` VARCHAR,
            `year` INT,
            `horse_power` INT,
            `cruse_control` TINYINT,
            `power_wheel` TINYINT,
            `fully_autonomic` TINYINT,
            `market_price` INT,
            `available_disk_slot` INT,
            `usb_slot_type` VARCHAR,
            `usb_slots` INT,
            `is_touch_display` INT,
            `stage_level` TINYINT,
            `stage_text` VARCHAR,
            `parts_improved_list` VARCHAR
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'full_vehicles',
            'properties.bootstrap.servers' = 'broker:29092',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = 'http://schema-registry:8082',
            'properties.group.id'='full_vehicle_consumer_v1',
            'properties.max.message.bytes'='3000000',
            'scan.startup.mode'='earliest-offset'
        )
    """
    kafka_full_ad_ddl = """
        CREATE TABLE full_ads (
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
            `vehicle_id` VARCHAR,
            `km` INT,
            `prev_owner_number` TINYINT,
            `market_info_id` VARCHAR,
            `media_type_id` INT,
            `year_on_road` INT,
            `test_date` DATE,
            `improve_id` INT,
            `air_bags` INT,
            `sun_roof` TINYINT,
            `magnesium_wheels` TINYINT,
            `revers_sensors` TINYINT,
            `abs` TINYINT,
            `hybrid` TINYINT,
            `doors` TINYINT,
            `environment_friendly_level` INT,
            `security_test_level` INT,
            `manufacturer_id` INT,
            `manufacturer_text` VARCHAR,
            `model_id` INT,
            `model_text` VARCHAR,
            `submodel_id` INT,
            `submodel_text` VARCHAR,
            `year` INT,
            `horse_power` INT,
            `cruse_control` TINYINT,
            `power_wheel` TINYINT,
            `fully_autonomic` TINYINT,
            `market_price` INT,
            `available_disk_slot` INT,
            `usb_slot_type` VARCHAR,
            `usb_slots` INT,
            `is_touch_display` INT,
            `stage_level` TINYINT,
            `stage_text` VARCHAR,
            `parts_improved_list` VARCHAR,
            PRIMARY KEY (order_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'key.format' = 'raw',
            'topic' = 'full_ads',
            'properties.bootstrap.servers' = 'broker:29092',
            'value.format' = 'avro-confluent',
            'value.avro-confluent.url' = 'http://schema-registry:8082',
            'sink.parallelism' = '1',
            'properties.auto.register.schemas'= 'true',
            'properties.use.latest.version'= 'true',
            'properties.max.block.ms' = '600000',
            'sink.buffer-flush.interval' = '100',
            'sink.buffer-flush.max-rows' = '100'
        )
    """
    env_settings = EnvironmentSettings.new_instance() \
      .in_streaming_mode().build()
    t_env = TableEnvironment.create(env_settings)
    t_env.get_config().set('pipeline.jars',get_jars_full_path()) \
    .set("parallelism.default", get_env('PARALLELISM', '1')) \
    .set("table.display.max-column-width", '2000')

    ddl_list = [
       kafka_full_orders_ddl, 
       kafka_full_vehicles_ddl,
       kafka_full_ad_ddl
    ]

    for ddl in ddl_list:
        t_env.execute_sql(ddl)

    full_orders_table = t_env.from_path('full_orders')
    full_vehicles_table = t_env.from_path('full_vehicles')
    full_orders_table \
    .join(full_vehicles_table) \
    .where(F.col('order_id') == F.col('vehicle_order_id')) \
    .drop_columns(F.col('vehicle_order_id')) \
    .execute_insert('full_ads').wait()
    

if __name__ == '__main__':
    log_processing()