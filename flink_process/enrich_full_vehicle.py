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
    kafka_vehicles_ddl = """
        CREATE TABLE vehicles (
            `VehicleId` VARCHAR,
            `KM` INT,
            `PrevOwnerNumber` TINYINT,
            `OrderId` VARCHAR,
            `MarketInfoId` VARCHAR,
            `MediaTypeId` INT,
            `YearOnRoad` INT,
            `TestDate` DATE,
            `ImproveId` INT,
            PRIMARY KEY (VehicleId) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'Vehicles',
            'properties.bootstrap.servers' = 'broker:29092',
            'value.format' = 'debezium-avro-confluent',
            'value.debezium-avro-confluent.url' = 'http://schema-registry:8082',
            'properties.group.id'='vehicles_consumer_v1',
            'properties.max.message.bytes'='3000000',
            'scan.startup.mode'='earliest-offset'
        )
    """
    kafka_market_info_ddl = """
        CREATE TABLE market_info (
            `MarketInfoId` VARCHAR,
            `AirBags` INT,
            `SunRoof` TINYINT,
            `MagnesiumWheels` TINYINT,
            `ReversSensors` TINYINT,
            `ABS` TINYINT,
            `Hybrid` TINYINT,
            `Doors` TINYINT,
            `EnvironmentFriendlyLevel` INT,
            `SecurityTestLevel` INT,
            `ManufacturerId` INT,
            `ManufacturerText` VARCHAR,
            `ModelId` INT,
            `ModelText` VARCHAR,
            `SubModelId` INT,
            `SubModelText` VARCHAR,
            `Year` INT,
            `HorsePower` INT,
            `CruseControl` TINYINT,
            `PowerWheel` TINYINT,
            `FullyAutonomic` TINYINT,
            `MarketPrice` INT,
            PRIMARY KEY (MarketInfoId) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'MarketInfo',
            'properties.bootstrap.servers' = 'broker:29092',
            'value.format' = 'debezium-avro-confluent',
            'value.debezium-avro-confluent.url' = 'http://schema-registry:8082',
            'properties.group.id'='market_info_consumer_v1',
            'properties.max.message.bytes'='3000000',
            'scan.startup.mode'='earliest-offset'
        )
    """
    kafka_media_type_ddl = """
        CREATE TABLE media_types (
            `MediaTypeId` INT,
            `AvailableDiskSlot` INT,
            `UsbSlotType` VARCHAR,
            `UsbSlots` INT,
            `IsTouchDisplay` INT,
            PRIMARY KEY (MediaTypeId) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'MediaType',
            'properties.bootstrap.servers' = 'broker:29092',
            'value.format' = 'debezium-avro-confluent',
            'value.debezium-avro-confluent.url' = 'http://schema-registry:8082',
            'properties.group.id'='media_types_consumer_v1',
            'properties.max.message.bytes'='3000000',
            'scan.startup.mode'='earliest-offset'
        )
    """
    kafka_improves_ddl = """
        CREATE TABLE improves (
            `ImproveId` INT,
            `StageLevel` TINYINT,
            `StageText` VARCHAR,
            `PartsImprovedList` VARCHAR,
            PRIMARY KEY (ImproveId) NOT ENFORCED
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'Improves',
            'properties.bootstrap.servers' = 'broker:29092',
            'value.format' = 'debezium-avro-confluent',
            'value.debezium-avro-confluent.url' = 'http://schema-registry:8082',
            'properties.group.id'='customers_consumer_v1',
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
            `parts_improved_list` VARCHAR,
            PRIMARY KEY (vehicle_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'key.format' = 'raw',
            'topic' = 'full_vehicles',
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
    .set("table.display.max-column-width", '2000') \
    .set("table.exec.source.cdc-events-duplicate", "true")


    ddl_list = [
       kafka_vehicles_ddl, 
       kafka_market_info_ddl,
       kafka_media_type_ddl,
       kafka_improves_ddl,
       kafka_full_vehicles_ddl
    ]

    for ddl in ddl_list:
        t_env.execute_sql(ddl)

    vehicles_table = t_env.from_path('vehicles')
    mark_info_table = t_env.from_path('market_info')
    media_types_table = t_env.from_path('media_types')
    improves_table = t_env.from_path('improves')

    mark_info_table = mark_info_table \
    .rename_columns(F.col('MarketInfoId').alias('market_info_id_right'))

    media_types_table = media_types_table \
    .rename_columns(F.col('MediaTypeId').alias('media_type_id_right'))

    improves_table = improves_table \
    .rename_columns(F.col('ImproveId').alias('improve_id_right'))

    vehicles_table = vehicles_table \
    .join(mark_info_table).where(F.col('MarketInfoId') == F.col('market_info_id_right')) \
    .join(media_types_table).where(F.col('MediaTypeId') == F.col('media_type_id_right')) \
    .join(improves_table).where(F.col('ImproveId') == F.col('improve_id_right')) \
    .drop_columns(
        F.col('market_info_id_right'),
        F.col('media_type_id_right'), 
        F.col('improve_id_right')
    ) \
    .select(
        F.col('VehicleId').alias('vehicle_id'),
        F.col('KM').alias('km'),
        F.col('PrevOwnerNumber').alias('prev_owner_number'),
        F.col('OrderId').alias('vehicle_order_id'),
        F.col('MarketInfoId').alias('market_info_id'),
        F.col('MediaTypeId').alias('media_type_id'),
        F.col('YearOnRoad').alias('year_on_road'),
        F.col('TestDate').alias('test_date'),
        F.col('ImproveId').alias('improve_id'),
        F.col('AirBags').alias('air_bags'),
        F.col('SunRoof').alias('sun_roof'),
        F.col('MagnesiumWheels').alias('magnesium_wheels'),
        F.col('ReversSensors').alias('revers_sensors'),
        F.col('ABS').alias('abs'),
        F.col('Hybrid').alias('hybrid'),
        F.col('Doors').alias('doors'),
        F.col('EnvironmentFriendlyLevel').alias('environment_friendly_level'),
        F.col('SecurityTestLevel').alias('security_test_level'),
        F.col('ManufacturerId').alias('manufacturer_id'),
        F.col('ManufacturerText').alias('manufacturer_text'),
        F.col('ModelId').alias('model_id'),
        F.col('ModelText').alias('model_text'),
        F.col('SubModelId').alias('submodel_id'),
        F.col('SubModelText').alias('submodel_text'),
        F.col('Year').alias('year'),
        F.col('HorsePower').alias('horse_power'),
        F.col('CruseControl').alias('cruse_control'),
        F.col('PowerWheel').alias('power_wheel'),
        F.col('FullyAutonomic').alias('fully_autonomic'),
        F.col('MarketPrice').alias('market_price'),
        F.col('AvailableDiskSlot').alias('available_disk_slot'),
        F.col('UsbSlotType').alias('usb_slot_type'),
        F.col('UsbSlots').alias('usb_slots'),
        F.col('IsTouchDisplay').alias('is_touch_display'),
        F.col('StageLevel').alias('stage_level'),
        F.col('StageText').alias('stage_text'),
        F.col('PartsImprovedList').alias('parts_improved_list')
    ).execute_insert('full_vehicles').wait()
    

if __name__ == '__main__':
    log_processing()