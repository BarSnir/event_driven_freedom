from pyflink.table import expressions as F 
from libs.connectors.kafka import FlinkKafkaConnector
from libs.streaming import FlinkStreamingEnvironment

def log_processing():
    streaming_env = FlinkStreamingEnvironment('enrich_full_vehicle')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    vehicles_topic_connector = FlinkKafkaConnector(job_config.get('vehicles_topic'))
    market_info_topic_connector = FlinkKafkaConnector(job_config.get('market_info_topic'))
    media_types_topic_connector = FlinkKafkaConnector(job_config.get('media_types_topic'))
    improves_topic_connector = FlinkKafkaConnector(job_config.get('improves_topic'))
    full_vehicles_topic_connector = FlinkKafkaConnector(job_config.get('full_vehicles_topic'))
    kafka_vehicles_ddl = vehicles_topic_connector.generate_kafka_connector('kafka')
    kafka_market_info_ddl = market_info_topic_connector.generate_kafka_connector('kafka')
    kafka_media_type_ddl = media_types_topic_connector.generate_kafka_connector('kafka')
    kafka_improves_ddl = improves_topic_connector.generate_kafka_connector('kafka')
    kafka_full_vehicles_ddl = full_vehicles_topic_connector.generate_kafka_connector('upsert-kafka')

    ddl_list = [
       kafka_vehicles_ddl, 
       kafka_market_info_ddl,
       kafka_media_type_ddl,
       kafka_improves_ddl,
       kafka_full_vehicles_ddl
    ]
    for ddl in ddl_list:
        table_env.execute_sql(ddl)

    vehicles_table = table_env.from_path(vehicles_topic_connector.table_name)
    mark_info_table = table_env.from_path(market_info_topic_connector.table_name)
    media_types_table = table_env.from_path(media_types_topic_connector.table_name)
    improves_table = table_env.from_path(improves_topic_connector.table_name)

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
        F.col('FamilyTypeId').alias('family_type_id'),
        F.col('FamilyTypeText').alias('family_type_text'),
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
    ).execute_insert(full_vehicles_topic_connector.table_name).wait()
    

if __name__ == '__main__':
    log_processing()