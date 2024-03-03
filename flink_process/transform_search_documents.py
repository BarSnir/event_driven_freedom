from pyflink.table import expressions as F, DataTypes
from libs.connectors.kafka import FlinkKafkaConnector
from libs.streaming import FlinkStreamingEnvironment
from libs.utils.udfs import FlinkUDFs

def log_processing():
    streaming_env = FlinkStreamingEnvironment('transform_search_documents')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    full_ads_topic_connector = FlinkKafkaConnector(job_config.get('full_ads_topic'))
    search_documents_topic_connector = FlinkKafkaConnector(job_config.get('search_document_topic'))
    ddl_list = [
        full_ads_topic_connector.generate_kafka_connector('kafka'), 
        search_documents_topic_connector.generate_kafka_connector('upsert-kafka')
    ]
    for ddl in ddl_list:
        table_env.execute_sql(ddl)
    full_ads_table = table_env.from_path(full_ads_topic_connector.table_name)
    full_ads_table.filter(F.col('status_id') == 1)
    full_ads_table.select(
        F.col('order_id'),
        F.col('site_token'),
        F.col('status_id'),
        F.col('price'),
        F.col('market_price'),
        F.row(
            F.col('manufacturer_id'),
            F.col('manufacturer_text'),
            F.col('model_id'),
            F.col('model_text'),
            F.col('submodel_id'),
            F.col('submodel_text'),
            F.col('year'),
            F.col('km'),
            F.col('horse_power'),
            F.col('prev_owner_number').cast(DataTypes.INT()).alias('prev_owner_number'),
            F.col('year_on_road'),
            F.col('test_date'),
            F.row(
                F.col('fully_autonomic').cast(DataTypes.BOOLEAN()).alias('fully_autonomic'),
                F.col('power_wheel').cast(DataTypes.BOOLEAN()).alias('power_wheel'),
                F.col('cruse_control').cast(DataTypes.BOOLEAN()).alias('cruse_control'),
                F.col('sun_roof').cast(DataTypes.BOOLEAN()).alias('sun_roof'),
                F.col('magnesium_wheels').cast(DataTypes.BOOLEAN()).alias('magnesium_wheels'),
                F.col('air_bags').cast(DataTypes.INT()).alias('air_bags'),
                F.col('revers_sensors').cast(DataTypes.BOOLEAN()).alias('revers_sensors'),
                F.col('abs').cast(DataTypes.BOOLEAN()).alias('abs'),
                F.col('hybrid').cast(DataTypes.BOOLEAN()).alias('hybrid'),
                F.col('doors').cast(DataTypes.INT()).alias('doors'),
                F.col('environment_friendly_level').cast(DataTypes.INT()).alias('environment_friendly_level'),
                F.col('security_test_level').cast(DataTypes.INT()).alias('security_test_level'),
            ).alias('peripheral_equipment'),
            F.row(
                F.col('improve_id'),
                F.col('parts_improved_list'),
                F.col('stage_level').cast(DataTypes.INT()).alias('stage_level'),
                F.col('stage_text')
            ).alias('improvements'),
            F.row(
                F.col('available_disk_slot'),
                F.col('usb_slot_type'),
                F.col('usb_slots'),
                F.col('is_touch_display')
            ).alias('media')
        ).alias('vehicle_specs'),
        F.row(
            F.col('customer_id'),
            F.concat(F.col('first_name'),'_',F.col('last_name')) \
            .cast(DataTypes.STRING()).alias('full_name')
        ).alias('customer_details'),
        F.row(
            F.col('vehicle_id'),
            FlinkUDFs.cast_to_array(F.col('images_urls')).alias('images_urls'),
            F.col('images_count').cast(DataTypes.INT()).alias('images_count'),
        ).alias('meta_data')
    ).execute_insert(search_documents_topic_connector.table_name).wait()

if __name__ == '__main__':
    log_processing()