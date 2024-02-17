from pyflink.table import expressions as F 
from libs.connectors.kafka import FlinkKafkaConnector
from libs.streaming import FlinkStreamingEnvironment

def log_processing():
    streaming_env = FlinkStreamingEnvironment('enrich_full_ad')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    full_order_topic_connector = FlinkKafkaConnector(job_config.get('full_order_topic'))
    full_vehicles_topic_connector = FlinkKafkaConnector(job_config.get('full_vehicles_topic'))
    full_ads_topic_connector = FlinkKafkaConnector(job_config.get('full_ads_topic'))
    kafka_full_orders_ddl = full_order_topic_connector.generate_kafka_connector('kafka')
    kafka_full_vehicles_ddl = full_vehicles_topic_connector.generate_kafka_connector('kafka')
    kafka_full_ad_ddl = full_ads_topic_connector.generate_kafka_connector('upsert-kafka')
    ddl_list = [
       kafka_full_orders_ddl, 
       kafka_full_vehicles_ddl,
       kafka_full_ad_ddl
    ]
    for ddl in ddl_list:
        table_env.execute_sql(ddl)

    full_orders_table = table_env.from_path(full_order_topic_connector.table_name)
    full_vehicles_table = table_env.from_path(full_vehicles_topic_connector.table_name)
    full_orders_table \
    .join(full_vehicles_table) \
    .where(F.col('order_id') == F.col('vehicle_order_id')) \
    .drop_columns(F.col('vehicle_order_id')) \
    .execute_insert(full_ads_topic_connector.table_name).wait()
    

if __name__ == '__main__':
    log_processing()