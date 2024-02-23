from pyflink.table import expressions as F 
from libs.connectors.kafka import FlinkKafkaConnector
from libs.streaming import FlinkStreamingEnvironment

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
    full_ads_table.filter(F.col('status_id') == 1) \
    .execute_insert(search_documents_topic_connector.table_name).wait()

if __name__ == '__main__':
    log_processing()