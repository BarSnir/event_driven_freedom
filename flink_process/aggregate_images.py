from pyflink.table import expressions as F 
from pyflink.table.expression import DataTypes
from libs.connectors.kafka import FlinkKafkaConnector
from libs.streaming import FlinkStreamingEnvironment

def log_processing():
    streaming_env = FlinkStreamingEnvironment('aggregate_images')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    images_topic_connector = FlinkKafkaConnector(job_config.get('images_topic'))
    aggregate_topic_connector = FlinkKafkaConnector(job_config.get('aggregate_topic'))
    kafka_images_ddl = images_topic_connector.generate_kafka_connector("kafka")
    kafka_agg_images_ddl = aggregate_topic_connector.generate_kafka_connector("upsert-kafka")
    table_env.execute_sql(kafka_images_ddl)
    table_env.execute_sql(kafka_agg_images_ddl)

    images_table = table_env.from_path(images_topic_connector.table_name) \
    .rename_columns(F.col('OrderId').alias('ImageOrderId')) \
    .add_columns(F.col('Priority').cast(DataTypes.STRING()).alias('ImagePriority')) \
    .add_columns(F.concat(F.col('Url'), '_', F.col('ImagePriority')).alias('ImagePriorityAgg')) \
    .drop_columns(F.col('Priority'), F.col('Url'), F.col('ImageId')) \
    .group_by(F.col('ImageOrderId')) \
    .select(
      F.col('ImageOrderId').alias('images_order_id'),
      F.col('ImagePriorityAgg').count.alias('images_count'),
      F.col('ImagePriorityAgg').list_agg(',').alias('images_urls')
    )
    images_table.execute_insert(aggregate_topic_connector.table_name).wait()

if __name__ == '__main__':
    log_processing()