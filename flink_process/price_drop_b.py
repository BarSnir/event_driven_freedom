from pyflink.table import expressions as F
from libs.connectors.kafka import FlinkKafkaConnector
from libs.streaming import FlinkStreamingEnvironment
from pyflink.table.window import Tumble

def log_processing():
    streaming_env = FlinkStreamingEnvironment('price_drop_b')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    orders_topic_connector = FlinkKafkaConnector(job_config.get('price_drop_topic'))
    # price_drop_connector = FlinkKafkaConnector(job_config.get('price_drop_topic'))
    ddl_list = [
        orders_topic_connector.generate_kafka_connector('kafka'), 
        # price_drop_connector.generate_kafka_connector('upsert-kafka')
    ]
    for ddl in ddl_list:
        table_env.execute_sql(ddl)

    orders_table = table_env.from_path(orders_topic_connector.table_name)
    orders_table \
    .window(Tumble.over(F.lit(10).seconds).on(F.col("ts")).alias("w")) \
    .group_by(F.col('order_id'), F.col('w')) \
    .select(F.col('order_id'), F.col('order_id').count.alias('count'), F.col("w").start, F.col("w").end) \
    .where(F.col('count') == 1) \
    .execute().print()

if __name__ == '__main__':
    log_processing()