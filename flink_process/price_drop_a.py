from pyflink.table import expressions as F
from libs.connectors.kafka import FlinkKafkaConnector
from libs.streaming import FlinkStreamingEnvironment

def log_processing():
    streaming_env = FlinkStreamingEnvironment('price_drop_a')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    orders_topic_connector = FlinkKafkaConnector(job_config.get('orders_topic'))
    price_drop_connector = FlinkKafkaConnector(job_config.get('price_drop_topic'))
    ddl_list = [
        orders_topic_connector.generate_kafka_connector('kafka'), 
        price_drop_connector.generate_kafka_connector('upsert-kafka')
    ]
    for ddl in ddl_list:
        table_env.execute_sql(ddl)

    orders_table = table_env.from_path(orders_topic_connector.table_name)
    orders_table = orders_table.filter(F.col('op') == 'u')
    orders_table = orders_table.select(
        F.col('after').get('OrderId').alias('order_id'),
        F.col('before').get('Price').alias('prev_price'),
        F.col('after').get('Price').alias('after_price')
    )
    alert_table = orders_table.group_by(
        F.col('order_id')
    ).select(
        F.col('order_id'),
        F.col('prev_price').first_value.alias('first_price'),
        F.col('after_price').last_value.alias('last_price'),
        F.col('prev_price').last_value.alias('prev_price'),
    ).filter(
        F.col('last_price') < F.col('prev_price') * 0.9 \
        and \
        F.col('prev_price') < F.col('first_price') * 0.9
    )
    alert_table.select(
        F.col('order_id'),
        F.col('first_price'),
        F.col('last_price'),
        F.col('prev_price'),
        (
            ((F.col('first_price') - F.col('last_price')) * 100)/ (F.col('first_price'))
        ).alias('price_drop_percentage')
    ).execute_insert(price_drop_connector.table_name)

if __name__ == '__main__':
    log_processing()