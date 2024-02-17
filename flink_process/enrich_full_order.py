from pyflink.table import expressions as F 
from libs.connectors.kafka import FlinkKafkaConnector
from libs.streaming import FlinkStreamingEnvironment

def log_processing():
    streaming_env = FlinkStreamingEnvironment('enrich_full_orders')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    orders_topic_connector = FlinkKafkaConnector(job_config.get('orders_topic'))
    aggregate_topic_connector = FlinkKafkaConnector(job_config.get('aggregate_topic'))
    customers_topic_connector = FlinkKafkaConnector(job_config.get('customers_topic'))
    full_order_topic_connector = FlinkKafkaConnector(job_config.get('full_order_topic'))
    kafka_orders_ddl = orders_topic_connector.generate_kafka_connector('kafka')
    kafka_images_ddl = aggregate_topic_connector.generate_kafka_connector('kafka')
    kafka_customers_ddl = customers_topic_connector.generate_kafka_connector('kafka')
    kafka_full_order_ddl =  full_order_topic_connector.generate_kafka_connector('upsert-kafka')

    for ddl in [kafka_orders_ddl, kafka_images_ddl, kafka_customers_ddl, kafka_full_order_ddl]:
        table_env.execute_sql(ddl)

    order_table = table_env.from_path(orders_topic_connector.table_name)
    images_table = table_env.from_path(aggregate_topic_connector.table_name)
    customers = table_env.from_path(customers_topic_connector.table_name) \
    .select(
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
    full_order = order_table.join(images_table) \
    .where(F.col('OrderId') == F.col('images_order_id')) \
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
    .drop_columns(F.col('customer_id_table'))
    full_order.execute_insert(full_order_topic_connector.table_name).wait()

if __name__ == '__main__':
    log_processing()