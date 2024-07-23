from libs.connectors.kafka import FlinkKafkaConnector
from libs.streaming import FlinkStreamingEnvironment

def log_process_sql():
    streaming_env = FlinkStreamingEnvironment('price_drop_b')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    orders_topic_connector = FlinkKafkaConnector(job_config.get('price_drop_topic_source'))
    price_drop_connector = FlinkKafkaConnector(job_config.get('price_drop_alert_topic'))
    ddl_list = [
        orders_topic_connector.generate_kafka_connector('kafka'), 
        price_drop_connector.generate_kafka_connector('upsert-kafka')
    ]
    for ddl in ddl_list:
        table_env.execute_sql(ddl)

    table_env.sql_query(f"""
        SELECT 
            order_id, 
            SUM(last_price) AS last_price,
            SUM(prev_price) AS prev_price,
            SUM(price_drop_percentage) AS price_drop_percentage,
            window_start, 
            window_end
        FROM TABLE(
            CUMULATE(
                TABLE {orders_topic_connector.table_name},
                DESCRIPTOR(ts),
                INTERVAL '1' SECONDS,
                INTERVAL '24' HOURS
            )
        )
        GROUP BY 
            order_id, 
            window_start, 
            window_end
        HAVING 
            COUNT(last_price) = 1;
    """).execute_insert(price_drop_connector.table_name)
    

if __name__ == '__main__':
    log_process_sql()