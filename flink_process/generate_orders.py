from pyflink.table import expressions as F 
from libs.utils.udfs import FlinkUDFs
from libs.connectors.jdbc import FlinkJDBCConnector
from libs.streaming import FlinkStreamingEnvironment

def process():
    streaming_env = FlinkStreamingEnvironment('generate_orders')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=2)
    vehicles_jdbc_source = FlinkJDBCConnector(job_config.get('vehicles_jdbc_source'))
    orders_jdbc_sink = FlinkJDBCConnector(job_config.get('orders_jdbc_sink'))
    source_ddl = vehicles_jdbc_source.generate_jdbc_connector()
    sink_ddl = orders_jdbc_sink.generate_jdbc_connector()
    source_table = table_env.execute_sql(source_ddl)
    sink_table = table_env.execute_sql(sink_ddl)
    market_info_table = table_env.from_path(vehicles_jdbc_source.table_name)
    market_info_table_execution = market_info_table.select(
        F.col('OrderID'),
        FlinkUDFs.get_site_token(16).alias('SiteToken'),
        FlinkUDFs.get_int_range('price', 'order_ranges').alias('Price'),
        FlinkUDFs.get_int_range('status_id', 'order_ranges').alias('StatusId'),
        FlinkUDFs.get_int_range('customer_id', 'order_ranges').alias('CustomerId'),
    )
    market_info_table_execution.execute_insert(orders_jdbc_sink.table_name).wait()
    source_table.collect().close()
    sink_table.collect().close()
    
if __name__ == "__main__":
    process()