from pyflink.table.udf import udf
from libs.utils.udfs import FlinkUDFs
from libs.connectors.jdbc import FlinkJDBCConnector
from libs.streaming import FlinkStreamingEnvironment
from pyflink.table import DataTypes, expressions as F
from libs.connectors.file_system import FlinkFileSystemConnector

customer_id_list = []
@udf(result_type=DataTypes.INT())
def get_customer_id():
    global customer_id_list
    return int(customer_id_list.pop())

def process():
    global customer_id_list
    customer_id_dict = {}
    streaming_env = FlinkStreamingEnvironment('generate_customers')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    # Work with parallelism 1 at first to avoid duplicates customer id from list
    source_connector_fs = FlinkFileSystemConnector(job_config.get('customers_csv_source'))
    source_connector_jdbc = FlinkJDBCConnector(job_config.get('orders_jdbc_source'))
    sink_connector_jdbc = FlinkJDBCConnector(job_config.get('customers_jdbc_sink'))
    fs_source_ddl = source_connector_fs.generate_fs_connector('csv')
    db_source_ddl = source_connector_jdbc.generate_jdbc_connector()
    sink_ddl = sink_connector_jdbc.generate_jdbc_connector()

    fs_source_table = table_env.execute_sql(fs_source_ddl)
    jdbc_source_table = table_env.execute_sql(db_source_ddl)
    jdbc_sink_table = table_env.execute_sql(sink_ddl)

    panda_table = table_env.from_path(source_connector_jdbc.table_name).to_pandas()
    for customer in panda_table.to_dict(orient='records'):
       customer_id_dict.update({str(customer['CustomerId']): 0})

    # save some memory
    panda_table = []
    table_env.execute_sql(f"DROP TABLE {source_connector_jdbc.table_name}")
    customer_id_list = list(customer_id_dict.keys())
    customer_id_dict = {}
    
    table_env.from_path(source_connector_fs.table_name).select(
        get_customer_id().alias('CustomerId'),
        F.col('first_name').alias('FirstName'),
        F.col('last_name').alias('LastName'),
        F.col('email').alias('Email'),
        FlinkUDFs.get_int_range('customer_type_id', 'customer_ranges').alias('CustomerTypeId'),
        F.to_date(FlinkUDFs.get_join_date()).alias('JoinDate'),
        FlinkUDFs.get_int_range('is_suspended', 'customer_ranges').alias('IsSuspended'),
        FlinkUDFs.get_int_range('suspended_reason_id', 'customer_ranges').alias('SuspendedReasonId'),
        FlinkUDFs.generate_password(16).alias('Password'),
        FlinkUDFs.get_int_range('auth_type_id', 'customer_ranges').alias('AuthTypeId'),
    ).add_columns(
       FlinkUDFs.get_customer_type_text('customer_type', F.col('CustomerTypeId')).alias('CustomerTypeText'),
       FlinkUDFs.get_customer_type_text('suspended', F.col('SuspendedReasonId')).alias('SuspendedReasonText'),
       F.concat('site/profile/', F.col('CustomerId').cast(DataTypes.STRING()), '.jpeg').alias('ProfileImage'),
    ).execute_insert(sink_connector_jdbc.table_name).wait()

    fs_source_table.collect().close()
    jdbc_source_table.collect().close()
    jdbc_sink_table.collect().close()

if __name__ == "__main__":
    process()