from libs.utils.udfs import FlinkUDFs
from pyflink.table import expressions as F
from libs.streaming import FlinkStreamingEnvironment
from libs.connectors.datagen import FlinkDatagenConnector
from libs.connectors.jdbc import FlinkJDBCConnector

def process():
    streaming_env = FlinkStreamingEnvironment('generate_statuses')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    status_source_datagen_connector = FlinkDatagenConnector(job_config.get('status_source_datagen_connector'))
    status_sink_datagen_connector = FlinkJDBCConnector(job_config.get('status_sink_datagen_connector'))
    source_ddl = status_source_datagen_connector.generate_datagen_connector()
    sink_ddl = status_sink_datagen_connector.generate_jdbc_connector()
    source_table = table_env.execute_sql(source_ddl)
    sink_table = table_env.execute_sql(sink_ddl)
    table_env.from_path(status_source_datagen_connector.table_name).select(
        F.col('StatusId').alias('StatusID'),
        FlinkUDFs.get_status_text(F.col('StatusId')).alias('StatusText')
    ).execute_insert(status_sink_datagen_connector.table_name).wait()
    source_table.collect().close()
    sink_table.collect().close()

if __name__ == "__main__":
    process()