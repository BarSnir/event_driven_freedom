from pyflink.table import expressions as F
from libs.streaming import FlinkStreamingEnvironment
from libs.connectors.jdbc import FlinkJDBCConnector
from libs.connectors.datagen import FlinkDatagenConnector

def process():
    streaming_env = FlinkStreamingEnvironment('generate_auth_types')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    datagen_source_connector = FlinkDatagenConnector(job_config.get('auth_types_datagen_source'))
    jdbc_source_connector = FlinkJDBCConnector(job_config.get('auth_types_jdbc_sink'))
    source_ddl = datagen_source_connector.generate_datagen_connector()
    sink_ddl = jdbc_source_connector.generate_jdbc_connector()
    source_table = table_env.execute_sql(source_ddl)
    sink_table = table_env.execute_sql(sink_ddl)
    table_env.from_path(datagen_source_connector.table_name).select(
        F.col('AuthTypeID'),
        F.if_then_else(F.col('AuthTypeID') == 1, 1, 0).alias('AuthByGoogle'),
        F.if_then_else(F.col('AuthTypeID') == 2, 1, 0).alias('AuthByApple'),
        F.if_then_else(F.col('AuthTypeID') == 3, 1, 0).alias('AuthByFacebook'),
        F.if_then_else(F.col('AuthTypeID') == 4, 1, 0).alias('AuthByTwitter'),
    ).execute_insert(jdbc_source_connector.table_name).wait()
    source_table.collect().close()
    sink_table.collect().close()
    
if __name__ == "__main__":
    process()