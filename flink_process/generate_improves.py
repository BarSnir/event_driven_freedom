from libs.utils.udfs import FlinkUDFs
from pyflink.table import expressions as F
from libs.connectors.jdbc import FlinkJDBCConnector
from libs.streaming import FlinkStreamingEnvironment
from libs.connectors.datagen import FlinkDatagenConnector

def process():
    streaming_env = FlinkStreamingEnvironment('generate_improves')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    improves_datagen_source = FlinkDatagenConnector(job_config.get('improves_datagen_source'))
    improves_jdbc_sink = FlinkJDBCConnector(job_config.get('improves_jdbc_sink'))
    source_ddl = improves_datagen_source.generate_datagen_connector()
    sink_ddl = improves_jdbc_sink.generate_jdbc_connector()
    source_table = table_env.execute_sql(source_ddl)
    sink_table = table_env.execute_sql(sink_ddl)
    table_env.from_path(improves_datagen_source.table_name).select(
        F.col('ImproveId'),
        FlinkUDFs.get_stage_level_id(F.col('ImproveId')).alias('StageLevel'),
        FlinkUDFs.get_stage_level_text(F.col('ImproveId')).alias('StageText'),
        FlinkUDFs.get_improved_parts(F.col('ImproveId')).alias('PartsImprovedList')
    ).execute_insert(improves_jdbc_sink.table_name).wait()
    source_table.collect().close()
    sink_table.collect().close()

if __name__ == "__main__":
    process()