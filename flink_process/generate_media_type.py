from libs.utils.udfs import FlinkUDFs
from pyflink.table import expressions as F
from libs.streaming import FlinkStreamingEnvironment
from libs.connectors.datagen import FlinkDatagenConnector
from libs.connectors.jdbc import FlinkJDBCConnector

def process():
    streaming_env = FlinkStreamingEnvironment('generate_media_types')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=1)
    media_type_source_connector = FlinkDatagenConnector(job_config.get('media_type_source'))
    media_type_jdbc_sink_connector = FlinkJDBCConnector(job_config.get('media_type_jdbc_sink'))
    source_ddl = media_type_source_connector.generate_datagen_connector()
    sink_ddl = media_type_jdbc_sink_connector.generate_jdbc_connector()
    source_table = table_env.execute_sql(source_ddl)
    sink_table = table_env.execute_sql(sink_ddl)
    table_env.from_path(media_type_source_connector.table_name).select(
       F.col('MediaTypeID'),
       F.col('AvailableDiskSlot'),
       FlinkUDFs.get_slots_type().alias('UsbSlotType'),
       FlinkUDFs.get_num_of_slots().alias('UsbSlots'),
       F.col('IsTouchDisplay'),
    ).execute_insert(media_type_jdbc_sink_connector.table_name).wait()
    source_table.collect().close()
    sink_table.collect().close()
if __name__ == "__main__":
    process()