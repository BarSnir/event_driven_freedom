import random
from pyflink.table import expressions as F
from libs.utils.udfs import FlinkUDFs
from libs.connectors.jdbc import FlinkJDBCConnector
from libs.streaming import FlinkStreamingEnvironment

def process():
    streaming_env = FlinkStreamingEnvironment('generate_vehicles')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=2)
    market_info_jdbc_source = FlinkJDBCConnector(job_config.get('market_info_jdbc_source'))
    vehicles_jdbc_sink = FlinkJDBCConnector(job_config.get('vehicles_jdbc_sink'))
    source_ddl = market_info_jdbc_source.generate_jdbc_connector()
    sink_ddl = vehicles_jdbc_sink.generate_jdbc_connector()
    market_info_source = table_env.execute_sql(source_ddl)
    vehicles_sink = table_env.execute_sql(sink_ddl)
    market_info_table = table_env.from_path(market_info_jdbc_source.table_name)
    for i in range(10):
        market_info_table_execution = market_info_table.select(
            F.uuid().alias('VehicleId'),
            FlinkUDFs.get_int_range('km', 'vehicles_ranges').alias('KM'),
            FlinkUDFs.get_int_range('prev_owner_number', 'vehicles_ranges').alias('PrevOwnerNumber'),
            F.col('MarketInfoId'),
            FlinkUDFs.get_int_range('media_type_id', 'vehicles_ranges').alias('MediaTypeId'),
            (F.col('Year')+ random.randrange(0,2)).alias('YearOnRoad'),
            F.to_date(FlinkUDFs.generate_test_date()).alias('TestDate'),
            FlinkUDFs.get_int_range('improve_id', 'vehicles_ranges').alias('ImproveId'),
        ).add_columns(F.col('VehicleId').replace('-','').alias('OrderId'))
        market_info_table_execution.execute_insert(vehicles_jdbc_sink.table_name).wait()
    market_info_source.collect().close()
    vehicles_sink.collect().close()
if __name__ == "__main__":
    process()