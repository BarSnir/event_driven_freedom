from libs.utils.udfs import FlinkUDFs
from libs.streaming import FlinkStreamingEnvironment
from libs.connectors.jdbc import FlinkJDBCConnector
from pyflink.table import DataTypes, expressions as F
from libs.connectors.file_system import FlinkFileSystemConnector

def process():
    streaming_env = FlinkStreamingEnvironment('market_info')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=2)
    source_connector = FlinkFileSystemConnector(job_config.get('market_info_csv'))
    sink_jdbc_connector = FlinkJDBCConnector(job_config.get('market_info_mysql'))
    market_info_csv_ddl = source_connector.generate_fs_connector('csv')
    market_info_jdbc_ddl = sink_jdbc_connector.generate_jdbc_connector()
    market_info_csv_source = table_env.execute_sql(market_info_csv_ddl)
    market_info_jdbc_sink = table_env.execute_sql(market_info_jdbc_ddl)
    market_info_table = table_env.from_path(source_connector.table_name)
    market_info_table = market_info_table.select(
        F.col('make').alias('ManufacturerText'),
        F.col('model').alias('ModelText'),
        F.col('year').cast(DataTypes.INT()).alias('Year')
    ).filter(F.col('ManufacturerText') != 'make')
    market_info_table.select(
        F.uuid().alias('MarketInfoId'),
        FlinkUDFs.calc_gemetria(F.col('ManufacturerText')).alias('ManufacturerId'),
        F.col('ManufacturerText'),
        FlinkUDFs.calc_gemetria(F.concat(F.col('ManufacturerText'), F.col('ModelText'))).alias('ModelId'),
        F.col('ModelText'),
        F.col('Year'),
        FlinkUDFs.get_int_range('market_price', 'market_info_ranges').alias('MarketPrice'),
        FlinkUDFs.get_int_range('airbags', 'market_info_ranges').alias('AirBags'),
        FlinkUDFs.get_mysql_boolean('Year_Sunroof',F.col('Year'), 'market_info_year').alias('SunRoof'),
        FlinkUDFs.get_mysql_boolean('Year_MagnesiumWheels',F.col('Year'), 'market_info_year').alias('MagnesiumWheels'),
        FlinkUDFs.get_mysql_boolean('Year_ReversSensors',F.col('Year'), 'market_info_year').alias('ReversSensors'),
        FlinkUDFs.get_mysql_boolean(None, 0, 'market_info_year').alias('ABS'),
        FlinkUDFs.get_mysql_boolean('Year_Hybrid', F.col('Year'), 'market_info_year').alias('Hybrid'),
        FlinkUDFs.get_int_range('doors', 'market_info_ranges').alias('Doors'),
        FlinkUDFs.get_int_range('environment_friendly_level', 'market_info_ranges').alias('EnvironmentFriendlyLevel'),
        FlinkUDFs.get_int_range('security_test_level', 'market_info_ranges').alias('SecurityTestLevel'),
        FlinkUDFs.get_int_range('horse_power', 'market_info_ranges').alias('HorsePower'),
        FlinkUDFs.get_mysql_boolean('Year_CruseControl', F.col('Year'), 'market_info_year').alias('CruseControl'),
        FlinkUDFs.get_mysql_boolean('Year_PowerWheel', F.col('Year'), 'market_info_year').alias('PowerWheel'),
        FlinkUDFs.get_mysql_boolean('Year_FullyAutonomic' ,F.col('Year'), 'market_info_year').alias('FullyAutonomic'),
    ).execute_insert(sink_jdbc_connector.table_name).wait()
    market_info_csv_source.collect().close()
    market_info_jdbc_sink.collect().close()
if __name__ == "__main__":
    process()