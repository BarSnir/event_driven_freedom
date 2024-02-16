import random
from pyflink.table.udf import udf
from pyflink.table import ( 
    DataTypes,
    expressions as F
)
from libs.streaming import FlinkStreamingEnvironment
from libs.connectors.file_system import FlinkFileSystemConnector
import logging
logger = logging.getLogger()

@udf(result_type=DataTypes.BIGINT())
def calc_gemetria(text):
    total = 0
    clean_text = text.replace(' ','') \
    .replace('-','') \
    .replace('&', 'and') \
    .replace('.','') \
    .replace('(', '') \
    .replace(')', '') \
    .replace('/', '') \
    .lower()
    abc_gemetria = {'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 5, 'f': 6, 'g': 7, 'h': 8, 'i': 9, 'j': 10, 'k': 20, 'l': 30, 'm': 40, 'n': 50, 'o': 60, 'p': 70, 'q': 80, 'r': 90, 's': 100, 't': 200, 'u': 300, 'v': 400, 'w': 500, 'x': 600, 'y': 700, 'z': 800}
    for letter in clean_text:
        num = abc_gemetria.get(letter, None)
        if num is None:
            total = total + int(letter)
            continue
        total = total + num
    return total

@udf(result_type=DataTypes.TINYINT())
def get_mysql_boolean(col_condition_name, col_condition_value):
    col_condition_dict = {
        'Year_Sunroof': 2008,
        'Year_MagnesiumWheels': 2005,
        'Year_ReversSensors': 2002,
        'Year_Hybrid': 2012,
        'Year_CruseControl': 2005,
        'Year_PowerWheel': 1999,
        'Year_FullyAutonomic': 2021
    }
    col_condition = col_condition_dict.get(col_condition_name, None)
    if col_condition is not None and col_condition > col_condition_value:
        return 0
    return random.randrange(0, 2)

@udf(result_type=DataTypes.INT())
def get_int_range(text):
    col_range_dict = {
        'airbags': {'low': 0, 'high': 5},
        'doors': {'low': 2, 'high': 5},
        'environment_friendly_level': {'low': 0, 'high': 11},
        'security_test_level': {'low': 0, 'high': 11},
        'horse_power': {'low': 8*10, 'high': 45*10},
        'market_price': {'low': 1000*10, 'high': 45000*10},
    }
    col_range = col_range_dict.get(text, 0)
    value = random.randrange(
        col_range.get('low'), 
        col_range.get('high')
    )
    return value

def process():
    streaming_env = FlinkStreamingEnvironment('market_info')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment()
    source_connector = FlinkFileSystemConnector(job_config.get('market_info_csv'))
    source_ddl = source_connector.generate_fs_connector('csv')
    sink_ddl = """
        CREATE TABLE MysqlSink (
            `MarketInfoId` VARCHAR,
            `ManufacturerId` BIGINT,
            `ManufacturerText` VARCHAR,
            `ModelId` BIGINT,
            `ModelText` VARCHAR,
            `Year` INT,
            `MarketPrice` INT,
            `AirBags` INT,
            `SunRoof` TINYINT,
            `MagnesiumWheels` TINYINT,
            `ReversSensors` TINYINT,
            `ABS` TINYINT,
            `Hybrid` TINYINT,
            `Doors` INT,
            `EnvironmentFriendlyLevel` INT,
            `SecurityTestLevel` INT,
            `HorsePower` INT,
            `CruseControl` TINYINT,
            `PowerWheel` TINYINT,
            `FullyAutonomic` TINYINT,
            PRIMARY KEY (MarketInfoId) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'MarketInfo',
            'username'='root',
            'password'='password',
            'sink.parallelism' = '2',
            'sink.buffer-flush.interval' = '0',
            'sink.buffer-flush.max-rows' = '10',
            'sink.max-retries' = '10'
        );
    """
    source_table = table_env.execute_sql(source_ddl)
    sink_table = table_env.execute_sql(sink_ddl)
    market_info_table = table_env.from_path(source_connector.table_name)
    market_info_table = market_info_table.select(
        F.col('make').alias('ManufacturerText'),
        F.col('model').alias('ModelText'),
        F.col('year').cast(DataTypes.INT()).alias('Year')
    ).filter(F.col('ManufacturerText') != 'make')
    market_info_table.select(
        F.uuid().alias('MarketInfoId'),
        calc_gemetria(F.col('ManufacturerText')).alias('ManufacturerId'),
        F.col('ManufacturerText'),
        calc_gemetria(F.concat(F.col('ManufacturerText'), F.col('ModelText'))).alias('ModelId'),
        F.col('ModelText'),
        F.col('Year'),
        get_int_range('market_price').alias('MarketPrice'),
        get_int_range('airbags').alias('AirBags'),
        get_mysql_boolean('Year_Sunroof',F.col('Year')).alias('SunRoof'),
        get_mysql_boolean('Year_MagnesiumWheels',F.col('Year')).alias('MagnesiumWheels'),
        get_mysql_boolean('Year_ReversSensors',F.col('Year')).alias('ReversSensors'),
        get_mysql_boolean(None, 0).alias('ABS'),
        get_mysql_boolean('Year_Hybrid',F.col('Year')).alias('Hybrid'),
        get_int_range(('doors')).alias('Doors'),
        get_int_range(('environment_friendly_level')).alias('EnvironmentFriendlyLevel'),
        get_int_range(('security_test_level')).alias('SecurityTestLevel'),
        get_int_range(('horse_power')).alias('HorsePower'),
        get_mysql_boolean('Year_CruseControl', F.col('Year')).alias('CruseControl'),
        get_mysql_boolean('Year_PowerWheel', F.col('Year')).alias('PowerWheel'),
        get_mysql_boolean('Year_FullyAutonomic' ,F.col('Year')).alias('FullyAutonomic'),
    ).execute_insert('MysqlSink').wait()
    source_table.collect().close()
    sink_table.collect().close()
if __name__ == "__main__":
    process()