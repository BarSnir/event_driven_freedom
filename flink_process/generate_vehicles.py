import random
from datetime import date
from pyflink.table.udf import udf
from pyflink.table import ( 
    EnvironmentSettings, 
    TableEnvironment,
    DataTypes,
    expressions as F
)

@udf(result_type=DataTypes.STRING())
def generate_test_date():
    today = str(date.today())
    curr_year = today[:4]
    return curr_year+'-'+str(random.randrange(1,12))+'-'+str(random.randrange(1,29))

@udf(result_type=DataTypes.INT())
def get_int_range(text):
    col_range_dict = {
        'km': {'low': 1000*10, 'high': 45000*10},
        'prev_owner_number': {'low':0, 'high': 7},
        'media_type_id': {'low': 1, 'high': 11},
        'improve_id': {'low': 1, 'high': 11},
    }
    col_range = col_range_dict.get(text, 0)
    value = random.randrange(
        col_range.get('low'), 
        col_range.get('high')
    )
    return value

def get_jars_path():
    return f'file:///opt/flink/opt/'

def get_jars_full_path() -> str:
  jars_path = get_jars_path()
  jars = [
    'flink-connector-jdbc-3.1.0-1.17.jar;',
    'mysql-connector-java-5.1.9.jar;',
    'flink-python-1.17.1.jar'
  ]
  full_str = ''
  for jar in jars:
    full_str = f'{full_str}{jars_path}{jar}'
  return full_str

def process():
    environment_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(environment_settings)
    t_env.get_config().set('pipeline.jars',get_jars_full_path()) \
    .set('python.fn-execution.bundle.time', '100000') \
    .set('python.fn-execution.bundle.size', '10') \
    .set('parallelism.default', '8')

    source_ddl = """
        CREATE TABLE MysqlSource (
            `MarketInfoId` VARCHAR,
            `Year` INT,
            PRIMARY KEY (MarketInfoId) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'MarketInfo',
            'username'='root',
            'password'='password',
            'scan.fetch-size'='500'
        );
    """ 
    sink_ddl = """
        CREATE TABLE MysqlSink (
            `VehicleId` CHAR(36),
            `KM` INT,
            `PrevOwnerNumber` INT,
            `MarketInfoId` VARCHAR,
            `MediaTypeId` INT,
            `YearOnRoad` INT,
            `TestDate` DATE,
            `ImproveId` INT,
            `OrderId` VARCHAR NOT NULL,
            PRIMARY KEY (MarketInfoId) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'Vehicles',
            'username'='root',
            'password'='password',
            'sink.parallelism' = '8',
            'sink.buffer-flush.interval' = '0',
            'sink.buffer-flush.max-rows' = '10',
            'sink.max-retries' = '10'
        );
    """
    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    market_info_table = t_env.from_path('MysqlSource')
    for i in range(10):
        market_info_table_execution = market_info_table.select(
            F.uuid().alias('VehicleId'),
            get_int_range('km').alias('KM'),
            get_int_range('prev_owner_number').alias('PrevOwnerNumber'),
            F.col('MarketInfoId'),
            get_int_range('media_type_id').alias('MediaTypeId'),
            (F.col('Year')+ random.randrange(0,2)).alias('YearOnRoad'),
            F.to_date(generate_test_date()).alias('TestDate'),
            get_int_range('improve_id').alias('ImproveId'),
        ).add_columns(F.col('VehicleId').replace('-','').alias('OrderId'))
        market_info_table_execution.execute_insert("MysqlSink").wait(60000)
if __name__ == "__main__":
    process()