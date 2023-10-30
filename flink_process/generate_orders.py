import random, string
from pyflink.table.udf import udf
from pyflink.table import ( 
    EnvironmentSettings, 
    TableEnvironment,
    DataTypes,
    expressions as F
)

@udf(result_type=DataTypes.STRING())
def get_site_token(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))

@udf(result_type=DataTypes.INT())
def get_int_range(text):
    col_range_dict = {
        'price': {'low': 1000*10, 'high': 45000*10},
        'status_id': {'low': 1, 'high': 11},
        'customer_id': {'low': 1, 'high': 8000},
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
    .set('parallelism.default', '4')

    source_ddl = """
        CREATE TABLE MysqlSource (
            `OrderID` VARCHAR,
            PRIMARY KEY (OrderID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'Vehicles',
            'username'='root',
            'password'='password',
            'scan.fetch-size'='500'
        );
    """ 
    sink_ddl = """
        CREATE TABLE MysqlSink (
            `OrderID` VARCHAR,
            `SiteToken` VARCHAR,
            `Price` INT,
            `StatusId` INT,
            `CustomerId` INT,
            PRIMARY KEY (OrderID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'Orders',
            'username'='root',
            'password'='password',
            'sink.parallelism' = '5',
            'sink.buffer-flush.interval' = '0',
            'sink.buffer-flush.max-rows' = '10',
            'sink.max-retries' = '10'
        );
    """
    source_table =t_env.execute_sql(source_ddl)
    sink_table = t_env.execute_sql(sink_ddl)
    market_info_table = t_env.from_path('MysqlSource')
    market_info_table_execution = market_info_table.select(
        F.col('OrderID'),
        get_site_token(16).alias('SiteToken'),
        get_int_range('price').alias('Price'),
        get_int_range('status_id').alias('StatusId'),
        get_int_range('customer_id').alias('CustomerId'),
    )
    market_info_table_execution.execute_insert("MysqlSink").wait()
    source_table.collect().close()
    sink_table.collect().close()
if __name__ == "__main__":
    process()