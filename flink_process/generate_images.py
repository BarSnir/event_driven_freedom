import random, string
from pyflink.table.udf import udf
from pyflink.table import ( 
    EnvironmentSettings, 
    TableEnvironment,
    DataTypes,
    expressions as F
)


@udf(result_type=DataTypes.INT())
def get_num_of_slots():
    return random.randrange(1,5)

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

order_id_list = []

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
            'table-name' = 'Orders',
            'username'='root',
            'password'='password',
            'scan.fetch-size'='500'
        );
    """
    sink_ddl = """
        CREATE TABLE MysqlSink (
            `ImageId` VARCHAR,
            `OrderId` VARCHAR,
            `Priority` INT,
            `Url` VARCHAR,
            PRIMARY KEY (ImageId) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'Images',
            'username'='root',
            'password'='password',
            'sink.parallelism' = '4',
            'sink.buffer-flush.interval' = '0',
            'sink.buffer-flush.max-rows' = '10',
            'sink.max-retries' = '10'
        );
    """
    source_table =t_env.execute_sql(source_ddl)
    sink_table = t_env.execute_sql(sink_ddl)
    panda_table = t_env.from_path('MysqlSource').to_pandas()
    for item in panda_table.to_dict(orient='records'):
       order_id_list.append(item['OrderID'])

    CONST_DATATYPE = DataTypes.ROW([
       DataTypes.FIELD("ImageId", DataTypes.STRING()),
       DataTypes.FIELD("OrderId", DataTypes.STRING()),
       DataTypes.FIELD("Priority", DataTypes.INT()),
       DataTypes.FIELD("Url", DataTypes.STRING())
    ])
    rows = []
    for order_id in order_id_list:
       for image_number in range(random.randrange(1,5)):
          letters = string.ascii_lowercase
          token = ''.join(random.choice(letters) for i in range(16))
          rows.append((token, order_id, image_number+1, f'site-url/{token}'))
    t_env.from_elements(rows, CONST_DATATYPE).execute_insert('MysqlSink').wait()
    source_table.collect().close()
    sink_table.collect().close()
if __name__ == "__main__":
    process()