from pyflink.table.udf import udf
from pyflink.table import ( 
    EnvironmentSettings, 
    TableEnvironment,
    DataTypes,
    expressions as F
)

@udf(result_type=DataTypes.STRING())
def get_text(id):
    status_dict = {
       '1': 'activate',
       '2': 'waiting_for_cms_approve',
       '3': 'waiting_for_payment_approve',
       '4': 'freeze',
       '5': 'sold',
       '6': 'in_publish_queue',
       '7': 'deactivate',
    }
    return status_dict.get(str(id))

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
        CREATE TABLE StatusesSource (
            `StatusId` INT
        ) WITH (
            'connector' = 'datagen', 
            'number-of-rows' = '7',
            'fields.StatusId.kind' = 'sequence',
            'fields.StatusId.start' = '1',
            'fields.StatusId.end' = '7'
        );
    """
    sink_ddl = """
        CREATE TABLE MysqlSink (
            `StatusID` INT,
            `StatusText` VARCHAR,
            PRIMARY KEY (StatusID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'Statuses',
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
    t_env.from_path('StatusesSource').select(
        F.col('StatusId').alias('StatusID'),
        get_text(F.col('StatusId')).alias('StatusText')
    ).execute_insert('MysqlSink').wait(50000)
if __name__ == "__main__":
    process()