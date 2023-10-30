from pyflink.table import ( 
    EnvironmentSettings, 
    TableEnvironment,
    expressions as F
)

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
    .set('parallelism.default', '1')
    source_ddl = """
        CREATE TABLE AuthSource (
            `AuthTypeID` INT
        ) WITH (
            'connector' = 'datagen', 
            'number-of-rows' = '4',
            'fields.AuthTypeID.kind' = 'sequence',
            'fields.AuthTypeID.start' = '1',
            'fields.AuthTypeID.end' = '4'
        );
    """
    sink_ddl = """
        CREATE TABLE MysqlSink (
            `AuthTypeID` INT,
            `AuthByGoogle` INT,
            `AuthByApple` INT,
            `AuthByFacebook` INT,
            `AuthByTwitter` INT,
            PRIMARY KEY (AuthTypeID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'AuthTypes',
            'username'='root',
            'password'='password',
            'sink.parallelism' = '1',
            'sink.buffer-flush.interval' = '0',
            'sink.buffer-flush.max-rows' = '10',
            'sink.max-retries' = '10'
        );
    """
    source_table =t_env.execute_sql(source_ddl)
    sink_table = t_env.execute_sql(sink_ddl)
    t_env.from_path('AuthSource').select(
        F.col('AuthTypeID'),
        F.if_then_else(F.col('AuthTypeID') == 1, 1, 0).alias('AuthByGoogle'),
        F.if_then_else(F.col('AuthTypeID') == 2, 1, 0).alias('AuthByApple'),
        F.if_then_else(F.col('AuthTypeID') == 3, 1, 0).alias('AuthByFacebook'),
        F.if_then_else(F.col('AuthTypeID') == 4, 1, 0).alias('AuthByTwitter'),
    ).execute_insert('MysqlSink').wait()
    source_table.collect().close()
    sink_table.collect().close()
if __name__ == "__main__":
    process()