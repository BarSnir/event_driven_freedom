from pyflink.table.udf import udf
from pyflink.table import ( 
    EnvironmentSettings, 
    TableEnvironment,
    DataTypes,
    expressions as F
)

@udf(result_type=DataTypes.STRING())
def get_stage_level_text(id):
    stage_level_dict = {
        8: 'SOFTWARE',
        9: 'SOFTWARE+ENGINE',
        10: 'SOFTWARE+ENGINE+SHIELD',
    }
    return stage_level_dict.get(id, None)

@udf(result_type=DataTypes.INT())
def get_stage_level_id(id):
    stage_level_dict = {
        8: 1,
        9: 2,
        10: 3,
    }
    return stage_level_dict.get(id, None)

@udf(result_type=DataTypes.STRING())
def get_improved_parts(id):
    improve_dict = {
        1: ['media'],
        2: ['media', 'speaker'],
        3: ['media', 'speaker', 'fuel-system'],
        4: ['media', 'speaker', 'fuel-system', 'back-seats-media'],
        5: ['media', 'speaker', 'fuel-system', 'back-seats-media', 'shaded-windows'],
        6: ['media', 'speaker', 'fuel-system', 'back-seats-media', 'shaded-windows', 'engine-cooler'],
        7: ['media', 'speaker', 'fuel-system', 'back-seats-media', 'shaded-windows', 'engine-cooler', 'engine-fan'],
        8: ['media', 'speaker', 'fuel-system', 'back-seats-media', 'shaded-windows', 'engine-cooler', 'engine-fan', 'stage-1'],
        9: ['media', 'speaker', 'fuel-system', 'back-seats-media', 'shaded-windows', 'engine-cooler', 'engine-fan', 'stage-1', 'stage-2'],
        10: ['media', 'speaker', 'fuel-system', 'back-seats-media', 'shaded-windows', 'engine-cooler', 'engine-fan', 'stage-1', 'stage-2', 'stage-3'],
    }
    return str(improve_dict.get(id))

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
        CREATE TABLE ImproveSource (
            `ImproveId` INT
        ) WITH (
            'connector' = 'datagen', 
            'number-of-rows' = '10',
            'fields.ImproveId.kind' = 'sequence',
            'fields.ImproveId.start' = '1',
            'fields.ImproveId.end' = '10'
        );
    """
    sink_ddl = """
        CREATE TABLE MysqlSink (
            `ImproveId` INT,
            `StageLevel` INT,
            `StageText` VARCHAR,
            `PartsImprovedList` VARCHAR,
            PRIMARY KEY (ImproveId) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'Improves',
            'username'='root',
            'password'='password',
            'sink.parallelism' = '1',
            'sink.buffer-flush.interval' = '0',
            'sink.buffer-flush.max-rows' = '10',
            'sink.max-retries' = '10'
        );
    """
    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    t_env.from_path('ImproveSource').select(
        F.col('ImproveId'),
        get_stage_level_id(F.col('ImproveId')).alias('StageLevel'),
        get_stage_level_text(F.col('ImproveId')).alias('StageText'),
        get_improved_parts(F.col('ImproveId')).alias('PartsImprovedList')
    ).execute_insert("MysqlSink").wait(100000)
if __name__ == "__main__":
    process()