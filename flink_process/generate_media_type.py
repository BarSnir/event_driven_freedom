import random
from pyflink.table.udf import udf
from pyflink.table import ( 
    EnvironmentSettings, 
    TableEnvironment,
    DataTypes,
    expressions as F
)

@udf(result_type=DataTypes.STRING())
def get_slots_type():
    index_usb_slot = random.randrange(0,2)
    slots_type_list = ['B', 'C']
    return slots_type_list[index_usb_slot] 

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

def process():
    environment_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(environment_settings)
    t_env.get_config().set('pipeline.jars',get_jars_full_path()) \
    .set('python.fn-execution.bundle.time', '100000') \
    .set('python.fn-execution.bundle.size', '10') \
    .set('parallelism.default', '1')

    source_ddl = """
        CREATE TABLE MediaTypeSource (
            `MediaTypeID` INT,
            `AvailableDiskSlot` BOOLEAN,
            `IsTouchDisplay` BOOLEAN
        ) WITH (
            'connector' = 'datagen', 
            'number-of-rows' = '10',
            'fields.MediaTypeID.kind' = 'sequence',
            'fields.MediaTypeID.start' = '1',
            'fields.MediaTypeID.end' = '10'
        );
    """
    sink_ddl = """
        CREATE TABLE MysqlSink (
            `MediaTypeID` INT,
            `AvailableDiskSlot` BOOLEAN,
            `UsbSlotType` VARCHAR,
            `UsbSlots` INT,
            `IsTouchDisplay` BOOLEAN,
            PRIMARY KEY (MediaTypeID) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'MediaType',
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
    t_env.from_path('MediaTypeSource').select(
       F.col('MediaTypeID'),
       F.col('AvailableDiskSlot'),
       get_slots_type().alias('UsbSlotType'),
       get_num_of_slots().alias('UsbSlots'),
       F.col('IsTouchDisplay'),
    ).execute_insert("MysqlSink").wait()
    source_table.collect().close()
    sink_table.collect().close()
if __name__ == "__main__":
    process()