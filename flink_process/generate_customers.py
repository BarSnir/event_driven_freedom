import random, string
from pyflink.table.udf import udf
from pyflink.table import ( 
    EnvironmentSettings, 
    TableEnvironment,
    DataTypes,
    expressions as F
)

customer_id_list = []


@udf(result_type=DataTypes.INT())
def get_int_range(text):
    col_range_dict = {
        'customer_type_id': {'low': 1, 'high': 6},
        'is_suspended': {'low': 0, 'high': 2},
        'suspended_reason_id': {'low': 1, 'high': 7},
        'auth_type_id': {'low': 1, 'high': 5},
    }
    col_range = col_range_dict.get(text, 0)
    value = random.randrange(
        col_range.get('low'), 
        col_range.get('high')
    )
    return value

@udf(result_type=DataTypes.STRING())
def get_text(dict_type, id):
    suspended_dict = {
       '1': 'impostor',
       '2': 'payment',
       '3': 'hacker',
       '4': 'scam',
       '5': 'bot',
       '6': 'spam',
    }
    customer_type_dict = {
       '1': 'regular',
       '2': 'bronze',
       '3': 'silver',
       '4': 'gold',
       '5': 'platinum',
    }
    abstract_dict = locals()[f'{dict_type}_dict']
    return abstract_dict.get(str(id))

@udf(result_type=DataTypes.INT())
def get_customer_id():
    global customer_id_list
    return int(customer_id_list.pop())

@udf(result_type=DataTypes.STRING())
def get_join_date():
    year = str(random.randrange(2020,2024))
    month = str(random.randrange(1,12))
    day = str(random.randrange(1,29))
    return year+'-'+month+'-'+day

@udf(result_type=DataTypes.STRING())
def generate_password(length):
   letters = string.ascii_lowercase
   return ''.join(random.choice(letters) for i in range(length))

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
    global customer_id_list
    customer_id_dict = {}
    environment_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(environment_settings)
    t_env.get_config().set('pipeline.jars',get_jars_full_path()) \
    .set('python.fn-execution.bundle.time', '100000') \
    .set('python.fn-execution.bundle.size', '10') \
    .set('parallelism.default', '1')
    # work with parallelism 1 at first to avoid duplicates customer id from list
    fs_source_ddl = """
        CREATE TABLE FileSystemSource (
            `first_name` VARCHAR,
            `last_name` VARCHAR,
            `email` VARCHAR
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/flink/datasets/customers',
            'format' = 'csv'
        );
    """
    db_source_ddl = """
        CREATE TABLE MysqlSource (
            `CustomerId` INT,
            PRIMARY KEY (CustomerId) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'Orders',
            'username'='root',
            'password'='password',
            'scan.fetch-size'='500'
    );"""

    sink_ddl = """
        CREATE TABLE MysqlSink (
            `CustomerId` INT,
            `FirstName` VARCHAR,
            `LastName` VARCHAR,
            `Email` VARCHAR,
            `CustomerTypeId` INT,
            `JoinDate` DATE,
            `IsSuspended` INT,
            `SuspendedReasonId` INT,
            `Password` VARCHAR,
            `AuthTypeId` INT,
            `CustomerTypeText` VARCHAR,
            `SuspendedReasonText` VARCHAR,
            `ProfileImage` VARCHAR,
            PRIMARY KEY (CustomerId) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'Customers',
            'username'='root',
            'password'='password',
            'sink.parallelism' = '8',
            'sink.buffer-flush.interval' = '0',
            'sink.buffer-flush.max-rows' = '10',
            'sink.max-retries' = '10'
        );
    """
    t_env.execute_sql(fs_source_ddl)
    t_env.execute_sql(db_source_ddl)
    t_env.execute_sql(sink_ddl)

    
    panda_table = t_env.from_path('MysqlSource').to_pandas()
    for customer in panda_table.to_dict(orient='records'):
       customer_id_dict.update({str(customer['CustomerId']): 0})

    # save some memory
    panda_table = []
    t_env.execute_sql("DROP TABLE MysqlSource")
    customer_id_list = list(customer_id_dict.keys())
    customer_id_dict = {}
    
    t_env.from_path('FileSystemSource').select(
        get_customer_id().alias('CustomerId'),
        F.col('first_name').alias('FirstName'),
        F.col('last_name').alias('LastName'),
        F.col('email').alias('Email'),
        get_int_range('customer_type_id').alias('CustomerTypeId'),
        F.to_date(get_join_date()).alias('JoinDate'),
        get_int_range('is_suspended').alias('IsSuspended'),
        get_int_range('suspended_reason_id').alias('SuspendedReasonId'),
        generate_password(16).alias('Password'),
        get_int_range('auth_type_id').alias('AuthTypeId'),
    ).add_columns(
       get_text('customer_type', F.col('CustomerTypeId')).alias('CustomerTypeText'),
       get_text('suspended', F.col('SuspendedReasonId')).alias('SuspendedReasonText'),
       F.concat(
          ' site/profile/',
            F.col('CustomerId').cast(DataTypes.STRING()),
            '.jpeg'
        ).alias('ProfileImage'),
    ).execute_insert('MysqlSink').wait(500000000)
    
if __name__ == "__main__":
    process()