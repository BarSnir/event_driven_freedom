import os
from random import randrange
from pyflink.table.udf import udf
from pyflink.table import ( 
    EnvironmentSettings, 
    TableEnvironment,
    DataTypes,
    expressions as F
)


@udf(result_type='BIGINT')
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

def get_jars_path():
    return f'file:///Users/barsnir/Desktop/flink_playground/flink-1.17.1/opt/'

def get_jars_full_path() -> str:
  jars_path = get_jars_path()
  jars = [
    'flink-connector-jdbc-3.1.0-1.17.jar;',
    'mysql-connector-java-5.1.9.jar'
  ]
  full_str = ''
  for jar in jars:
    full_str = f'{full_str}{jars_path}{jar}'
  return full_str

def process():

    environment_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(environment_settings)
    t_env.get_config().set('pipeline.jars',get_jars_full_path())

    source_ddl = """
        CREATE TABLE MarketInfoInit (
            `year` VARCHAR,
            `make` VARCHAR,
            `model` VARCHAR,
            `body_styles` VARCHAR
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/flink/datasets',
            'format' = 'csv'
        );
    """
    sink_ddl = """
        CREATE TABLE MysqlSink (
            `MarketInfoId` BIGINT,
            `ManufacturerId` BIGINT,
            `ManufacturerText` VARCHAR,
            `ModelId` BIGINT,
            `ModelText` VARCHAR,
            `Year` INT,
            PRIMARY KEY (MarketInfoId) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:mysql://db:3306/production',
            'table-name' = 'MarketInfo',
            'username'='root',
            'password'='password'
        );
    """
    t_env.execute_sql(source_ddl)
    t_env.execute_sql(sink_ddl)
    market_info_table = t_env.from_path('MarketInfoInit')
    market_info_table = market_info_table.select(
        F.col('make').alias('ManufacturerText'),
        F.col('model').alias('ModelText'),
        F.col('year').cast(DataTypes.INT()).alias('Year')
    ).filter(F.col('ManufacturerText') != 'make').execute().print()
    # market_info_table.select(
    #     (
    #         calc_gemetria(F.concat(F.col('ManufacturerText'), F.col('ModelText'))) \
    #         + \
    #         F.col('Year')
    #         +
    #         randrange(1, 1000000)
    #     ).alias('MarketInfoId'),
    #     calc_gemetria(F.col('ManufacturerText')).alias('ManufacturerId'),
    #     F.col('ManufacturerText'),
    #     calc_gemetria(F.concat(F.col('ManufacturerText'), F.col('ModelText'))).alias('ModelId'),
    #     F.col('ModelText'),
    #     F.col('Year')
    # ).execute_insert('MysqlSink').wait(100000000)

if __name__ == "__main__":
    process()