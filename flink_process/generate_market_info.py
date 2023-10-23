import hashlib
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


def process():
    environment_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(environment_settings)
    source_ddl = """
        CREATE TABLE MarketInfoInit (
            `year` VARCHAR,
            `make` VARCHAR,
            `model` VARCHAR,
            `body_styles` VARCHAR
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///Users/barsnir/Desktop/projects/event_driven_freedom/datasets/market_info',
            'format' = 'csv'
        )
    """
    t_env.execute_sql(source_ddl)
    market_info_table = t_env.from_path('MarketInfoInit')
    market_info_table = market_info_table.select(
        F.col('make').alias('ManufacturerText'),
        F.col('model').alias('ModelText'),
        F.col('year').cast(DataTypes.INT()).alias('Year')
    ).filter(F.col('ManufacturerText') != 'make')
    market_info_table.select(
        (
            calc_gemetria(F.concat(F.col('ManufacturerText'), F.col('ModelText'))) \
            + \
            F.col('Year')
        ).alias('MarketInfoId'),
        calc_gemetria(F.col('ManufacturerText')).alias('ManufacturerId'),
        F.col('ManufacturerText'),
        calc_gemetria(F.concat(F.col('ManufacturerText'), F.col('ModelText'))).alias('ModelId'),
        F.col('ModelText'),
        F.col('Year')
    ).execute().print()

if __name__ == "__main__":
    process()