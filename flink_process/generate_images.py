import random, string
from pyflink.table import DataTypes
from libs.connectors.jdbc import FlinkJDBCConnector
from libs.streaming import FlinkStreamingEnvironment

SCHEMA = DataTypes.ROW([
    DataTypes.FIELD("ImageId", DataTypes.STRING()),
    DataTypes.FIELD("OrderId", DataTypes.STRING()),
    DataTypes.FIELD("Priority", DataTypes.INT()),
    DataTypes.FIELD("Url", DataTypes.STRING())
])

def process():
    rows = []
    order_id_list =[]
    streaming_env = FlinkStreamingEnvironment('generate_images')
    job_config = streaming_env.job_config
    table_env = streaming_env.get_table_streaming_environment(parallelism=3)
    orders_jdbc_source = FlinkJDBCConnector(job_config.get('orders_jdbc_source'))
    images_jdbc_sink = FlinkJDBCConnector(job_config.get('images_jdbc_sink'))
    # BE AWARE OF FLUSH INTERVAL!!!
    source_ddl = orders_jdbc_source.generate_jdbc_connector()
    sink_ddl = images_jdbc_sink.generate_jdbc_connector()
    table_env.execute_sql(source_ddl)
    table_env.execute_sql(sink_ddl)
    panda_table = table_env.from_path(orders_jdbc_source.table_name).to_pandas()
    for item in panda_table.to_dict(orient='records'):
       order_id_list.append(item['OrderID'])
    for order_id in order_id_list:
       for image_number in range(random.randrange(1, 10)):
          letters = string.ascii_lowercase
          token = ''.join(random.choice(letters) for i in range(16))
          rows.append((token, order_id, image_number+1, f'site-url/{token}'))
    images_table = table_env.from_elements(rows, SCHEMA)
    images_table.execute_insert(images_jdbc_sink.table_name).wait()
if __name__ == "__main__":
    process()