

class FlinkConnector:

    def __init__(self, config):
        self.table_name = config.get('table_name')
        self.schema = self.generate_schema(config.get('schema'))

    def generate_schema(self, list_schema):
        schema = ''
        for item in list_schema:
            schema += item
        return schema 

    def get_create_table_state(self):
        return f"""
        CREATE TABLE {self.table_name} (
            {self.schema}
        )
        """