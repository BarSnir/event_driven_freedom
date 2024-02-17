from . import FlinkConnector

class FlinkJDBCConnector(FlinkConnector):

    def __init__(self, config):
        super().__init__(config)
        self.connector_config_list = config.get('connector_config')

    def generate_jdbc_connector(self):
        return f"""
            {self.get_create_table_state()}
            WITH (
                'connector' = 'jdbc',
                {self.get_connectors_config_string()}
            );
        """
    
    def get_connectors_config_string(self):
        connector_config_string = ''
        for item in self.connector_config_list:
            connector_config_string += item
        return connector_config_string