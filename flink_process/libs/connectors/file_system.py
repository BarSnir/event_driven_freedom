from . import FlinkConnector

class FlinkFileSystemConnector(FlinkConnector):

    def __init__(self, config):
        super().__init__(config)
        self.path = config.get('path')

    def generate_fs_connector(self, format):
        return f"""
            {self.get_create_table_state()}
            WITH (
                'connector' = 'filesystem',
                'path' = '{self.path}',
                'format' = '{format}'
            );
        """

