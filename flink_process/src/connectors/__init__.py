class Connectors:
    def __init__(self, config: dict):
        self._config = config

class FileSystemConnector(Connectors):

    def __init__(self, config: dict):
        super().__init__(config)

class KafkaUpsertConnector(Connectors):

    def __init__(self, config: dict):
        super().__init__(config)

class MySqlConnector(Connectors):

    def __init__(self, config: dict):
        super().__init__(config)

class KafkaConnector(Connectors):

    def __init__(self, config: dict):
        super().__init__(config)

