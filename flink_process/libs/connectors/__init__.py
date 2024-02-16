
class FlinkConnector:

    def __init__(self):
        pass

    def get_csv_connector(self):
        return """
        CREATE TABLE MarketInfoInit (
            `year` VARCHAR,
            `make` VARCHAR,
            `model` VARCHAR,
            `body_styles` VARCHAR
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/flink/datasets/market_info',
            'format' = 'csv'
        );
    """  