from libs.utils.logger import ColorLogger
from steps import (
    process_debezium,
    setup_database,
    process_batch_dataset,
    process_stream_enriches,
    process_search_documents,
    process_elasticsearch_connector,
    process_s3_connector,
    process_neo4j_connector
)

if __name__ == "__main__":
    logger = ColorLogger("event_driven_freedom").get_logger()
    setup_database.process(logger)
    process_batch_dataset.process(logger)
    process_debezium.process(logger)
    process_stream_enriches.process(logger)
    process_search_documents.process(logger)
    process_elasticsearch_connector.process(logger)
    process_s3_connector.process(logger)
    process_neo4j_connector.process(logger)
    logger.info("Done, thank you for given freedom to your data!")
