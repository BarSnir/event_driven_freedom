from libs.utils.logger import ColorLogger
from steps import (
    setup_database,
    process_batch_dataset,
    process_stream_dataset
)

if __name__ == "__main__":
    logger = ColorLogger("event_driven_freedom").get_logger()
    setup_database.process(logger)
    process_batch_dataset.process(logger)
    process_stream_dataset.process(logger)
