import os
from steps import (
    setup_database,
    process_batch_dataset,
)

if __name__ == "__main__":
    setup_database.process()
    process_batch_dataset.process()