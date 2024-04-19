import os
from minio import Minio

class MinioHandler:

    def __init__(self, logger):
        self.logger = logger
        self.bucket_name = os.getenv('SINK_BUCKET')
        self.client = self._get_client()

    def _get_client(self):
        return Minio(
            os.getenv('MINIO_HOST'),
            access_key=os.getenv('MINIO_USER'),
            secret_key=os.getenv('MINIO_PASSWORD'),
            secure=False
        )

    def create_bucket(self):
        found = self.client.bucket_exists(self.bucket_name)
        if not found:
            self.client.make_bucket(self.bucket_name)
            self.logger.info(f"Created Minio bucket {self.bucket_name}.")
            return
        self.logger.info(f"Minio bucket {self.bucket_name} already exists!")
