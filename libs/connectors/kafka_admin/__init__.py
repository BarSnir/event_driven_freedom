import os, time
from confluent_kafka.admin import AdminClient

class KafkaAdminClientWrap:

    def __init__(self, logger):
        self.logger = logger
        self.config = self.build_client_config()
        self.client = AdminClient(self.config)

    def build_client_config(self):
        return {
            'bootstrap.servers': os.getenv('KAFKA_SERVERS'),     
        }
    
    def find_topics(self, topics_list, retries=10, backoff_seconds=10):
        retries_counter = 0
        while True:
            time.sleep(backoff_seconds)
            exists_topic_list = list(
                self.client.list_topics().topics.keys()
            )
            self.logger.debug(exists_topic_list)
            if all(topic in exists_topic_list for topic in topics_list):
                self.logger.debug("Found topics")
                break
            retries_counter += 1 
            if retries_counter > retries:
                raise Exception("Failed to find topics.")
