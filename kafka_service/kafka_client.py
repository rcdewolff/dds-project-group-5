import os
import logging
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable
import time

class Client:
    def __init__(self, service_name, topics_to_watch):
        self.service_name = service_name
        self.bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        
        self.producer = self._split_second_retry(
            lambda: KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                # Standard practice: retry sending messages automatically
                retries=5 
            )
        )

        self.consumer = KafkaConsumer(
            *topics_to_watch,
            bootstrap_servers=self.bootstrap_servers,
            group_id=f'{self.service_name}-group',
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )

    def _split_second_retry(self, func):
        """Prevents crash if Kafka is still booting up in Docker."""
        for i in range(10):
            try:
                return func()
            except NoBrokersAvailable:
                logging.warning(f"Waiting for Kafka... attempt {i+1}")
                time.sleep(2)
        raise ConnectionError("Could not connect to Kafka after 20 seconds.")

