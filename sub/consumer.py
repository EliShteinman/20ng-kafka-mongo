import json
import logging

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)

class Consumer:

    def __init__(self, topic, url: str, port: int, group_id: str, consumer_timeout_ms: int = 10000):
        self.topic = topic
        self.url = url
        self.port = port
        self.group_id = group_id
        self.consumer_timeout_ms = consumer_timeout_ms

        self.consumer = KafkaConsumer(self.topic,
                                      group_id=self.group_id,
                                      value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                      bootstrap_servers=[f"{self.url}:{self.port}"],
                                      consumer_timeout_ms=self.consumer_timeout_ms)


    def consume(self):
        for message in self.consumer:
            yield message.value


