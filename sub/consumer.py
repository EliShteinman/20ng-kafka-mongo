import json
import logging

from kafka import KafkaConsumer

logger = logging.getLogger(__name__)


class Consumer:
    """
    Get messages from Kafka server.
    """

    def __init__(self, topic, url: str, port: int, group_id: str, consumer_timeout_ms: int = 10000):
        """
        Set up connection to Kafka.

        Args:
            topic: Name of the topic to read from
            url: Kafka server address like 'localhost'
            port: Kafka server port like 9092
            group_id: Name for this consumer group
            consumer_timeout_ms: How long to wait for messages (default 10000)
        """
        self.topic = topic
        self.url = url
        self.port = port
        self.group_id = group_id
        self.consumer_timeout_ms = consumer_timeout_ms

        # Create Kafka consumer
        self.consumer = KafkaConsumer(self.topic,
                                      group_id=self.group_id,
                                      value_deserializer=lambda message: json.loads(message.decode('ascii')),
                                      bootstrap_servers=[f"{self.url}:{self.port}"],
                                      consumer_timeout_ms=self.consumer_timeout_ms)

    def consume(self):
        """
        Get the consumer object to read messages.

        Returns:
            KafkaConsumer object that can read messages
        """
        return self.consumer