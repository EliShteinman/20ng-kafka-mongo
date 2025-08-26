import json
import logging

from kafka import KafkaProducer

logger = logging.getLogger(__name__)


class Producer:
    """
    Send messages to Kafka server.
    """

    def __init__(self, url: str, port: int):
        """
        Connect to Kafka server.

        Args:
            url: Kafka server address like 'localhost'
            port: Kafka server port like 9092
        """
        logger.info(f"Connecting to Kafka at {url}:{port}")

        self.producer = KafkaProducer(
            bootstrap_servers=f"{url}:{port}",
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

        logger.info("Kafka producer ready")

    def send(self, topic: str, message: str):
        """
        Send one message to a Kafka topic.

        Args:
            topic: Name of the topic where to send message
            message: The message content to send
        """
        logger.debug(f"Sending message to topic: {topic}")

        try:
            self.producer.send(topic, message)
            logger.debug(f"Message sent to {topic}")
        except Exception as e:
            logger.error(f"Failed to send message to {topic}: {e}")
            raise