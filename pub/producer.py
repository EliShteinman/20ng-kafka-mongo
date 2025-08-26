import json

from kafka import KafkaProducer


class Producer:
    def __init__(self, url: str, port: int):
        self.producer = KafkaProducer(
            bootstrap_servers=f"{url}:{port}",
            value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        )

    def send(self, topic: str, message: dict):
        self.producer.send(topic, message)
