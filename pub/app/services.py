import logging
from typing import Any, List, Optional
from aiokafka import AIOKafkaProducer
from .dal import NewsGroupsDAL
from . import config

logger = logging.getLogger(__name__)


class AsyncKafkaProducer:
    """A simple, reusable async Kafka producer wrapper."""

    def __init__(self, bootstrap_servers: str):
        self._producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: v.encode('utf-8')
        )
        self._started = False

    async def start(self):
        if not self._started:
            await self._producer.start()
            self._started = True
            logger.info("AsyncKafkaProducer started.")

    async def stop(self):
        if self._started:
            await self._producer.stop()
            self._started = False
            logger.info("AsyncKafkaProducer stopped.")

    async def send(self, topic: str, value: Any):
        if not self._started:
            raise RuntimeError("Kafka producer is not started")
        import json
        await self._producer.send_and_wait(topic, json.dumps(value).encode('utf-8'))


class PublisherService:
    """Orchestrates fetching data and publishing it to Kafka."""

    def __init__(self, dal: NewsGroupsDAL, producer: AsyncKafkaProducer):
        self.dal = dal
        self.producer = producer

    async def publish_messages(self, count: int) -> Optional[int]:
        """
        Fetches a batch of messages and publishes them to their respective topics.
        Returns the number of messages sent, or None if no data is left.
        """
        logger.info(f"Attempting to publish {count} message(s) per category.")
        batch = self.dal.get_next_batch(count)

        if batch is None:
            return None

        for item in batch:
            topic = item["topic"]
            payload = item["payload"]
            await self.producer.send(topic, payload)

        logger.info(f"Successfully published {len(batch)} messages.")
        return len(batch)


# --- Singleton Instances ---
news_dal = NewsGroupsDAL()
kafka_producer = AsyncKafkaProducer(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
publisher_service = PublisherService(dal=news_dal, producer=kafka_producer)