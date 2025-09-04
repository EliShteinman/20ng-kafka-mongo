import asyncio
import json
import logging
from typing import Any, AsyncIterator
from aiokafka import AIOKafkaConsumer
from .dal import MongoDAL
from . import config

logger = logging.getLogger(__name__)

class AsyncKafkaConsumer:
    """A simple, reusable async Kafka consumer wrapper."""
    def __init__(self, topic: str, bootstrap_servers: str, group_id: str):
        self._consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        )
        self._started = False

    async def start(self):
        if not self._started:
            await self._consumer.start()
            self._started = True
            logger.info(f"AsyncKafkaConsumer started for topic '{self._consumer.subscription()}'.")

    async def stop(self):
        if self._started:
            await self._consumer.stop()
            self._started = False
            logger.info("AsyncKafkaConsumer stopped.")

    async def consume(self) -> AsyncIterator[Any]:
        if not self._started:
            raise RuntimeError("Kafka consumer is not started")
        async for msg in self._consumer:
            yield msg.value


class SubscriberService:
    """Orchestrates consuming from Kafka and saving to MongoDB."""
    def __init__(self, dal: MongoDAL, consumer: AsyncKafkaConsumer):
        self.dal = dal
        self.consumer = consumer
        self.task: Optional[asyncio.Task] = None

    async def _consume_and_save(self):
        """The core background task loop."""
        await self.consumer.start()
        try:
            async for message_data in self.consumer.consume():
                logger.info(f"Received message: {str(message_data)[:100]}...")
                await self.dal.save_message(message_data)
                logger.info("Message saved to MongoDB.")
        except asyncio.CancelledError:
            logger.info("Consumption task was cancelled.")
        except Exception as e:
            logger.error(f"Error in consumer loop: {e}", exc_info=True)
        finally:
            await self.consumer.stop()

    def start_consuming(self):
        """Starts the background consumer task."""
        if not self.task or self.task.done():
            self.task = asyncio.create_task(self._consume_and_save())
            logger.info("Kafka consumer background task started.")

    def stop_consuming(self):
        """Stops the background consumer task gracefully."""
        if self.task and not self.task.done():
            self.task.cancel()
            logger.info("Kafka consumer background task cancellation requested.")

# --- Singleton Instances ---
mongo_dal = MongoDAL(
    uri=config.MONGO_URI,
    db_name=config.MONGO_DB_NAME,
    collection_name=config.MONGO_COLLECTION_NAME
)
kafka_consumer = AsyncKafkaConsumer(
    topic=config.KAFKA_TOPIC,
    bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
    group_id=config.KAFKA_GROUP_ID
)
subscriber_service = SubscriberService(dal=mongo_dal, consumer=kafka_consumer)