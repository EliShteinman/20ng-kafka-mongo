import logging
from datetime import datetime, timezone
from .models import MessageIn
logger = logging.getLogger(__name__)


class Manager:
    """
    Control data flow from reader to Kafka.
    """

    def __init__(self, dal, consumer):
        """
        Setup manager.
        data: DataRead object
        producer: Producer object
        """
        self.dal = dal
        self.consumer = consumer
        logger.info("Manager ready")
        self.time = None

    async def get_data_from_mongo(self):
        self.time = datetime.now()
        return await self.dal.receive_messages_from(self.time)

    async def get_data_from_kafka(self):
        messages = self.consumer.consume()
        result = []
        for message in messages:
            data = message.value
            status = await self._insert_mes_to_mongo(data)
            result.append(status)
        return result

    async def _insert_mes_to_mongo(self, mes):
        message_in = MessageIn(
            data=mes["data"],
            category=mes["category"],
            created_at = datetime.now(timezone.utc)
        )
        data = await self.dal.create_item(message_in)
        return data


