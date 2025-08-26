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
        self.last_check_time = datetime.now(timezone.utc)

    async def get_data_from_mongo(self):
        new_messages = await self.dal.receive_messages_from(self.last_check_time)
        self.last_check_time = datetime.now(timezone.utc)
        return new_messages

    async def get_data_from_kafka(self):
        messages = self.consumer.consume()
        result = []
        for message in messages:
            data = message.value
            status = await self._insert_mes_to_mongo(data)
            result.append(status)
        return result

    async def _insert_mes_to_mongo(self, mes):
        logger.info(f"Received: type={type(mes)}, content={str(mes)[:200]}...")
        message_in = MessageIn(
            data=mes["data"],
            category=mes["label"],
            created_at=datetime.now(timezone.utc)
        )
        data = await self.dal.create_item(message_in)
        return data


