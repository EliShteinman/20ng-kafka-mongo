import logging
from datetime import datetime, timezone

from .models import MessageIn

logger = logging.getLogger(__name__)


class Manager:
    """
    Control data flow from Kafka to MongoDB.
    Gets messages from Kafka and saves them to database.
    """

    def __init__(self, data_loader, consumer):
        """
        Set up the manager.

        Args:
            data_loader: DataLoader object that handles database
            consumer: Consumer object that reads from Kafka
        """
        self.data_loader = data_loader
        self.consumer = consumer
        logger.info("Manager ready")
        self.last_check_time = datetime.now(timezone.utc)

    async def get_data_from_mongo(self):
        """
        Get new messages from database since last check.
        Updates the last check time after getting messages.

        Returns:
            List of new MessageOut objects
        """
        new_messages = await self.data_loader.receive_messages_from(
            self.last_check_time
        )
        self.last_check_time = datetime.now(timezone.utc)
        return new_messages

    async def get_data_from_kafka(self):
        """
        Read messages from Kafka and save them to database.

        Returns:
            List of results from saving each message
        """
        messages = self.consumer.consume()
        result = []
        for message in messages:
            message_data = message.value
            status = await self._insert_message_to_mongo(message_data)
            result.append(status)
        return result

    async def _insert_message_to_mongo(self, message):
        """
        Save one message to MongoDB database.

        Args:
            message: Dictionary with message data from Kafka

        Returns:
            MessageOut object with saved message data
        """
        logger.info(f"Received: type={type(message)}, content={str(message)[:200]}...")
        message_in = MessageIn(
            data=message["data"],
            category=message["label"],
            created_at=datetime.now(timezone.utc),
        )
        saved_message = await self.data_loader.create_item(message_in)
        return saved_message
