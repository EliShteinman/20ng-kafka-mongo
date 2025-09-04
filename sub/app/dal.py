# sub/app/dal.py (מתוקן סופית)
import logging
from typing import List
from pymongo import AsyncMongoClient  # <-- התיקון המרכזי כאן
from pymongo.collection import Collection
from .models import MessageInDB

logger = logging.getLogger(__name__)


class MongoDAL:
    def __init__(self, uri: str, db_name: str, collection_name: str):
        self._client = AsyncMongoClient(uri)
        self._db = self._client[db_name]
        self.collection: Collection = self._db[collection_name]
        logger.info(f"MongoDAL initialized for collection '{collection_name}'.")

    async def save_message(self, message_data: dict) -> MessageInDB:
        """Saves a single message document to the collection."""
        message = MessageInDB(**message_data)
        document = message.model_dump(by_alias=True)
        document.pop("_id", None)

        result = await self.collection.insert_one(document)
        created_document = await self.collection.find_one({"_id": result.inserted_id})
        return MessageInDB(**created_document)

    async def get_all_messages(self) -> List[MessageInDB]:
        """Fetches ALL messages from the collection, as per requirement."""
        messages = []
        cursor = self.collection.find({})
        async for document in cursor:
            messages.append(MessageInDB(**document))
        return messages

    async def ping(self) -> bool:
        """Checks if the database connection is alive."""
        try:
            await self._client.admin.command('ping')
            return True
        except Exception:
            return False