import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from .models import Message
from pymongo import AsyncMongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError, PyMongoError

logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.collection_name = collection_name
        self.client: Optional[AsyncMongoClient] = None
        self.db: Optional[Database] = None
        self.collection: Optional[Collection] = None

    async def connect(self):
        try:
            self.client = AsyncMongoClient(
                self.mongo_uri, serverSelectionTimeoutMS=5000
            )
            await self.client.admin.command("ping")
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            logger.info("Successfully connected to MongoDB.")
            await self._setup_indexes()
        except PyMongoError as e:
            logger.error(f"DATABASE CONNECTION FAILED: {e}")
            self.client = None
            self.db = None
            self.collection = None

    async def _setup_indexes(self):
        if self.collection is not None:
            try:
                await self.collection.create_index("created_at")
                logger.info("Index on 'created_at' ensured.")
            except PyMongoError as e:
                logger.error(f"Failed to create index: {e}")

    def disconnect(self):
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB.")

    async def create_item(self, item: Message) -> Message:
        if self.collection is None:
            raise RuntimeError("Database connection is not available.")

        try:
            item_dict = item.model_dump(by_alias=True, exclude_none=True)
            insert_result = await self.collection.insert_one(item_dict)
            created_item = await self.collection.find_one(
                {"_id": insert_result.inserted_id}
            )
            if created_item:
                created_item["_id"] = str(created_item["_id"])
                logger.info(f"Successfully created message in category {item.category}.")
            return created_item

        except DuplicateKeyError:
            logger.warning(f"Duplicate message attempted for category {item.category}.")
            raise ValueError("Message already exists.")
        except PyMongoError as e:
            logger.error(f"Error creating message in category {item.category}: {e}")
            raise RuntimeError(f"Database operation failed: {e}")


    async def receive_messages_from(
        self,
        time: datetime,
    ) -> List[Message]:

        if self.collection is None:
            raise RuntimeError("Database connection is not available.")

        query = {"created_at": {"$gte": time}}

        try:
            cursor = self.collection.find(query).sort("created_at", 1)
            items: List[Message] = []
            async for mes in cursor:
                mes["_id"] = str(mes["_id"])
                items.append(mes)
            return items
        except PyMongoError as e:
            logger.error(f"Error retrieving data since {time}: {e}")
            raise RuntimeError(f"Database operation failed: {e}")