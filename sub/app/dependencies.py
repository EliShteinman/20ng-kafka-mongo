# services/data_loader/dependencies.py
import logging
import os

from consumer import Consumer
from dal import DataLoader
from manager import Manager

# Read configuration from environment variables in a central place.
MONGO_HOST = os.getenv("MONGO_HOST", "localhost")
MONGO_PORT = int(os.getenv("MONGO_PORT", 27017))
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASSWORD = os.getenv("MONGO_PASSWORD")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "news")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "news")

KAFKA_URL = os.environ.get("KAFKA_URL", "localhost")
KAFKA_PORT = int(os.environ.get("KAFKA_PORT", 9092))
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "news-consumer")
LOG_LEVEL = os.getenv("LOG_LEVEL", "debug").upper()
LOG_KAFKA = os.getenv("LOG_KAFKA", "ERROR").upper()
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "news")


logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.getLogger("kafka").setLevel(getattr(logging, LOG_KAFKA, logging.ERROR))


if MONGO_USER and MONGO_PASSWORD:
    MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@{MONGO_HOST}:{MONGO_PORT}/?authSource=admin"
else:
    MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"


# Create a single, shared instance (Singleton) of the DataLoader.
# All other parts of the application will import this instance to interact with the database.
data_loader = DataLoader(
    mongo_uri=MONGO_URI, db_name=MONGO_DB_NAME, collection_name=MONGO_COLLECTION_NAME
)
consumer = Consumer(KAFKA_TOPIC, KAFKA_URL, KAFKA_PORT, KAFKA_GROUP_ID)
manager = Manager(data_loader, consumer)
