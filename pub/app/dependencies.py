import logging
import os

from dal import DataRead
from manager import Manager
from producer import Producer

KAFKA_URL = os.environ.get("KAFKA_URL", "localhost")
KAFKA_PORT = int(os.environ.get("KAFKA_PORT", 9092))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_KAFKA = os.getenv("LOG_KAFKA", "ERROR").upper()

# -----------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logging.getLogger("kafka").setLevel(getattr(logging, LOG_KAFKA, logging.ERROR))


# ------------------------
data_reader = DataRead()
producer = Producer(KAFKA_URL, KAFKA_PORT)
manager = Manager(data_reader, producer)
