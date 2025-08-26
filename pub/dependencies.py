import os
import logging


from .dal import DataRead
from .producer import Producer
from .manager import Manager

KAPKA_URL = os.environ.get('KAPKA_URL', "localhost")
KAPKA_PORT = int(os.environ.get('KAPKA_PORT', 9092))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()




# -----------------------
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# ------------------------
dal = DataRead()
producer = Producer(KAPKA_URL, KAPKA_PORT)
manager = Manager(dal, producer)
