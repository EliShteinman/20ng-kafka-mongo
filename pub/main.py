import logging
import os
from contextlib import asynccontextmanager
from . import dependencies
from fastapi import FastAPI
from kafka import KafkaProducer
import json

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


logger = logging.getLogger(__name__)


data = dict()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup")
    try:
        data["producer"] = KafkaProducer(
            bootstrap_servers=f"{dependencies.KAPKA_URL}:{dependencies.KAPKA_PORT}",
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        logger.info("")
    except Exception as e:
        logger.error(f"{e}")
    yield
    logger.info("Application shutdown")


app = FastAPI(
    lifespan=lifespan,
    title="1",
    version="1",
    description="1",
)


@app.get("/")
def health_check_endpoint():
    return {"status": "ok", "service": ""}


@app.get("/pub")
def push_pub():
    manager = data.get("manager")
    if manager is None:
        from .manager import Manager
    data["manager"].send_data()
