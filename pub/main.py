import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from .dependencies import manager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Application startup")
    try:
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
    manager.send_data()
