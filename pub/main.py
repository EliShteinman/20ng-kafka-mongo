import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

from .dependencies import manager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    App startup and shutdown.
    """
    logger.info("Application startup")
    try:
        logger.info("App is ready")
    except Exception as e:
        logger.error(f"Startup error: {e}")
    yield
    logger.info("Application shutdown")


app = FastAPI(
    lifespan=lifespan,
    title="News Publisher",
    version="1.0",
    description="Send news data to Kafka",
)


@app.get("/")
def health_check_endpoint():
    """
    Check if service is working.
    Returns: status message
    """
    logger.debug("Health check called")
    return {"status": "ok", "service": "news-publisher"}


@app.get("/pub")
def push_pub(count: int = 1):
    """
    Send data to Kafka.
    count: how many messages per category to send
    Returns: success message
    """
    logger.info(f"Publish endpoint called, count: {count}")

    try:
        sent_count = manager.send_data(count)

        if sent_count is None:
            logger.info("All data finished")
            return {"status": "finished", "message": "All messages have been sent"}

        logger.info("Data published successfully")
        return {"status": "success", "message": f"Sent {sent_count} messages to Kafka"}
    except Exception as e:
        logger.error(f"Failed to publish data: {e}")
        raise HTTPException(status_code=500, detail=f"Publish failed: {str(e)}")