import logging
from contextlib import asynccontextmanager
from typing import List
from fastapi import FastAPI, HTTPException, status
from .services import subscriber_service, mongo_dal
from .models import MessageInDB

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handles application startup and shutdown.
    NEW: Starts a background task to consume from Kafka.
    """
    logger.info("Application startup...")
    subscriber_service.start_consuming()
    yield
    logger.info("Application shutdown...")
    subscriber_service.stop_consuming()

app = FastAPI(
    lifespan=lifespan,
    title="News Subscriber",
    description=f"Consumes from Kafka topic '{subscriber_service.consumer._consumer.subscription()}' and saves to MongoDB."
)

@app.get("/messages", response_model=List[MessageInDB], summary="Get All Saved Messages")
async def get_all_messages():
    """
    Retrieves ALL messages stored in the database for this subscriber.
    """
    try:
        messages = await mongo_dal.get_all_messages()
        return messages
    except Exception as e:
        logger.error(f"Failed to retrieve messages: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Failed to retrieve messages from database.")

@app.get("/health", summary="Health Check")
async def health_check():
    """Checks service and database connectivity."""
    db_ok = await mongo_dal.ping()
    if not db_ok:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database connection failed."
        )
    return {
        "status": "ok",
        "database": "connected"
    }