import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status

from dependencies import data_loader, manager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Handle app startup and shutdown.
    Connect to database when app starts.
    Disconnect from database when app stops.
    """
    logger.info("Application startup: connecting to database...")
    try:
        await data_loader.connect()
        logger.info("Database connection established successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")

    yield
    logger.info("Application shutdown: disconnecting from database...")
    try:
        data_loader.disconnect()
        logger.info("Database disconnection completed.")
    except Exception as e:
        logger.error(f"Error during database disconnection: {e}")


app = FastAPI(
    lifespan=lifespan,
    title="News Subscriber",
    version="1.0",
    description="Read news data from Kafka and save to MongoDB",
)


@app.get("/")
def health_check_endpoint():
    """
    Simple health check.
    Check if the service is running.

    Returns:
        Dictionary with status and service name
    """
    return {"status": "ok", "service": "news-subscriber"}


@app.get("/health")
def detailed_health_check():
    """
    Detailed health check.
    Check if service and database are working.

    Returns:
        Dictionary with detailed status info

    Raises:
        HTTPException: 503 error if database is not available
    """
    database_status = (
        "connected" if data_loader.collection is not None else "disconnected"
    )

    if database_status == "disconnected":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not available",
        )

    return {
        "status": "ok",
        "service": "news-subscriber",
        "version": "1.0",
        "database_status": database_status,
    }


@app.get("/update-data")
async def update_data():
    """
    Read new messages from Kafka and save them to database.

    Returns:
        Results from processing Kafka messages
    """
    await manager.get_data_from_kafka()


@app.get("/get-messages")
async def get_messages():
    """
    Get new messages from database since last check.

    Returns:
        List of new messages from MongoDB
    """
    messages_data = await manager.get_data_from_mongo()
    return messages_data
