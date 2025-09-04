import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from .services import publisher_service, kafka_producer

logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handles application startup and shutdown events."""
    logger.info("Application startup...")
    await kafka_producer.start()
    yield
    logger.info("Application shutdown...")
    await kafka_producer.stop()

app = FastAPI(
    lifespan=lifespan,
    title="News Publisher",
    description="Publishes messages from the 20 Newsgroups dataset to Kafka."
)

@app.get("/publish", summary="Publish messages to Kafka")
async def publish_endpoint(count: int = 1):
    """
    Triggers the process of fetching and publishing news messages.
    - Fetches `count` messages from each of the 20 news categories.
    - Publishes them to 'interesting' or 'not_interesting' topics.
    """
    try:
        sent_count = await publisher_service.publish_messages(count)
        if sent_count is None:
            return {"status": "complete", "message": "All available data has been published."}
        return {"status": "success", "messages_published": sent_count}
    except Exception as e:
        logger.error(f"Failed to publish messages: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An unexpected error occurred during publishing.")

@app.get("/health", summary="Health Check")
def health_check():
    return {"status": "ok"}