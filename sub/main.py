import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status

from .dependencies import data_loader, manager

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
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
    title="1",
    version="1",
    description="1",
)



@app.get("/")
def health_check_endpoint():
    """
    Health check endpoint.
    Used by OpenShift's readiness and liveness probes.
    """
    return {"status": "ok", "service": "1"}


@app.get("/health")
def detailed_health_check():
    """
    Detailed health check endpoint.
    Returns 503 if database is not available.
    """
    db_status = "connected" if data_loader.collection is not None else "disconnected"

    if db_status == "disconnected":
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Database not available",
        )

    return {
        "status": "ok",
        "service": "1",
        "version": "1",
        "database_status": db_status,
    }

@app.get("/update-data")
async def update_data():
    await manager.get_data_from_kafka()


@app.get("/get-messege")
async def get_messege():
    data = await manager.get_data_from_mongo()
    return data