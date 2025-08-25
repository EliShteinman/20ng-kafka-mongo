# services/data_loader/main.py
import logging
import os
from contextlib import asynccontextmanager
from .dal import DataRead
from fastapi import FastAPI, HTTPException, status

from .crud import soldiers
from .dependencies import data_loader

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
        data_read = DataRead()
        data["read"] = data_read
        logger.info("Database connection established successfully.")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")

    yield

    # On server shutdown:
    logger.info("Application shutdown")


# Create the main FastAPI application instance
app = FastAPI(
    lifespan=lifespan,
    title="FastAPI MongoDB CRUD Service",
    version="2.0",
    description="A microservice for managing soldier data, deployed on OpenShift.",
)



@app.get("/")
def health_check_endpoint():
    """
    Health check endpoint.
    Used by OpenShift's readiness and liveness probes.
    """
    return {"status": "ok", "service": "FastAPI MongoDB CRUD Service"}


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
        "service": "FastAPI MongoDB CRUD Service",
        "version": "2.0",
        "database_status": db_status,
    }


app.get("/pub")
async def get_pub():
    return data["read"].get_pub()