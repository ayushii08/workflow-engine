"""
FastAPI application entry point for Workflow Engine.
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from app.api import routes
from app.api import websocket
from app.storage.store import get_storage
from app.workflows.data_quality import register_data_quality_tools

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup/shutdown events."""
    logger.info("Starting Workflow Engine...")

    # Register built-in tools, including the data quality pipeline
    register_data_quality_tools()
    # Initialize storage
    storage = get_storage()
    logger.info(f"Storage initialized: {type(storage).__name__}")
    yield
    logger.info("Shutting down Workflow Engine...")


app = FastAPI(
    title="Workflow Engine API",
    description="A minimal but powerful workflow/graph execution engine",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(routes.router, prefix="/api", tags=["graphs"])
app.include_router(websocket.router, prefix="/ws", tags=["websocket"])


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "Workflow Engine",
        "version": "1.0.0"
    }


@app.get("/health")
async def health_check():
    """Detailed health check."""
    storage = get_storage()
    return {
        "status": "healthy",
        "graphs": len(storage.graphs),
        "runs": len(storage.runs)
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )