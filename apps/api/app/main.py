import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from app.api.api import api_router
from app.core.config import settings
from app.db.session import warmup_db, AsyncSessionLocal
from sqlalchemy import text
import asyncio

logger = logging.getLogger(__name__)

async def heartbeat():
    while True:
        await asyncio.sleep(30)
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text("SELECT 1"))
        except Exception:
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting up — warming DB connection...")
    asyncio.create_task(heartbeat()) 
    # asyncio.create_task(warmup_db())  # ← changed from await to create_task
    yield
    logger.info("👋 Shutting down...")


app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan,
)

# Set all CORS enabled origins
all_origins = [str(origin) for origin in settings.BACKEND_CORS_ORIGINS]
# Whitelist production frontend and local dev
all_origins.extend([
    "https://complianceaiexpert.netlify.app",
    "https://ca-copilot-mrwj.onrender.com",
    "http://localhost:3000",
    "http://localhost:8000",
    "http://127.0.0.1:3000",
    "*"
])

app.add_middleware(
    CORSMiddleware,
    allow_origins=all_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix=settings.API_V1_STR)

@app.get("/health")
def health_check():
    return {"status": "ok"}

@app.get("/")
def root_check():
    return {"message": "Backend is running"}
