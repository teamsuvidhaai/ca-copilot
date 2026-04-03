import ssl
import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from app.core.config import settings

logger = logging.getLogger(__name__)

# Determine SSL requirement based on the host
is_local = "localhost" in settings.DATABASE_URL or "127.0.0.1" in settings.DATABASE_URL

connect_args = {
    "command_timeout": 60,
    "statement_cache_size": 0,       # CRITICAL — PgBouncer doesn't support prepared statements
    "prepared_statement_cache_size": 0,  # Belt-and-suspenders for older asyncpg
    "timeout": 60,                   # Supabase free-tier can take 20s+ on cold start
    "server_settings": {
        "jit": "off",                # Faster connection setup
        "tcp_keepalives_idle": "600",
        "tcp_keepalives_interval": "30",
        "tcp_keepalives_count": "10",
    },
}

if not is_local:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    connect_args["ssl"] = ctx
else:
    connect_args["ssl"] = False

engine = create_async_engine(
    str(settings.DATABASE_URL),
    echo=False,
    future=True,
    pool_size=3,          # Keep small for free tier (max 5 total with overflow)
    max_overflow=3,       # Allow 2 extra under load
    pool_timeout=20,      # Seconds to wait for a connection from the pool
    pool_recycle=55,     # Recycle connections every 3 min (PgBouncer-safe)
    pool_pre_ping=True,   # Test connection before using
    connect_args=connect_args,
)

AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autocommit=False,
    autoflush=False,
)


async def get_db():
    """DB dependency — pool_pre_ping + warmup handle connection issues."""
    async with AsyncSessionLocal() as session:
        yield session


async def warmup_db():
    """Pre-connect to DB on startup to avoid cold-start timeouts."""
    for attempt in range(5):
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text("SELECT 1"))
            logger.info("✅ Database warmup successful")
            return
        except Exception as e:
            wait = (attempt + 1) * 3
            logger.warning(f"DB warmup attempt {attempt+1}/5 failed: {e}. Retrying in {wait}s...")
            await asyncio.sleep(wait)
    logger.error("⚠️ Database warmup failed after 5 attempts — will retry on first request")
