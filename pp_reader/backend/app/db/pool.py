"""asyncpg connection pool — created once at startup, shared across requests."""

import asyncpg

from app.config import settings

# Module-level pool reference; initialised by create_pool() in lifespan.
_pool: asyncpg.Pool | None = None


async def create_pool() -> asyncpg.Pool:
    """Create and cache the global asyncpg connection pool."""
    global _pool
    _pool = await asyncpg.create_pool(
        dsn=settings.database_url,
        min_size=2,
        max_size=10,
        max_inactive_connection_lifetime=300,  # 5 minutes idle timeout
    )
    return _pool


async def close_pool() -> None:
    """Gracefully close the global connection pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    """Return the active pool; raises RuntimeError if not initialised."""
    if _pool is None:
        raise RuntimeError("Database pool not initialised — call create_pool() first")
    return _pool
