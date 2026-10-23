"""Async Postgres pool — deliberately tiny so pool exhaustion is easy to trigger.

The schema lives in k8s/base/postgres/init.sql (used by both Kubernetes and Compose).
"""

import asyncio
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://orders:orders@localhost:5432/orders")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "3"))
DB_POOL_ACQUIRE_TIMEOUT = float(os.getenv("DB_POOL_ACQUIRE_TIMEOUT", "0.5"))
# Simulated query time so a burst of traffic causes real pool contention.
DB_QUERY_DELAY_SECONDS = float(os.getenv("DB_QUERY_DELAY_SECONDS", "0.15"))

_pool: asyncpg.Pool | None = None


class PoolExhaustedError(Exception):
    """No connection could be handed out within DB_POOL_ACQUIRE_TIMEOUT.

    Usually every connection is busy; it can also mean opening a replacement
    connection was slow (e.g. right after Postgres restarted). The in_use/max
    numbers in the message tell the two apart.
    """


async def init_db() -> None:
    global _pool
    _pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=DB_POOL_SIZE, command_timeout=10)


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def is_ready() -> bool:
    return _pool is not None


def get_pool_metrics() -> tuple[int, int]:
    if not _pool:
        return 0, DB_POOL_SIZE
    active = _pool.get_size() - _pool.get_idle_size()
    return max(0, active), _pool.get_max_size()


@asynccontextmanager
async def _connection() -> AsyncIterator[asyncpg.Connection]:
    if _pool is None:
        raise RuntimeError("Database pool is not initialised")
    try:
        conn = await _pool.acquire(timeout=DB_POOL_ACQUIRE_TIMEOUT)
    except asyncio.TimeoutError as exc:
        in_use, max_size = get_pool_metrics()
        raise PoolExhaustedError(
            f"no connection within {DB_POOL_ACQUIRE_TIMEOUT}s (in_use={in_use}, max={max_size})"
        ) from exc
    try:
        yield conn
    finally:
        await _pool.release(conn)


def _to_dict(row: asyncpg.Record | None) -> dict | None:
    if row is None:
        return None
    return {"id": row["id"], "created_at": row["created_at"].isoformat()}


async def get_order(order_id: str, extra_delay: float = 0.0) -> dict | None:
    async with _connection() as conn:
        await conn.execute("SELECT pg_sleep($1)", DB_QUERY_DELAY_SECONDS + extra_delay)
        row = await conn.fetchrow("SELECT id, created_at FROM orders WHERE id = $1", order_id)
    return _to_dict(row)


async def create_order(order_id: str) -> dict:
    async with _connection() as conn:
        row = await conn.fetchrow(
            """INSERT INTO orders (id) VALUES ($1)
               ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id
               RETURNING id, created_at""",
            order_id,
        )
        await conn.execute("SELECT pg_sleep($1)", DB_QUERY_DELAY_SECONDS)
    return _to_dict(row)
