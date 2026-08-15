"""Async Postgres pool — deliberately tiny so pool exhaustion is easy to trigger."""

import os

import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://orders:orders@localhost:5432/orders")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "3"))
DB_POOL_ACQUIRE_TIMEOUT = float(os.getenv("DB_POOL_ACQUIRE_TIMEOUT", "0.5"))

_pool: asyncpg.Pool | None = None


async def init_db() -> None:
    global _pool
    _pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=DB_POOL_SIZE,
        command_timeout=10,
    )
    async with _pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id         TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
        """)


async def close_db() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def get_order(order_id: str) -> dict | None:
    async with _pool.acquire(timeout=DB_POOL_ACQUIRE_TIMEOUT) as conn:
        # pg_sleep holds the connection long enough to cause real pool contention
        row = await conn.fetchrow(
            "SELECT id, created_at FROM orders WHERE id = $1 AND pg_sleep(0.15) IS NOT NULL",
            order_id,
        )
    if row is None:
        return None
    return {"id": row["id"], "created_at": row["created_at"].isoformat()}


async def get_order_slow(order_id: str) -> dict | None:
    """Same query but with an artificial 2-second sleep — used by X-Demo-Scenario: slow."""
    async with _pool.acquire(timeout=DB_POOL_ACQUIRE_TIMEOUT) as conn:
        await conn.execute("SELECT pg_sleep(2)")
        row = await conn.fetchrow(
            "SELECT id, created_at FROM orders WHERE id = $1",
            order_id,
        )
    if row is None:
        return None
    return {"id": row["id"], "created_at": row["created_at"].isoformat()}


async def create_order(order_id: str) -> dict:
    async with _pool.acquire(timeout=DB_POOL_ACQUIRE_TIMEOUT) as conn:
        row = await conn.fetchrow(
            """INSERT INTO orders (id) VALUES ($1)
               ON CONFLICT (id) DO UPDATE SET id = EXCLUDED.id
               RETURNING id, created_at""",
            order_id,
        )
        # hold the connection to make pool contention realistic
        await conn.execute("SELECT pg_sleep(0.15)")
    return {"id": row["id"], "created_at": row["created_at"].isoformat()}
