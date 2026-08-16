import asyncio
import json
import os
import time
from collections import deque

import asyncpg
import httpx
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

from common.logging import get_logger, log_error

app = FastAPI(title="Anomaly Detector Service")
Instrumentator().instrument(app).expose(app)
logger = get_logger("anomaly-detector")

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://orders:orders@localhost:5432/orders")
ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://alertmanager:9093")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "app-logs")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "anomaly-detector")
ANOMALY_WINDOW_SEC = int(os.getenv("ANOMALY_WINDOW_SEC", "60"))
ANOMALY_THRESHOLD = int(os.getenv("ANOMALY_THRESHOLD", "5"))
ANOMALY_EVAL_INTERVAL_SEC = float(os.getenv("ANOMALY_EVAL_INTERVAL_SEC", "10"))

_db_pool: asyncpg.Pool | None = None
_window: deque = deque()
_running = True


class AlertResponse(BaseModel):
    id: int
    detected_at: str
    window_sec: int
    error_count: int
    total_count: int
    error_rate: float
    details: dict | None = None


async def init_db() -> None:
    global _db_pool
    _db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    async with _db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS anomaly_alerts (
                id          SERIAL PRIMARY KEY,
                detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                window_sec  INT         NOT NULL,
                error_count INT         NOT NULL,
                total_count INT         NOT NULL,
                error_rate  REAL        NOT NULL,
                details     JSONB
            )
        """)


async def close_db() -> None:
    global _db_pool
    if _db_pool:
        await _db_pool.close()
        _db_pool = None


def kafka_consumer_worker():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])

    while _running:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                logger.error(f"Kafka error: {msg.error()}")
            continue

        try:
            val = msg.value().decode("utf-8")
            data = json.loads(val)
            # Log format can be raw or wrapped payload from Fluent Bit
            log_payload = data.get("log", data)
            if isinstance(log_payload, str):
                try:
                    log_payload = json.loads(log_payload)
                except Exception:
                    log_payload = {"message": log_payload}

            level = str(log_payload.get("levelname", log_payload.get("level", ""))).upper()
            event_name = str(log_payload.get("event", ""))

            is_err = level == "ERROR" or "error" in event_name or event_name.endswith("_timeout") or event_name == "db_pool_exhausted"
            _window.append((time.time(), is_err, log_payload))
        except Exception:
            pass

    consumer.close()


async def evaluator_loop():
    last_alert_time = 0.0
    while _running:
        await asyncio.sleep(ANOMALY_EVAL_INTERVAL_SEC)
        now = time.time()

        # Prune old logs outside sliding window
        cutoff = now - ANOMALY_WINDOW_SEC
        while _window and _window[0][0] < cutoff:
            _window.popleft()

        total_count = len(_window)
        error_count = sum(1 for item in _window if item[1])
        error_rate = (error_count / total_count) if total_count > 0 else 0.0

        if error_count >= ANOMALY_THRESHOLD and (now - last_alert_time) >= ANOMALY_EVAL_INTERVAL_SEC:
            last_alert_time = now
            details = {
                "threshold": ANOMALY_THRESHOLD,
                "recent_errors": [
                    item[2] for item in list(_window) if item[1]
                ][-5:]
            }
            log_error(
                logger,
                "anomaly_alert_triggered",
                "system-alert",
                service="anomaly-detector",
                error_count=error_count,
                total_count=total_count,
                error_rate=error_rate,
                window_sec=ANOMALY_WINDOW_SEC,
            )

            if _db_pool:
                try:
                    async with _db_pool.acquire() as conn:
                        await conn.execute(
                            """
                            INSERT INTO anomaly_alerts (window_sec, error_count, total_count, error_rate, details)
                            VALUES ($1, $2, $3, $4, $5)
                            """,
                            ANOMALY_WINDOW_SEC,
                            error_count,
                            total_count,
                            float(error_rate),
                            json.dumps(details),
                        )
                except Exception as exc:
                    logger.error(f"Failed to record anomaly alert in DB: {exc}")

            await send_alertmanager_alert(error_count, total_count, error_rate, details)


async def send_alertmanager_alert(error_count: int, total_count: int, error_rate: float, details: dict) -> None:
    if not ALERTMANAGER_URL:
        return
    payload = [
        {
            "labels": {
                "alertname": "AnomalyErrorSpike",
                "severity": "critical",
                "service": "anomaly-detector",
            },
            "annotations": {
                "summary": "Log anomaly detected: high error rate",
                "description": f"Detected {error_count} errors out of {total_count} logs ({error_rate * 100:.1f}%) in {ANOMALY_WINDOW_SEC}s window.",
            },
        }
    ]
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.post(f"{ALERTMANAGER_URL}/api/v2/alerts", json=payload)
            resp.raise_for_status()
    except Exception as exc:
        logger.warning(f"Failed to forward alert to Alertmanager: {exc}")


@app.on_event("startup")
async def startup():
    await init_db()
    asyncio.to_thread(kafka_consumer_worker)
    asyncio.create_task(evaluator_loop())


@app.on_event("shutdown")
async def shutdown():
    global _running
    _running = False
    await close_db()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "anomaly-detector"}


@app.get("/alerts")
async def get_alerts(limit: int = 10) -> list[dict]:
    if not _db_pool:
        raise HTTPException(status_code=500, detail="Database connection pool unavailable")

    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, detected_at, window_sec, error_count, total_count, error_rate, details FROM anomaly_alerts ORDER BY id DESC LIMIT $1",
            limit,
        )

    alerts = []
    for r in rows:
        details_val = r["details"]
        if isinstance(details_val, str):
            try:
                details_val = json.loads(details_val)
            except Exception:
                pass
        alerts.append({
            "id": r["id"],
            "detected_at": r["detected_at"].isoformat(),
            "window_sec": r["window_sec"],
            "error_count": r["error_count"],
            "total_count": r["total_count"],
            "error_rate": float(r["error_rate"]),
            "details": details_val,
        })
    return alerts
