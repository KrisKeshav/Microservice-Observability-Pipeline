"""Streams application logs from Kafka and alerts when errors spike.

A background thread consumes the `app-logs` topic and records one sample per
application log line. An asyncio task evaluates the sliding window every few
seconds, stores alerts in Postgres and forwards them to Alertmanager.
"""

import asyncio
import json
import os
import threading
import time
from collections import deque
from contextlib import asynccontextmanager

import asyncpg
import httpx
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, HTTPException, Query

from common.logging import get_logger, log_event, log_warning
from common.metrics import setup_metrics

SERVICE_NAME = "anomaly-detector"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://orders:orders@localhost:5432/orders")
ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://alertmanager:9093")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "app-logs")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "anomaly-detector")
ANOMALY_WINDOW_SEC = int(os.getenv("ANOMALY_WINDOW_SEC", "60"))
ANOMALY_THRESHOLD = int(os.getenv("ANOMALY_THRESHOLD", "5"))
ANOMALY_EVAL_INTERVAL_SEC = float(os.getenv("ANOMALY_EVAL_INTERVAL_SEC", "10"))
ANOMALY_COOLDOWN_SEC = float(os.getenv("ANOMALY_COOLDOWN_SEC", "300"))

logger = get_logger(SERVICE_NAME)


class ErrorWindow:
    """Thread-safe sliding window. The Kafka thread writes, the event loop reads."""

    def __init__(self, window_sec: int):
        self.window_sec = window_sec
        self._samples: deque[tuple[float, bool, dict]] = deque()
        self._lock = threading.Lock()

    def add(self, is_error: bool, record: dict, now: float | None = None) -> None:
        with self._lock:
            self._samples.append((now if now is not None else time.time(), is_error, record))

    def stats(self, now: float | None = None) -> tuple[int, int, list[dict]]:
        """Drop expired samples and return (total, errors, last five error records)."""
        cutoff = (now if now is not None else time.time()) - self.window_sec
        with self._lock:
            while self._samples and self._samples[0][0] < cutoff:
                self._samples.popleft()
            samples = list(self._samples)
        errors = [record for _, is_error, record in samples if is_error]
        return len(samples), len(errors), errors[-5:]


def classify(raw: bytes) -> tuple[bool, dict] | None:
    """Return (is_error, record) for an application log line, or None to ignore it.

    Only our structured logs (they carry an "event" field) are counted, and the
    detector's own logs are skipped so an alert can never feed back into itself.
    """
    try:
        record = json.loads(raw)
    except (ValueError, TypeError):
        return None
    if not isinstance(record, dict):
        return None
    # Fallback for a shipper that did not merge the JSON line into the record.
    if isinstance(record.get("log"), str):
        try:
            inner = json.loads(record["log"])
            if isinstance(inner, dict):
                record = inner
        except ValueError:
            pass
    if "event" not in record or record.get("name") == SERVICE_NAME:
        return None
    return str(record.get("levelname", "")).upper() == "ERROR", record


def should_alert(error_count: int, now: float, last_alert_at: float | None) -> bool:
    if error_count < ANOMALY_THRESHOLD:
        return False
    return last_alert_at is None or now - last_alert_at >= ANOMALY_COOLDOWN_SEC


window = ErrorWindow(ANOMALY_WINDOW_SEC)
_stop = threading.Event()
_consumer_thread: threading.Thread | None = None
_db_pool: asyncpg.Pool | None = None


def consume_logs(stop: threading.Event) -> None:
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": KAFKA_GROUP_ID,
        # Detection is about "now": start from the newest logs, not a backlog.
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([KAFKA_TOPIC])
    try:
        while not stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log_warning(logger, "kafka_consumer_error", "system", error=str(msg.error()))
                continue
            classified = classify(msg.value())
            if classified:
                window.add(*classified)
    finally:
        consumer.close()


async def record_alert(total: int, errors: int, recent_errors: list[dict]) -> None:
    error_rate = errors / total if total else 0.0
    details = {"threshold": ANOMALY_THRESHOLD, "recent_errors": recent_errors}
    log_warning(logger, "anomaly_alert_triggered", "system", error_count=errors, total_count=total,
                error_rate=error_rate, window_sec=ANOMALY_WINDOW_SEC)

    if _db_pool:
        try:
            async with _db_pool.acquire() as conn:
                await conn.execute(
                    """INSERT INTO anomaly_alerts (window_sec, error_count, total_count, error_rate, details)
                       VALUES ($1, $2, $3, $4, $5)""",
                    ANOMALY_WINDOW_SEC, errors, total, error_rate, json.dumps(details),
                )
        except Exception as exc:
            log_warning(logger, "anomaly_alert_db_write_failed", "system", error=str(exc))

    await send_alertmanager_alert(errors, total, error_rate)


async def send_alertmanager_alert(error_count: int, total_count: int, error_rate: float) -> None:
    if not ALERTMANAGER_URL:
        return
    payload = [{
        "labels": {"alertname": "AnomalyErrorSpike", "severity": "warning", "job": SERVICE_NAME},
        "annotations": {
            "summary": "Log anomaly detected: error spike",
            "description": f"{error_count} error logs out of {total_count} ({error_rate:.0%}) "
                           f"in the last {ANOMALY_WINDOW_SEC}s.",
        },
    }]
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.post(f"{ALERTMANAGER_URL}/api/v2/alerts", json=payload)
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        log_warning(logger, "alertmanager_forward_failed", "system", error=str(exc))


async def evaluator_loop() -> None:
    last_alert_at: float | None = None
    while not _stop.is_set():
        await asyncio.sleep(ANOMALY_EVAL_INTERVAL_SEC)
        now = time.time()
        total, errors, recent_errors = window.stats(now)
        if should_alert(errors, now, last_alert_at):
            last_alert_at = now
            await record_alert(total, errors, recent_errors)


async def init_db() -> None:
    global _db_pool
    _db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)


async def close_db() -> None:
    global _db_pool
    if _db_pool:
        await _db_pool.close()
        _db_pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _consumer_thread
    await init_db()
    _stop.clear()
    _consumer_thread = threading.Thread(target=consume_logs, args=(_stop,), name="kafka-consumer", daemon=True)
    _consumer_thread.start()
    evaluator = asyncio.create_task(evaluator_loop())
    log_event(logger, "anomaly_detector_started", "system", topic=KAFKA_TOPIC, group=KAFKA_GROUP_ID)
    yield
    _stop.set()
    evaluator.cancel()
    _consumer_thread.join(timeout=5)
    await close_db()


app = FastAPI(title="Anomaly Detector Service", lifespan=lifespan)
setup_metrics(app)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": SERVICE_NAME}


@app.get("/ready")
async def ready() -> dict[str, str]:
    if _db_pool is None or _consumer_thread is None or not _consumer_thread.is_alive():
        raise HTTPException(status_code=503, detail="Kafka consumer or database not running")
    return {"status": "ready", "service": SERVICE_NAME}


@app.get("/alerts")
async def get_alerts(limit: int = Query(10, ge=1, le=100)) -> list[dict]:
    if not _db_pool:
        raise HTTPException(status_code=503, detail="Database connection pool unavailable")

    async with _db_pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT id, detected_at, window_sec, error_count, total_count, error_rate, details
               FROM anomaly_alerts ORDER BY id DESC LIMIT $1""",
            limit,
        )

    alerts = []
    for r in rows:
        details = r["details"]
        if isinstance(details, str):
            details = json.loads(details)
        alerts.append({
            "id": r["id"],
            "detected_at": r["detected_at"].isoformat(),
            "window_sec": r["window_sec"],
            "error_count": r["error_count"],
            "total_count": r["total_count"],
            "error_rate": float(r["error_rate"]),
            "details": details,
        })
    return alerts
