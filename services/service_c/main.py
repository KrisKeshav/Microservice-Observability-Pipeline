import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, HTTPException
from prometheus_client import Gauge
from pydantic import BaseModel

from common import database
from common.database import PoolExhaustedError, create_order, get_order, get_pool_metrics
from common.logging import get_logger, log_error, log_event
from common.metrics import setup_metrics
from common.tracing import setup_telemetry

# Fault injection via the X-Demo-Scenario header. Enabled only in the dev overlay,
# so nobody can trigger failures in prod by sending a header.
FAULT_INJECTION_ENABLED = os.getenv("FAULT_INJECTION_ENABLED", "false").lower() == "true"

logger = get_logger("service-c")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await database.init_db()
    yield
    await database.close_db()


app = FastAPI(title="Service C", lifespan=lifespan)
setup_telemetry(app, "service-c")
setup_metrics(app)

db_pool_active = Gauge("db_pool_active", "Active database connections in the pool")
db_pool_max = Gauge("db_pool_max", "Max database connections configured for pool")
db_pool_active.set_function(lambda: get_pool_metrics()[0])
db_pool_max.set_function(lambda: get_pool_metrics()[1])


class OrderCreate(BaseModel):
    order_id: str


def _db_failure(exc: Exception, request_id: str, order_id: str) -> HTTPException:
    if isinstance(exc, PoolExhaustedError):
        log_error(logger, "db_pool_exhausted", request_id, order_id=order_id, service="c", error=str(exc))
        return HTTPException(status_code=503, detail="Database connection pool exhausted")
    log_error(logger, "db_error", request_id, order_id=order_id, service="c",
              error_type=type(exc).__name__, error=str(exc))
    return HTTPException(status_code=500, detail="Database error")


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "c"}


@app.get("/ready")
async def ready() -> dict[str, str]:
    if not database.is_ready():
        raise HTTPException(status_code=503, detail="Database pool not initialised")
    return {"status": "ready", "service": "c"}


@app.get("/internal/validate/{order_id}")
async def validate_order(
    order_id: str,
    x_request_id: str = Header(...),
    x_demo_scenario: str | None = Header(default=None),
) -> dict:
    log_event(logger, "request_received", x_request_id, order_id=order_id, service="c")
    scenario = x_demo_scenario if FAULT_INJECTION_ENABLED else None

    if scenario == "error":
        log_error(logger, "forced_validation_failure", x_request_id, order_id=order_id, service="c")
        raise HTTPException(status_code=500, detail="Forced Service C validation failure")

    try:
        result = await get_order(order_id, extra_delay=2.0 if scenario == "slow" else 0.0)
    except Exception as exc:
        raise _db_failure(exc, x_request_id, order_id)

    log_event(logger, "request_completed", x_request_id, order_id=order_id, service="c")
    return {"service": "c", "order_id": order_id, "valid": True, "record": result}


@app.post("/internal/orders")
async def create_order_endpoint(body: OrderCreate, x_request_id: str = Header(...)) -> dict:
    log_event(logger, "create_order_received", x_request_id, order_id=body.order_id, service="c")
    try:
        result = await create_order(body.order_id)
    except Exception as exc:
        raise _db_failure(exc, x_request_id, body.order_id)
    log_event(logger, "order_created", x_request_id, order_id=body.order_id, service="c")
    return {"service": "c", "order": result}
