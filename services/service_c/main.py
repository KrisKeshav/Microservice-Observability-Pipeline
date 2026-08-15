import asyncio

import asyncpg
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from common.database import close_db, create_order, get_order, get_order_slow, init_db
from common.logging import get_logger, log_error, log_event
from common.tracing import setup_telemetry

app = FastAPI(title="Service C")
setup_telemetry(app, "service-c")
logger = get_logger("service-c")



class OrderCreate(BaseModel):
    order_id: str


@app.on_event("startup")
async def startup():
    await init_db()


@app.on_event("shutdown")
async def shutdown():
    await close_db()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "c"}


@app.get("/internal/validate/{order_id}")
async def validate_order(order_id: str, x_request_id: str = Header(...), x_demo_scenario: str | None = Header(default=None)) -> dict:
    request_id = x_request_id
    log_event(logger, "request_received", request_id, order_id=order_id, service="c")

    if x_demo_scenario == "error":
        log_error(logger, "forced_validation_failure", request_id, order_id=order_id, service="c")
        raise HTTPException(status_code=500, detail="Forced Service C validation failure")

    try:
        log_event(logger, "db_query_start", request_id, order_id=order_id, service="c")
        if x_demo_scenario == "slow":
            result = await get_order_slow(order_id)
        else:
            result = await get_order(order_id)
        log_event(logger, "db_query_success", request_id, order_id=order_id, service="c")
    except (asyncpg.exceptions.InterfaceError, asyncio.TimeoutError) as exc:
        log_error(logger, "db_pool_exhausted", request_id,
                  order_id=order_id, service="c",
                  error_type=type(exc).__name__, error=str(exc))
        raise HTTPException(status_code=503, detail="Database connection pool exhausted")
    except Exception as exc:
        log_error(logger, "db_error", request_id,
                  order_id=order_id, service="c",
                  error_type=type(exc).__name__, error=str(exc))
        raise HTTPException(status_code=500, detail="Database error")

    log_event(logger, "request_completed", request_id, order_id=order_id, service="c")
    return {"service": "c", "order_id": order_id, "valid": True, "record": result}


@app.post("/internal/orders")
async def create_order_endpoint(body: OrderCreate, x_request_id: str = Header(...)) -> dict:
    request_id = x_request_id
    log_event(logger, "create_order_received", request_id, order_id=body.order_id, service="c")

    try:
        result = await create_order(body.order_id)
        log_event(logger, "order_created", request_id, order_id=body.order_id, service="c")
    except (asyncpg.exceptions.InterfaceError, asyncio.TimeoutError) as exc:
        log_error(logger, "db_pool_exhausted", request_id,
                  order_id=body.order_id, service="c",
                  error_type=type(exc).__name__, error=str(exc))
        raise HTTPException(status_code=503, detail="Database connection pool exhausted")
    except Exception as exc:
        log_error(logger, "db_error", request_id,
                  order_id=body.order_id, service="c",
                  error_type=type(exc).__name__, error=str(exc))
        raise HTTPException(status_code=500, detail="Database error")

    return {"service": "c", "order": result}
