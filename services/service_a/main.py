import os
import uuid
from contextlib import asynccontextmanager
from typing import Awaitable
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from common.logging import get_logger, log_error, log_event
from common.metrics import setup_metrics
from common.tracing import setup_telemetry

SERVICE_B_URL = os.getenv("SERVICE_B_URL", "http://127.0.0.1:8001")
# Must be longer than Service B's own budget for calling Service C.
SERVICE_B_TIMEOUT_SECONDS = float(os.getenv("SERVICE_B_TIMEOUT_SECONDS", "5"))

logger = get_logger("service-a")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await http_client.aclose()


app = FastAPI(title="Service A", lifespan=lifespan)
setup_telemetry(app, "service-a")
setup_metrics(app)
# One pooled client per process; created after tracing so it is instrumented.
http_client = httpx.AsyncClient(base_url=SERVICE_B_URL, timeout=SERVICE_B_TIMEOUT_SECONDS)


class OrderCreate(BaseModel):
    order_id: str


async def _forward(call: Awaitable[httpx.Response], request_id: str, order_id: str) -> dict:
    """Await a call to Service B and translate failures into gateway responses."""
    try:
        response = await call
        response.raise_for_status()
    except httpx.TimeoutException:
        log_error(logger, "service_b_timeout", request_id, order_id=order_id, service="a")
        raise HTTPException(status_code=504, detail="Service A timed out waiting for Service B")
    except httpx.HTTPStatusError as exc:
        status = exc.response.status_code
        log_error(logger, "service_b_error", request_id, order_id=order_id, service="a", downstream_status=status)
        if status == 503:
            # Pass "try again later" through so clients see the circuit breaker's Retry-After.
            retry_after = exc.response.headers.get("Retry-After")
            raise HTTPException(
                status_code=503,
                detail="Order service is temporarily unavailable",
                headers={"Retry-After": retry_after} if retry_after else None,
            )
        raise HTTPException(status_code=502, detail="Service B could not complete the request")
    except httpx.RequestError as exc:
        log_error(logger, "service_b_unavailable", request_id, order_id=order_id, service="a", error=str(exc))
        raise HTTPException(status_code=503, detail="Service B is unavailable")
    return response.json()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "a"}


@app.get("/ready")
async def ready() -> dict[str, str]:
    # Stateless gateway: readiness deliberately does not depend on Service B,
    # otherwise one slow dependency would pull every gateway pod out of rotation.
    return {"status": "ready", "service": "a"}


@app.get("/api/orders/{order_id}")
async def get_order(
    order_id: str,
    x_request_id: str | None = Header(default=None),
    x_demo_scenario: str | None = Header(default=None),
) -> dict:
    request_id = x_request_id or str(uuid.uuid4())
    log_event(logger, "request_received", request_id, order_id=order_id, service="a")
    headers = {"X-Request-ID": request_id}
    if x_demo_scenario:
        headers["X-Demo-Scenario"] = x_demo_scenario
    result = await _forward(
        http_client.get(f"/internal/orders/{quote(order_id, safe='')}", headers=headers), request_id, order_id
    )
    log_event(logger, "request_completed", request_id, order_id=order_id, service="a")
    return {"service": "a", "request_id": request_id, "result": result}


@app.post("/api/orders")
async def create_order(body: OrderCreate, x_request_id: str | None = Header(default=None)) -> dict:
    request_id = x_request_id or str(uuid.uuid4())
    log_event(logger, "create_order_received", request_id, order_id=body.order_id, service="a")
    order = await _forward(
        http_client.post("/internal/orders", json={"order_id": body.order_id}, headers={"X-Request-ID": request_id}),
        request_id,
        body.order_id,
    )
    log_event(logger, "order_created", request_id, order_id=body.order_id, service="a")
    return {"service": "a", "request_id": request_id, "order": order}
