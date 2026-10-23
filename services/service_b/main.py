import os
from contextlib import asynccontextmanager
from typing import Awaitable, Callable
from urllib.parse import quote

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from common.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from common.logging import get_logger, log_error, log_event, log_warning
from common.metrics import setup_metrics
from common.tracing import setup_telemetry

SERVICE_C_URL = os.getenv("SERVICE_C_URL", "http://127.0.0.1:8002")
# Shorter than Service A's timeout so B can answer A before A gives up.
SERVICE_C_TIMEOUT_SECONDS = float(os.getenv("SERVICE_C_TIMEOUT_SECONDS", "3"))

logger = get_logger("service-b")


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await http_client.aclose()


app = FastAPI(title="Service B", lifespan=lifespan)
setup_telemetry(app, "service-b")
setup_metrics(app)
http_client = httpx.AsyncClient(base_url=SERVICE_C_URL, timeout=SERVICE_C_TIMEOUT_SECONDS)

circuit_breaker = CircuitBreaker(
    service="service-b",
    target="service-c",
    failure_threshold=int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "3")),
    recovery_timeout=float(os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "5.0")),
)


class OrderCreate(BaseModel):
    order_id: str


async def _send(call: Callable[[], Awaitable[httpx.Response]]) -> httpx.Response:
    response = await call()
    response.raise_for_status()
    return response


async def _call_service_c(call: Callable[[], Awaitable[httpx.Response]], request_id: str, order_id: str) -> dict:
    """Call Service C through the circuit breaker and map failures to HTTP errors."""
    try:
        # Retry only when the connection was never made; that request did not reach C.
        response = await circuit_breaker.call(
            _send, call, request_id=request_id, retries=1, retry_on=(httpx.ConnectError,)
        )
    except CircuitBreakerOpenException as exc:
        log_warning(logger, "service_c_circuit_open", request_id, order_id=order_id, service="b",
                    retry_after=exc.retry_after)
        raise HTTPException(
            status_code=503,
            detail="Service C is degraded (circuit breaker open)",
            headers={"Retry-After": exc.retry_after_header},
        )
    except httpx.TimeoutException:
        log_error(logger, "service_c_timeout", request_id, order_id=order_id, service="b")
        raise HTTPException(status_code=504, detail="Service B timed out waiting for Service C")
    except httpx.HTTPStatusError as exc:
        log_error(logger, "service_c_error", request_id, order_id=order_id, service="b",
                  downstream_status=exc.response.status_code)
        raise HTTPException(status_code=502, detail="Service C could not complete the request")
    except httpx.RequestError as exc:
        log_error(logger, "service_c_unavailable", request_id, order_id=order_id, service="b", error=str(exc))
        raise HTTPException(status_code=503, detail="Service C is unavailable")
    return response.json()


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "b", "circuit_state": circuit_breaker.state.name}


@app.get("/ready")
async def ready() -> dict[str, str]:
    # The circuit breaker already handles a broken Service C; readiness stays local.
    return {"status": "ready", "service": "b"}


@app.get("/internal/orders/{order_id}")
async def process_order(
    order_id: str,
    x_request_id: str = Header(...),
    x_demo_scenario: str | None = Header(default=None),
) -> dict:
    log_event(logger, "request_received", x_request_id, order_id=order_id, service="b")
    headers = {"X-Request-ID": x_request_id}
    if x_demo_scenario:
        headers["X-Demo-Scenario"] = x_demo_scenario
    path = f"/internal/validate/{quote(order_id, safe='')}"
    validation = await _call_service_c(lambda: http_client.get(path, headers=headers), x_request_id, order_id)
    log_event(logger, "request_completed", x_request_id, order_id=order_id, service="b")
    return {"service": "b", "order_id": order_id, "validation": validation}


@app.post("/internal/orders")
async def create_order(body: OrderCreate, x_request_id: str = Header(...)) -> dict:
    log_event(logger, "create_order_received", x_request_id, order_id=body.order_id, service="b")
    order = await _call_service_c(
        lambda: http_client.post(
            "/internal/orders", json={"order_id": body.order_id}, headers={"X-Request-ID": x_request_id}
        ),
        x_request_id,
        body.order_id,
    )
    log_event(logger, "order_created", x_request_id, order_id=body.order_id, service="b")
    return {"service": "b", "order": order}
