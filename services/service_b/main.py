import os

import httpx
from fastapi import FastAPI, Header, HTTPException
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

from common.circuit_breaker import CircuitBreaker, CircuitBreakerOpenException
from common.logging import get_logger, log_event
from common.tracing import setup_telemetry

app = FastAPI(title="Service B")
setup_telemetry(app, "service-b")
Instrumentator().instrument(app).expose(app)
logger = get_logger("service-b")
SERVICE_C_URL = os.getenv("SERVICE_C_URL", "http://127.0.0.1:8002")
SERVICE_C_TIMEOUT_SECONDS = float(os.getenv("SERVICE_C_TIMEOUT_SECONDS", "0.5"))

circuit_breaker = CircuitBreaker(
    service="service-b",
    target="service-c",
    failure_threshold=int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "3")),
    recovery_timeout=float(os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "5.0")),
)


class OrderCreate(BaseModel):
    order_id: str


async def _fetch_validation_from_c(order_id: str, headers: dict[str, str]) -> httpx.Response:
    async with httpx.AsyncClient(timeout=SERVICE_C_TIMEOUT_SECONDS) as client:
        response = await client.get(f"{SERVICE_C_URL}/internal/validate/{order_id}", headers=headers)
        response.raise_for_status()
        return response


async def _create_order_in_c(order_id: str, headers: dict[str, str]) -> httpx.Response:
    async with httpx.AsyncClient(timeout=SERVICE_C_TIMEOUT_SECONDS) as client:
        response = await client.post(
            f"{SERVICE_C_URL}/internal/orders",
            json={"order_id": order_id},
            headers=headers,
        )
        response.raise_for_status()
        return response


@app.get("/health")
async def health() -> dict[str, str]:
    return {
        "status": "ok",
        "service": "b",
        "circuit_state": circuit_breaker.state.name,
    }


@app.get("/internal/orders/{order_id}")
async def process_order(
    order_id: str,
    x_request_id: str = Header(...),
    x_demo_scenario: str | None = Header(default=None),
) -> dict:
    request_id = x_request_id
    log_event(logger, "request_received", request_id, order_id=order_id, service="b")
    headers = {"X-Request-ID": request_id}
    if x_demo_scenario:
        headers["X-Demo-Scenario"] = x_demo_scenario

    try:
        response = await circuit_breaker.call(
            _fetch_validation_from_c,
            order_id,
            headers,
            request_id=request_id,
            retries=1,
        )
    except CircuitBreakerOpenException as exc:
        log_event(
            logger,
            "service_c_circuit_open",
            request_id,
            order_id=order_id,
            service="b",
            retry_after=exc.retry_after,
        )
        raise HTTPException(
            status_code=503,
            detail=f"Service C is degraded (Circuit Breaker OPEN). Retry after {exc.retry_after:.1f}s",
            headers={"Retry-After": str(int(exc.retry_after))},
        )
    except httpx.TimeoutException:
        log_event(logger, "service_c_timeout", request_id, order_id=order_id, service="b")
        raise HTTPException(status_code=504, detail="Service B timed out waiting for Service C")
    except httpx.HTTPStatusError as exc:
        log_event(
            logger,
            "service_c_error",
            request_id,
            order_id=order_id,
            service="b",
            downstream_status=exc.response.status_code,
        )
        raise HTTPException(status_code=502, detail="Service C rejected the order")
    except httpx.RequestError as exc:
        log_event(logger, "service_c_unavailable", request_id, order_id=order_id, service="b", error=str(exc))
        raise HTTPException(status_code=503, detail="Service C is unavailable")

    log_event(logger, "request_completed", request_id, order_id=order_id, service="b")
    return {"service": "b", "order_id": order_id, "validation": response.json()}


@app.post("/internal/orders")
async def create_order(body: OrderCreate, x_request_id: str = Header(...)) -> dict:
    request_id = x_request_id
    log_event(logger, "create_order_received", request_id, order_id=body.order_id, service="b")
    headers = {"X-Request-ID": request_id}

    try:
        response = await circuit_breaker.call(
            _create_order_in_c,
            body.order_id,
            headers,
            request_id=request_id,
            retries=1,
        )
    except CircuitBreakerOpenException as exc:
        log_event(
            logger,
            "service_c_circuit_open",
            request_id,
            order_id=body.order_id,
            service="b",
            retry_after=exc.retry_after,
        )
        raise HTTPException(
            status_code=503,
            detail=f"Service C is degraded (Circuit Breaker OPEN). Retry after {exc.retry_after:.1f}s",
            headers={"Retry-After": str(int(exc.retry_after))},
        )
    except httpx.TimeoutException:
        log_event(logger, "service_c_timeout", request_id, order_id=body.order_id, service="b")
        raise HTTPException(status_code=504, detail="Service B timed out waiting for Service C")
    except httpx.HTTPStatusError as exc:
        log_event(
            logger,
            "service_c_error",
            request_id,
            order_id=body.order_id,
            service="b",
            downstream_status=exc.response.status_code,
        )
        raise HTTPException(status_code=502, detail="Service C could not create the order")
    except httpx.RequestError as exc:
        log_event(logger, "service_c_unavailable", request_id, order_id=body.order_id, service="b", error=str(exc))
        raise HTTPException(status_code=503, detail="Service C is unavailable")

    log_event(logger, "order_created", request_id, order_id=body.order_id, service="b")
    return {"service": "b", "order": response.json()}

