import os
import uuid

import httpx
from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from common.logging import get_logger, log_event
from common.tracing import setup_telemetry

app = FastAPI(title="Service A")
setup_telemetry(app, "service-a")
logger = get_logger("service-a")
SERVICE_B_URL = os.getenv("SERVICE_B_URL", "http://127.0.0.1:8001")
SERVICE_B_TIMEOUT_SECONDS = float(os.getenv("SERVICE_B_TIMEOUT_SECONDS", "3"))



class OrderCreate(BaseModel):
    order_id: str


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "a"}


@app.get("/api/orders/{order_id}")
async def get_order(order_id: str, x_request_id: str | None = Header(default=None), x_demo_scenario: str | None = Header(default=None)) -> dict:
    request_id = x_request_id or str(uuid.uuid4())
    log_event(logger, "request_received", request_id, order_id=order_id, service="a")
    headers = {"X-Request-ID": request_id}
    if x_demo_scenario:
        headers["X-Demo-Scenario"] = x_demo_scenario
    try:
        async with httpx.AsyncClient(timeout=SERVICE_B_TIMEOUT_SECONDS) as client:
            response = await client.get(f"{SERVICE_B_URL}/internal/orders/{order_id}", headers=headers)
        response.raise_for_status()
    except httpx.TimeoutException:
        log_event(logger, "service_b_timeout", request_id, order_id=order_id, service="a")
        raise HTTPException(status_code=504, detail="Service A timed out waiting for Service B")
    except httpx.HTTPStatusError as exc:
        log_event(logger, "service_b_error", request_id, order_id=order_id, service="a", downstream_status=exc.response.status_code)
        raise HTTPException(status_code=502, detail="Service B could not complete the order")
    except httpx.RequestError as exc:
        log_event(logger, "service_b_unavailable", request_id, order_id=order_id, service="a", error=str(exc))
        raise HTTPException(status_code=503, detail="Service B is unavailable")
    log_event(logger, "request_completed", request_id, order_id=order_id, service="a")
    return {"service": "a", "request_id": request_id, "result": response.json()}


@app.post("/api/orders")
async def create_order(body: OrderCreate, x_request_id: str | None = Header(default=None)) -> dict:
    request_id = x_request_id or str(uuid.uuid4())
    log_event(logger, "create_order_received", request_id, order_id=body.order_id, service="a")
    try:
        async with httpx.AsyncClient(timeout=SERVICE_B_TIMEOUT_SECONDS) as client:
            response = await client.post(
                f"{SERVICE_B_URL}/internal/orders",
                json={"order_id": body.order_id},
                headers={"X-Request-ID": request_id},
            )
        response.raise_for_status()
    except httpx.TimeoutException:
        log_event(logger, "service_b_timeout", request_id, order_id=body.order_id, service="a")
        raise HTTPException(status_code=504, detail="Service A timed out waiting for Service B")
    except httpx.HTTPStatusError as exc:
        log_event(logger, "service_b_error", request_id, order_id=body.order_id, service="a", downstream_status=exc.response.status_code)
        raise HTTPException(status_code=502, detail="Service B could not create the order")
    except httpx.RequestError as exc:
        log_event(logger, "service_b_unavailable", request_id, order_id=body.order_id, service="a", error=str(exc))
        raise HTTPException(status_code=503, detail="Service B is unavailable")
    log_event(logger, "order_created", request_id, order_id=body.order_id, service="a")
    return {"service": "a", "request_id": request_id, "order": response.json()}
