import asyncio
import os
import random

from fastapi import FastAPI, Header, HTTPException

from common.logging import get_logger, log_event

app = FastAPI(title="Service C")
logger = get_logger("service-c")
FAILURE_RATE = float(os.getenv("SERVICE_C_FAILURE_RATE", "0.25"))
SLOW_RATE = float(os.getenv("SERVICE_C_SLOW_RATE", "0.15"))
SLOW_RESPONSE_SECONDS = float(os.getenv("SERVICE_C_SLOW_RESPONSE_SECONDS", "1.5"))


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "c"}


@app.get("/internal/validate/{order_id}")
async def validate_order(order_id: str, x_request_id: str = Header(...), x_demo_scenario: str | None = Header(default=None)) -> dict:
    request_id = x_request_id
    scenario = x_demo_scenario or _choose_scenario()
    log_event(logger, "request_received", request_id, order_id=order_id, service="c", scenario=scenario)
    if scenario == "slow":
        log_event(logger, "intentional_slow_response", request_id, order_id=order_id, service="c")
        await asyncio.sleep(SLOW_RESPONSE_SECONDS)
    elif scenario == "error":
        log_event(logger, "intentional_validation_failure", request_id, order_id=order_id, service="c")
        raise HTTPException(status_code=500, detail="Intentional Service C validation failure")
    elif scenario != "success":
        raise HTTPException(status_code=400, detail="X-Demo-Scenario must be success, error, or slow")
    log_event(logger, "request_completed", request_id, order_id=order_id, service="c")
    return {"service": "c", "order_id": order_id, "valid": True}


def _choose_scenario() -> str:
    roll = random.random()
    if roll < FAILURE_RATE:
        return "error"
    if roll < FAILURE_RATE + SLOW_RATE:
        return "slow"
    return "success"
