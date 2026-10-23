from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi.testclient import TestClient

from common.circuit_breaker import CircuitState
from services.service_b.main import app, circuit_breaker

client = TestClient(app)


@pytest.fixture(autouse=True)
def reset_breaker():
    circuit_breaker.reset()
    circuit_breaker.recovery_timeout = 5.0
    yield
    circuit_breaker.reset()


def _response(status: int, method: str = "GET", **kwargs) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request(method, "http://test"), **kwargs)


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["circuit_state"] == "CLOSED"


def test_process_order_success():
    mock_resp = _response(200, json={"valid": True, "order_id": "ord-1"})
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_resp):
        response = client.get("/internal/orders/ord-1", headers={"X-Request-ID": "req-b-1"})
    assert response.status_code == 200
    assert response.json()["order_id"] == "ord-1"


def test_downstream_error_returns_502_and_is_not_retried():
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=_response(500)) as mock_get:
        response = client.get("/internal/orders/ord-1", headers={"X-Request-ID": "req-b-2"})
    assert response.status_code == 502
    assert mock_get.call_count == 1


def test_connect_error_is_retried_once():
    ok = _response(200, json={"valid": True})
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock,
               side_effect=[httpx.ConnectError("refused"), ok]) as mock_get:
        response = client.get("/internal/orders/ord-1", headers={"X-Request-ID": "req-b-3"})
    assert response.status_code == 200
    assert mock_get.call_count == 2


def test_breaker_opens_after_repeated_failures_and_fails_fast():
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=_response(500)) as mock_get:
        for _ in range(3):
            client.get("/internal/orders/ord-1", headers={"X-Request-ID": "req-b-4"})
        assert circuit_breaker.state == CircuitState.OPEN

        mock_get.reset_mock()
        response = client.get("/internal/orders/ord-1", headers={"X-Request-ID": "req-b-5"})
    assert response.status_code == 503
    assert int(response.headers["Retry-After"]) >= 1
    assert mock_get.call_count == 0


def test_create_order_success():
    mock_resp = _response(200, "POST", json={"id": "ord-10"})
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp):
        response = client.post("/internal/orders", json={"order_id": "ord-10"}, headers={"X-Request-ID": "req-b-6"})
    assert response.status_code == 200
    assert response.json()["order"]["id"] == "ord-10"


def test_metrics():
    assert "circuit_breaker_state" in client.get("/metrics").text
