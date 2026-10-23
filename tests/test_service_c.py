from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

import services.service_c.main as service_c
from common.database import PoolExhaustedError

client = TestClient(service_c.app)
HEADERS = {"X-Request-ID": "req-c"}


@pytest.fixture
def faults_enabled(monkeypatch):
    monkeypatch.setattr(service_c, "FAULT_INJECTION_ENABLED", True)


def test_health():
    assert client.get("/health").json() == {"status": "ok", "service": "c"}


def test_not_ready_without_db_pool():
    assert client.get("/ready").status_code == 503


def test_validate_order_success():
    with patch("services.service_c.main.get_order", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {"id": "ord-c-1", "created_at": "2026-01-01T00:00:00"}
        response = client.get("/internal/validate/ord-c-1", headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["valid"] is True


def test_demo_header_is_ignored_when_fault_injection_disabled():
    with patch("services.service_c.main.get_order", new_callable=AsyncMock, return_value=None):
        response = client.get("/internal/validate/ord-c-1", headers={**HEADERS, "X-Demo-Scenario": "error"})
    assert response.status_code == 200


def test_demo_error_when_fault_injection_enabled(faults_enabled):
    response = client.get("/internal/validate/ord-c-1", headers={**HEADERS, "X-Demo-Scenario": "error"})
    assert response.status_code == 500


def test_demo_slow_adds_delay(faults_enabled):
    with patch("services.service_c.main.get_order", new_callable=AsyncMock, return_value=None) as mock_get:
        client.get("/internal/validate/ord-c-1", headers={**HEADERS, "X-Demo-Scenario": "slow"})
    assert mock_get.call_args.kwargs["extra_delay"] == 2.0


def test_pool_exhausted_returns_503():
    with patch("services.service_c.main.get_order", side_effect=PoolExhaustedError("busy")):
        assert client.get("/internal/validate/ord-c-1", headers=HEADERS).status_code == 503


def test_other_db_error_returns_500():
    with patch("services.service_c.main.get_order", side_effect=ValueError("bad row")):
        assert client.get("/internal/validate/ord-c-1", headers=HEADERS).status_code == 500


def test_create_order_success():
    with patch("services.service_c.main.create_order", new_callable=AsyncMock) as mock_create:
        mock_create.return_value = {"id": "ord-new", "created_at": "2026-01-01T00:00:00"}
        response = client.post("/internal/orders", json={"order_id": "ord-new"}, headers=HEADERS)
    assert response.status_code == 200
    assert response.json()["order"]["id"] == "ord-new"


def test_metrics_expose_pool_gauges():
    body = client.get("/metrics").text
    assert "db_pool_active" in body
    assert "db_pool_max" in body
