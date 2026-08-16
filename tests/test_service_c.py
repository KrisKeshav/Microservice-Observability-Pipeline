from unittest.mock import AsyncMock, patch

import asyncpg
from fastapi.testclient import TestClient

from services.service_c.main import app

client = TestClient(app)


def test_service_c_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "c"}


def test_service_c_validate_order_success():
    with patch("services.service_c.main.get_order", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = {"id": "ord-c-1", "created_at": "2026-01-01T00:00:00"}
        response = client.get("/internal/validate/ord-c-1", headers={"X-Request-ID": "req-c-1"})
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "c"
        assert data["order_id"] == "ord-c-1"
        assert data["valid"] is True


def test_service_c_validate_order_demo_error():
    response = client.get(
        "/internal/validate/ord-c-1",
        headers={"X-Request-ID": "req-c-err", "X-Demo-Scenario": "error"},
    )
    assert response.status_code == 500


def test_service_c_validate_order_db_pool_exhausted():
    with patch("services.service_c.main.get_order", side_effect=asyncpg.exceptions.InterfaceError("pool error")):
        response = client.get("/internal/validate/ord-c-1", headers={"X-Request-ID": "req-c-db"})
        assert response.status_code == 503


def test_service_c_create_order_success():
    with patch("services.service_c.main.create_order", new_callable=AsyncMock) as mock_create:
        mock_create.return_value = {"id": "ord-new", "created_at": "2026-01-01T00:00:00"}
        response = client.post("/internal/orders", json={"order_id": "ord-new"}, headers={"X-Request-ID": "req-c-post"})
        assert response.status_code == 200
        assert response.json()["order"]["id"] == "ord-new"


def test_service_c_metrics():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "db_pool_active" in response.text
    assert "db_pool_max" in response.text
