from unittest.mock import AsyncMock, patch

import httpx
from fastapi.testclient import TestClient

from services.service_b.main import app

client = TestClient(app)


def test_service_b_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "b"}


def test_service_b_process_order_success():
    mock_resp = httpx.Response(200, json={"valid": True, "order_id": "ord-1"}, request=httpx.Request("GET", "http://test"))
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_resp
        response = client.get("/internal/orders/ord-1", headers={"X-Request-ID": "req-b-1"})
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "b"
        assert data["order_id"] == "ord-1"


def test_service_b_process_order_downstream_error():
    mock_err_resp = httpx.Response(500, request=httpx.Request("GET", "http://test"))
    with patch("httpx.AsyncClient.get", side_effect=httpx.HTTPStatusError("500 Error", request=httpx.Request("GET", "http://test"), response=mock_err_resp)):
        response = client.get("/internal/orders/ord-1", headers={"X-Request-ID": "req-b-2"})
        assert response.status_code == 502


def test_service_b_create_order_success():
    mock_resp = httpx.Response(200, json={"id": "ord-10", "created_at": "2026-01-01T00:00:00"}, request=httpx.Request("POST", "http://test"))
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_resp
        response = client.post("/internal/orders", json={"order_id": "ord-10"}, headers={"X-Request-ID": "req-b-3"})
        assert response.status_code == 200
        assert response.json()["service"] == "b"
