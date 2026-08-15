from unittest.mock import AsyncMock, patch

import httpx
from fastapi.testclient import TestClient

from services.service_a.main import app

client = TestClient(app)


def test_service_a_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "a"}


def test_service_a_get_order_success():
    mock_resp = httpx.Response(200, json={"service": "b", "order_id": "ord-1", "validation": {"valid": True}}, request=httpx.Request("GET", "http://test"))
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock) as mock_get:
        mock_get.return_value = mock_resp
        response = client.get("/api/orders/ord-1", headers={"X-Request-ID": "test-req"})
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "a"
        assert data["request_id"] == "test-req"


def test_service_a_get_order_timeout():
    with patch("httpx.AsyncClient.get", side_effect=httpx.TimeoutException("Timeout", request=httpx.Request("GET", "http://test"))):
        response = client.get("/api/orders/ord-1")
        assert response.status_code == 504


def test_service_a_create_order_success():
    mock_resp = httpx.Response(200, json={"service": "b", "order": {"id": "ord-2"}}, request=httpx.Request("POST", "http://test"))
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_resp
        response = client.post("/api/orders", json={"order_id": "ord-2"}, headers={"X-Request-ID": "test-req"})
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "a"
        assert data["order"]["service"] == "b"
