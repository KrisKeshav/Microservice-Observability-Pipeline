import os
import subprocess
import sys
from unittest.mock import AsyncMock, patch

import httpx
from fastapi.testclient import TestClient

from services.service_a.main import app

client = TestClient(app)


def _response(status: int, method: str = "GET", **kwargs) -> httpx.Response:
    return httpx.Response(status, request=httpx.Request(method, "http://test"), **kwargs)


def test_health_and_ready():
    assert client.get("/health").json() == {"status": "ok", "service": "a"}
    assert client.get("/ready").status_code == 200


def test_get_order_success():
    mock_resp = _response(200, json={"service": "b", "order_id": "ord-1", "validation": {"valid": True}})
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_resp):
        response = client.get("/api/orders/ord-1", headers={"X-Request-ID": "test-req"})
    assert response.status_code == 200
    assert response.json()["request_id"] == "test-req"


def test_get_order_timeout_returns_504():
    with patch("httpx.AsyncClient.get", side_effect=httpx.TimeoutException("Timeout")):
        assert client.get("/api/orders/ord-1").status_code == 504


def test_downstream_503_passes_retry_after_through():
    mock_resp = _response(503, headers={"Retry-After": "4"})
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_resp):
        response = client.get("/api/orders/ord-1")
    assert response.status_code == 503
    assert response.headers["Retry-After"] == "4"


def test_downstream_500_returns_502():
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=_response(500)):
        assert client.get("/api/orders/ord-1").status_code == 502


def test_order_id_is_url_encoded_for_downstream_call():
    mock_resp = _response(200, json={})
    with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=mock_resp) as mock_get:
        client.get("/api/orders/a%3Fadmin%3Dtrue")
    assert mock_get.call_args.args[0] == "/internal/orders/a%3Fadmin%3Dtrue"


def test_create_order_success():
    mock_resp = _response(200, "POST", json={"service": "b", "order": {"id": "ord-2"}})
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=mock_resp):
        response = client.post("/api/orders", json={"order_id": "ord-2"}, headers={"X-Request-ID": "test-req"})
    assert response.status_code == 200
    assert response.json()["order"]["service"] == "b"


METRICS_PROBE = """
from unittest.mock import AsyncMock, patch
import httpx
from fastapi.testclient import TestClient
from services.service_a.main import app
client = TestClient(app)
client.get("/health")
ok = httpx.Response(200, json={}, request=httpx.Request("GET", "http://test"))
with patch("httpx.AsyncClient.get", new_callable=AsyncMock, return_value=ok):
    client.get("/api/orders/ord-metrics")
print(client.get("/metrics").text)
"""


def test_metrics_have_slo_bucket_and_exclude_probes():
    # Fresh process: every service shares prometheus_client's global registry, so
    # importing all four apps into one test process would mix their metrics.
    result = subprocess.run([sys.executable, "-c", METRICS_PROBE], capture_output=True, text=True, check=True,
                            env={**os.environ, "OTEL_SDK_DISABLED": "true"})
    body = result.stdout
    assert 'le="0.3"' in body
    assert 'handler="/api/orders/{order_id}"' in body
    assert 'handler="/health"' not in body
