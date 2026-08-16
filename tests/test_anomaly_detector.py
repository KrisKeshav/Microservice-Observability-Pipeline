from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient

import services.anomaly_detector.main as ad_module
from services.anomaly_detector.main import app

client = TestClient(app)


def test_anomaly_detector_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "anomaly-detector"}


def test_anomaly_detector_alerts_db_unavailable():
    ad_module._db_pool = None
    response = client.get("/alerts")
    assert response.status_code == 500


def test_anomaly_detector_alerts_success():
    mock_conn = AsyncMock()
    mock_conn.fetch.return_value = [
        {
            "id": 1,
            "detected_at": datetime.now(timezone.utc),
            "window_sec": 60,
            "error_count": 5,
            "total_count": 10,
            "error_rate": 0.5,
            "details": {"threshold": 5},
        }
    ]

    class MockAcquire:
        async def __aenter__(self):
            return mock_conn

        async def __aexit__(self, *args):
            pass

    mock_pool = MagicMock()
    mock_pool.acquire.return_value = MockAcquire()
    ad_module._db_pool = mock_pool

    response = client.get("/alerts")
    assert response.status_code == 200
    alerts = response.json()
    assert len(alerts) == 1
    assert alerts[0]["error_count"] == 5


def test_anomaly_detector_metrics():
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "http_requests_total" in response.text


def test_send_alertmanager_alert():
    import asyncio
    from unittest.mock import AsyncMock, patch

    import httpx

    mock_resp = httpx.Response(200, json={}, request=httpx.Request("POST", "http://test"))
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post:
        mock_post.return_value = mock_resp
        asyncio.run(ad_module.send_alertmanager_alert(5, 10, 0.5, {"threshold": 5}))
        assert mock_post.called
