import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi.testclient import TestClient

import services.anomaly_detector.main as ad

client = TestClient(ad.app)


def _log(levelname: str = "INFO", name: str = "service-c", **fields) -> bytes:
    return json.dumps({"levelname": levelname, "name": name, "event": "x", **fields}).encode()


def test_classify_counts_app_errors():
    assert ad.classify(_log("ERROR"))[0] is True
    assert ad.classify(_log("INFO"))[0] is False


def test_classify_ignores_own_logs_and_non_app_lines():
    assert ad.classify(_log("ERROR", name="anomaly-detector")) is None
    assert ad.classify(json.dumps({"message": "kafka broker log"}).encode()) is None
    assert ad.classify(b"not json") is None


def test_classify_unwraps_unmerged_log_field():
    wrapped = json.dumps({"log": _log("ERROR").decode(), "stream": "stdout"}).encode()
    assert ad.classify(wrapped)[0] is True


def test_window_expires_old_samples():
    window = ad.ErrorWindow(window_sec=60)
    window.add(True, {"n": 1}, now=0)
    window.add(False, {"n": 2}, now=50)
    window.add(True, {"n": 3}, now=100)
    total, errors, recent = window.stats(now=105)
    assert (total, errors) == (2, 1)
    assert recent == [{"n": 3}]


def test_should_alert_respects_threshold_and_cooldown(monkeypatch):
    monkeypatch.setattr(ad, "ANOMALY_THRESHOLD", 5)
    monkeypatch.setattr(ad, "ANOMALY_COOLDOWN_SEC", 300)
    assert ad.should_alert(4, now=1000, last_alert_at=None) is False
    assert ad.should_alert(5, now=1000, last_alert_at=None) is True
    assert ad.should_alert(50, now=1100, last_alert_at=1000) is False
    assert ad.should_alert(50, now=1300, last_alert_at=1000) is True


def test_lifespan_starts_consumer_thread():
    started = []

    def fake_consume(stop):
        started.append(True)
        stop.wait()

    with patch.object(ad, "init_db", AsyncMock()), patch.object(ad, "close_db", AsyncMock()), \
         patch.object(ad, "consume_logs", fake_consume), patch.object(ad, "_db_pool", MagicMock()):
        with TestClient(ad.app) as running:
            assert started == [True]
            assert running.get("/ready").status_code == 200
    assert not ad._consumer_thread.is_alive()


def test_health():
    assert client.get("/health").json() == {"status": "ok", "service": "anomaly-detector"}


def test_alerts_without_db_returns_503(monkeypatch):
    monkeypatch.setattr(ad, "_db_pool", None)
    assert client.get("/alerts").status_code == 503


def test_alerts_success(monkeypatch):
    conn = AsyncMock()
    conn.fetch.return_value = [{
        "id": 1, "detected_at": datetime.now(timezone.utc), "window_sec": 60,
        "error_count": 5, "total_count": 10, "error_rate": 0.5, "details": '{"threshold": 5}',
    }]
    pool = MagicMock()
    pool.acquire.return_value.__aenter__ = AsyncMock(return_value=conn)
    pool.acquire.return_value.__aexit__ = AsyncMock(return_value=None)
    monkeypatch.setattr(ad, "_db_pool", pool)

    alerts = client.get("/alerts").json()
    assert alerts[0]["error_count"] == 5
    assert alerts[0]["details"] == {"threshold": 5}


def test_send_alertmanager_alert():
    ok = httpx.Response(200, json={}, request=httpx.Request("POST", "http://test"))
    with patch("httpx.AsyncClient.post", new_callable=AsyncMock, return_value=ok) as mock_post:
        asyncio.run(ad.send_alertmanager_alert(5, 10, 0.5))
    payload = mock_post.call_args.kwargs["json"]
    assert payload[0]["labels"]["alertname"] == "AnomalyErrorSpike"
