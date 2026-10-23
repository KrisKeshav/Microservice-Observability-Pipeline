from unittest.mock import MagicMock, patch

import httpx
import pytest

import scripts.chaos_drill as drill


def _resp(status: int, headers: dict | None = None, payload=None) -> httpx.Response:
    return httpx.Response(status, headers=headers, json=payload, request=httpx.Request("GET", "http://test"))


def test_circuit_rejection_requires_503_with_retry_after():
    assert drill.is_circuit_rejection(_resp(503, {"Retry-After": "5"})) is True
    # A fast 502 from a plain downstream error is NOT the breaker.
    assert drill.is_circuit_rejection(_resp(502)) is False
    assert drill.is_circuit_rejection(_resp(503)) is False
    assert drill.is_circuit_rejection(None) is False


def test_send_order_request_reports_timeout():
    client = MagicMock()
    client.get.side_effect = httpx.TimeoutException("slow")
    status, _, resp = drill.send_order_request(client)
    assert status == "TIMEOUT"
    assert resp is None


def test_query_prometheus_returns_first_value():
    client = MagicMock()
    client.get.return_value = _resp(200, payload={"data": {"result": [{"value": [0, "2"]}]}})
    assert drill.query_prometheus(client, "circuit_breaker_state") == 2.0
    client.get.return_value = _resp(200, payload={"data": {"result": []}})
    assert drill.query_prometheus(client, "circuit_breaker_state") is None


def test_alertmanager_has_firing_alert():
    client = MagicMock()
    client.get.return_value = _resp(200, payload=[
        {"labels": {"alertname": "TelemetryServiceDown"}, "status": {"state": "active"}},
        {"labels": {"alertname": "DbPoolExhaustion"}, "status": {"state": "suppressed"}},
    ])
    assert drill.alertmanager_has_firing_alert(client, "TelemetryServiceDown") is True
    assert drill.alertmanager_has_firing_alert(client, "DbPoolExhaustion") is False


def test_refuses_to_run_against_prod(monkeypatch, capsys):
    monkeypatch.setattr(drill, "NAMESPACE", "prod")
    with patch("sys.argv", ["chaos_drill.py"]):
        assert drill.main() == 2
    assert "Refusing" in capsys.readouterr().out


def test_circuit_scenario_detects_breaker(monkeypatch):
    calls = iter([("502", 40.0, _resp(502))] * 3 + [(503, 2.0, _resp(503, {"Retry-After": "5"}))] * 7 + [(200, 30.0, None)])
    monkeypatch.setattr(drill, "send_order_request", lambda *a, **k: next(calls))
    monkeypatch.setattr(drill, "query_prometheus", lambda *a: 2.0)
    monkeypatch.setattr(drill.time, "sleep", lambda s: None)
    result = drill.run_circuit(MagicMock())
    assert result["circuit_opened"] is True
    assert result["requests_before_open"] == 3
    assert result["avg_rejection_latency_ms"] == 2.0
    assert result["recovered_status"] == 200


@pytest.mark.parametrize("scenario", ["circuit", "pod-kill", "db-exhaust", "log-buffer"])
def test_print_report_shows_only_measured_values(scenario, capsys):
    drill.print_report([{"scenario": scenario, "measured_value": 12.5}])
    out = capsys.readouterr().out
    assert scenario.upper() in out
    assert "12.5" in out
    assert "MTTR COMPARISON" not in out
