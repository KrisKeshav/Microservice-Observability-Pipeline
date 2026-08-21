from unittest.mock import patch

import httpx

from scripts.chaos_drill import (
    _alertmanager_has_firing_alert,
    _prometheus_scalar,
    _query_prometheus,
    _send_request,
    print_report,
)


def test_send_request_success():
    mock_resp = httpx.Response(200, json={"status": "ok"}, request=httpx.Request("GET", "http://test"))
    with patch("httpx.Client.get", return_value=mock_resp):
        client = httpx.Client()
        result = _send_request(client, True, "test-order")
        assert result is not None
        assert result.status_code == 200


def test_send_request_timeout():
    with patch("httpx.Client.get", side_effect=httpx.TimeoutException("timeout")):
        client = httpx.Client()
        result = _send_request(client, True, "test-timeout")
        assert result is None


def test_query_prometheus_success():
    prom_response = {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [{"value": [1234567890, "2"]}],
        },
    }
    mock_resp = httpx.Response(200, json=prom_response, request=httpx.Request("GET", "http://test"))
    with patch("httpx.Client.get", return_value=mock_resp):
        client = httpx.Client()
        result = _query_prometheus(client, "circuit_breaker_state")
        assert result is not None
        assert result["data"]["result"][0]["value"][1] == "2"


def test_query_prometheus_failure():
    with patch("httpx.Client.get", side_effect=Exception("connection refused")):
        client = httpx.Client()
        result = _query_prometheus(client, "some_query")
        assert result is None


def test_prometheus_scalar_returns_first_vector_value():
    response = {"data": {"result": [{"value": [1234567890, "0"]}]}}
    assert _prometheus_scalar(response) == 0.0
    assert _prometheus_scalar({"data": {"result": []}}) is None


def test_alertmanager_has_firing_telemetry_alert():
    alerts = [{"status": {"state": "active"}, "labels": {"alertname": "TelemetryServiceDown"}}]
    mock_response = httpx.Response(200, json=alerts, request=httpx.Request("GET", "http://test"))
    with patch("httpx.Client.get", return_value=mock_response):
        assert _alertmanager_has_firing_alert(httpx.Client(), "TelemetryServiceDown") is True


def test_print_report_circuit(capsys):
    results = [
        {
            "scenario": "circuit",
            "circuit_tripped": True,
            "baseline_latency_ms": 450.0,
            "failfast_latency_ms": 3.2,
        }
    ]
    print_report(results)
    captured = capsys.readouterr()
    assert "CIRCUIT" in captured.out
    assert "3.2ms" in captured.out


def test_print_report_pod_kill(capsys):
    results = [
        {
            "scenario": "pod-kill",
            "mttr_seconds": 12.5,
            "post_recovery_status": 200,
        }
    ]
    print_report(results)
    captured = capsys.readouterr()
    assert "POD-KILL" in captured.out
    assert "12.5s" in captured.out


def test_print_report_db_exhaust(capsys):
    results = [
        {
            "scenario": "db-exhaust",
            "error_count": 7,
            "requests": [{}] * 10,
            "db_pool_alert_firing": True,
            "circuit_state_after_exhaust": "OPEN",
        }
    ]
    print_report(results)
    captured = capsys.readouterr()
    assert "DB-EXHAUST" in captured.out
    assert "7/10" in captured.out


def test_print_report_pipeline_component_kill(capsys):
    print_report([
        {
            "scenario": "pipeline-component-kill",
            "component": "fluent-bit-shipper",
            "up_before": 1.0,
            "up_during": 0.0,
            "up_after": 1.0,
            "alertmanager_firing": True,
            "recovery_seconds": 23.4,
            "slack_confirmation_required": True,
        }
    ])
    captured = capsys.readouterr()
    assert "PIPELINE-COMPONENT-KILL" in captured.out
    assert "1.0/0.0/1.0" in captured.out
    assert "Slack confirmation" in captured.out
