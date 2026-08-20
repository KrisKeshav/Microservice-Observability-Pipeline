from unittest.mock import patch

import httpx

from scripts.chaos_drill import (
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
