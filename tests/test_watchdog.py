import json
from unittest.mock import MagicMock, patch

import httpx

import scripts.canary_watchdog as canary


def _json_response(payload: dict, status: int = 200) -> MagicMock:
    resp = MagicMock(status_code=status)
    resp.json.return_value = payload
    resp.raise_for_status.return_value = None
    return resp


def _loki_result(*lines: dict) -> dict:
    return {"data": {"result": [{"values": [["1", json.dumps(line)] for line in lines]}]}}


def test_find_trace_id_reads_trace_from_matching_log_line():
    client = MagicMock()
    client.get.return_value = _json_response(_loki_result({"request_id": "canary-1", "trace_id": "abc123"}))
    assert canary.find_trace_id_in_loki(client, "canary-1") == "abc123"
    assert 'canary-1' in client.get.call_args.kwargs["params"]["query"]


def test_find_trace_id_returns_none_when_log_missing():
    client = MagicMock()
    client.get.return_value = _json_response({"data": {"result": []}})
    assert canary.find_trace_id_in_loki(client, "canary-1") is None


def test_trace_exists_in_jaeger():
    client = MagicMock()
    client.get.return_value = _json_response({"data": [{"traceID": "abc123"}]})
    assert canary.trace_exists_in_jaeger(client, "abc123") is True
    client.get.return_value = _json_response({"data": []}, status=404)
    assert canary.trace_exists_in_jaeger(client, "abc123") is False


def test_run_canary_success():
    with patch.object(canary, "send_canary", return_value="canary-1"), \
         patch.object(canary, "find_trace_id_in_loki", return_value="abc123"), \
         patch.object(canary, "trace_exists_in_jaeger", return_value=True):
        assert canary.run_canary() == 0


def test_run_canary_alerts_when_log_never_arrives(monkeypatch):
    monkeypatch.setattr(canary, "WAIT_SECONDS", 0)
    with patch.object(canary, "send_canary", return_value="canary-1"), \
         patch.object(canary, "find_trace_id_in_loki", return_value=None), \
         patch.object(canary, "send_slack_alert") as alert:
        assert canary.run_canary() == 1
    assert "did not reach Loki" in alert.call_args.args[1]


def test_run_canary_cannot_run_when_service_a_down():
    with patch.object(canary, "send_canary", return_value=None):
        assert canary.run_canary() == 2


def test_send_canary_handles_connection_error():
    client = MagicMock()
    client.get.side_effect = httpx.ConnectError("refused")
    assert canary.send_canary(client) is None
