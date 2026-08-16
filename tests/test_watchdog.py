from unittest.mock import MagicMock, patch

from scripts.canary_watchdog import check_jaeger, check_loki, run_watchdog, send_slack_alert


def test_check_loki_found():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "data": {
            "result": [
                {"values": [["1234567890", '{"event": "order_created", "request_id": "canary-123"}']]}
            ]
        }
    }
    client = MagicMock()
    client.get.return_value = mock_resp

    assert check_loki(client, 300) is True


def test_check_loki_empty():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"data": {"result": []}}
    client = MagicMock()
    client.get.return_value = mock_resp

    assert check_loki(client, 300) is False


def test_check_jaeger_found():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "data": [
            {
                "traceID": "abc",
                "spans": [
                    {
                        "tags": [{"key": "http.request_id", "value": "canary-123"}]
                    }
                ]
            }
        ]
    }
    client = MagicMock()
    client.get.return_value = mock_resp

    assert check_jaeger(client, 300) is True


def test_check_jaeger_empty():
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {"data": []}
    client = MagicMock()
    client.get.return_value = mock_resp

    assert check_jaeger(client, 300) is False


def test_send_slack_alert(monkeypatch):
    monkeypatch.setattr("scripts.canary_watchdog.SLACK_WEBHOOK_URL", "http://localhost/mock-slack")
    client = MagicMock()
    send_slack_alert(client, "test alert")
    assert client.post.called


def test_run_watchdog_success():
    with patch("scripts.canary_watchdog.cluster_is_reachable", return_value=True), \
         patch("scripts.canary_watchdog.check_loki", return_value=True), \
         patch("scripts.canary_watchdog.check_jaeger", return_value=True):
        code = run_watchdog()
        assert code == 0


def test_run_watchdog_failure():
    with patch("scripts.canary_watchdog.cluster_is_reachable", return_value=True), \
         patch("scripts.canary_watchdog.check_loki", return_value=False), \
         patch("scripts.canary_watchdog.check_jaeger", return_value=True), \
         patch("scripts.canary_watchdog.send_slack_alert") as mock_alert:
        code = run_watchdog()
        assert code == 1
        assert mock_alert.called


def test_run_watchdog_skip_when_cluster_down():
    with patch("scripts.canary_watchdog.cluster_is_reachable", return_value=False):
        code = run_watchdog()
        assert code == 0
