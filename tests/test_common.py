import json
import logging

from common.logging import get_logger, log_error, log_event


def test_get_logger():
    logger = get_logger("test-service")
    assert isinstance(logger, logging.Logger)
    assert logger.name == "test-service"


def test_log_event(capsys):
    logger = get_logger("test-event-logger")
    log_event(logger, "order_created", "req-123", order_id="ord-999")
    captured = capsys.readouterr()
    assert "order_created" in captured.out
    assert "req-123" in captured.out
    data = json.loads(captured.out.strip().splitlines()[-1])
    assert data["event"] == "order_created"
    assert data["order_id"] == "ord-999"


def test_log_error(capsys):
    logger = get_logger("test-error-logger")
    log_error(logger, "db_error", "req-456", error="connection refused")
    captured = capsys.readouterr()
    assert "db_error" in captured.out
    assert "req-456" in captured.out
    data = json.loads(captured.out.strip().splitlines()[-1])
    assert data["event"] == "db_error"
    assert data["error"] == "connection refused"
