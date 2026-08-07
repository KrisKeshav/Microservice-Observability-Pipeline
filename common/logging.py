"""Structured logging helpers shared by all services."""

import logging
import sys

from pythonjsonlogger.json import JsonFormatter


def get_logger(service: str) -> logging.Logger:
    """Return a stdout logger whose records are one JSON object per line."""
    logger = logging.getLogger(service)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s %(request_id)s %(event)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def log_event(logger: logging.Logger, event: str, request_id: str, **fields: object) -> None:
    """Log an event with correlation fields kept at the top level of the JSON line."""
    logger.info(event, extra={"event": event, "request_id": request_id, **fields})


def log_error(logger: logging.Logger, event: str, request_id: str, **fields: object) -> None:
    """Same as log_event but at ERROR level — pool exhaustion, DB errors, etc."""
    logger.error(event, extra={"event": event, "request_id": request_id, **fields})
