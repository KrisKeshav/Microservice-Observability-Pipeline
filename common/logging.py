import logging
import sys

from opentelemetry import trace
from pythonjsonlogger.json import JsonFormatter


def get_logger(service: str) -> logging.Logger:
    logger = logging.getLogger(service)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s %(request_id)s %(event)s %(trace_id)s %(span_id)s")
    )
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


def _get_trace_context() -> dict[str, str]:
    span = trace.get_current_span()
    ctx = span.get_span_context()
    if ctx and ctx.is_valid:
        return {
            "trace_id": f"{ctx.trace_id:032x}",
            "span_id": f"{ctx.span_id:016x}",
        }
    return {}


def log_event(logger: logging.Logger, event: str, request_id: str, **fields: object) -> None:
    extra = {"event": event, "request_id": request_id, **_get_trace_context(), **fields}
    logger.info(event, extra=extra)


def log_error(logger: logging.Logger, event: str, request_id: str, **fields: object) -> None:
    extra = {"event": event, "request_id": request_id, **_get_trace_context(), **fields}
    logger.error(event, extra=extra)

