from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator

# Must include 0.3 because the latency SLO counts requests faster than 300ms.
LATENCY_BUCKETS = (0.05, 0.1, 0.3, 0.5, 1, 2.5, 5)

# Probe and scrape traffic would otherwise dilute error rates and SLOs.
EXCLUDED_HANDLERS = ["/metrics", "/health", "/ready"]


def setup_metrics(app: FastAPI) -> None:
    Instrumentator(excluded_handlers=EXCLUDED_HANDLERS).instrument(
        app, latency_lowr_buckets=LATENCY_BUCKETS
    ).expose(app, include_in_schema=False)
