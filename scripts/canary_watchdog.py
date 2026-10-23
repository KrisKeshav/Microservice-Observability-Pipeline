"""End-to-end canary for the telemetry pipeline.

1. Send one request through Service A with a unique request ID.
2. Find that request's log line in Loki (proves Fluent Bit -> Kafka -> Loki works).
3. Read the trace_id from the log line and fetch the trace from Jaeger
   (proves tracing works AND that logs and traces are correlated).

Exit codes: 0 = pipeline healthy, 1 = pipeline broken (Slack alert sent), 2 = could not run.

Usage:
    python scripts/canary_watchdog.py
"""

import json
import os
import sys
import time
import uuid

import httpx

SERVICE_A_URL = os.getenv("SERVICE_A_URL", "http://127.0.0.1:30080")
LOKI_URL = os.getenv("LOKI_URL", "http://127.0.0.1:30100")
JAEGER_URL = os.getenv("JAEGER_URL", "http://127.0.0.1:31686")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
WAIT_SECONDS = int(os.getenv("CANARY_WAIT_SECONDS", "60"))
POLL_SECONDS = 3


def send_canary(client: httpx.Client) -> str | None:
    request_id = f"canary-{uuid.uuid4().hex[:12]}"
    try:
        resp = client.get(f"{SERVICE_A_URL}/api/orders/{request_id}", headers={"X-Request-ID": request_id}, timeout=10)
    except httpx.HTTPError as exc:
        print(f"[canary] Service A unreachable: {exc}", file=sys.stderr)
        return None
    print(f"[canary] sent {request_id} -> HTTP {resp.status_code}")
    return request_id


def find_logs_in_loki(client: httpx.Client, request_id: str, lookback_seconds: int = 600) -> list[dict]:
    """Return the parsed log lines in Loki that mention request_id."""
    params = {
        "query": f'{{job="fluent-bit"}} |= "{request_id}"',
        "start": str(int((time.time() - lookback_seconds) * 1e9)),
        "limit": 50,
    }
    try:
        resp = client.get(f"{LOKI_URL}/loki/api/v1/query_range", params=params, timeout=5)
        resp.raise_for_status()
    except httpx.HTTPError:
        return []
    lines = []
    for stream in resp.json().get("data", {}).get("result", []):
        for _, line in stream.get("values", []):
            try:
                lines.append(json.loads(line))
            except ValueError:
                continue
    return lines


def find_trace_id_in_loki(client: httpx.Client, request_id: str) -> str | None:
    """Return the trace_id from the request's log lines once they reach Loki, else None."""
    return next((line["trace_id"] for line in find_logs_in_loki(client, request_id) if line.get("trace_id")), None)


def trace_exists_in_jaeger(client: httpx.Client, trace_id: str) -> bool:
    try:
        resp = client.get(f"{JAEGER_URL}/api/traces/{trace_id}", timeout=5)
    except httpx.HTTPError:
        return False
    return resp.status_code == 200 and bool(resp.json().get("data"))


def wait_for(check, timeout: int):
    deadline = time.time() + timeout
    while True:
        result = check()
        if result or time.time() >= deadline:
            return result
        time.sleep(POLL_SECONDS)


def send_slack_alert(client: httpx.Client, message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        print(f"[canary] ALERT (no SLACK_WEBHOOK_URL set): {message}", file=sys.stderr)
        return
    try:
        client.post(SLACK_WEBHOOK_URL, json={"text": f":rotating_light: Telemetry canary failed: {message}"}, timeout=5)
    except httpx.HTTPError as exc:
        print(f"[canary] could not post to Slack: {exc}", file=sys.stderr)


def run_canary() -> int:
    with httpx.Client() as client:
        request_id = send_canary(client)
        if request_id is None:
            return 2

        trace_id = wait_for(lambda: find_trace_id_in_loki(client, request_id), WAIT_SECONDS)
        if not trace_id:
            failure = f"log line for {request_id} did not reach Loki within {WAIT_SECONDS}s"
        elif not wait_for(lambda: trace_exists_in_jaeger(client, trace_id), WAIT_SECONDS):
            failure = f"trace {trace_id} for {request_id} not found in Jaeger within {WAIT_SECONDS}s"
        else:
            print(f"[canary] OK: {request_id} found in Loki and its trace {trace_id} found in Jaeger")
            return 0

        print(f"[canary] FAILED: {failure}", file=sys.stderr)
        send_slack_alert(client, failure)
        return 1


if __name__ == "__main__":
    sys.exit(run_canary())
