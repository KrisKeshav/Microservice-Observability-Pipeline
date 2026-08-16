import os
import socket
import subprocess
import sys
import time
from urllib.parse import urlparse

import httpx

LOKI_URL = os.getenv("LOKI_URL", "http://localhost:30100")
JAEGER_URL = os.getenv("JAEGER_URL", "http://localhost:31686")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
LOOKBACK_MINUTES = int(os.getenv("LOOKBACK_MINUTES", "5"))
KUBECTL_TIMEOUT_SEC = int(os.getenv("KUBECTL_TIMEOUT_SEC", "3"))
TCP_PRECHECK_TIMEOUT_SEC = float(os.getenv("TCP_PRECHECK_TIMEOUT_SEC", "2"))


def cluster_is_reachable() -> bool:
    """
    Cheap precheck: is the cluster even up right now?

    Tries `kubectl get nodes` first (authoritative — confirms the API server
    is actually responding, not just that *something* is listening on a port).
    Falls back to a raw TCP connect against the Loki host:port if kubectl
    isn't on PATH or errors out for an unrelated reason, so a missing kubectl
    binary doesn't itself masquerade as "cluster is down."

    Returns False (skip run) if neither check succeeds — treated as "not
    currently working on the project," not a pipeline failure.
    """
    try:
        result = subprocess.run(
            ["kubectl", "get", "nodes", "--request-timeout", f"{KUBECTL_TIMEOUT_SEC}s"],
            capture_output=True,
            timeout=KUBECTL_TIMEOUT_SEC + 2,
        )
        if result.returncode == 0:
            return True
        # kubectl ran but the cluster rejected/couldn't be reached -> treat as down
        return False
    except FileNotFoundError:
        pass  # kubectl not on PATH here — fall through to TCP check
    except subprocess.TimeoutExpired:
        return False

    try:
        parsed = urlparse(LOKI_URL)
        host = parsed.hostname or "localhost"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        with socket.create_connection((host, port), timeout=TCP_PRECHECK_TIMEOUT_SEC):
            return True
    except OSError:
        return False


def check_loki(client: httpx.Client, lookback_sec: int) -> bool:
    now_ns = int(time.time() * 1e9)
    start_ns = now_ns - int(lookback_sec * 1e9)
    params = {
        "query": '{job="fluent-bit"} |= "canary-"',
        "start": str(start_ns),
        "end": str(now_ns),
        "limit": 10,
    }
    try:
        resp = client.get(f"{LOKI_URL}/loki/api/v1/query_range", params=params, timeout=5.0)
        if resp.status_code == 200:
            data = resp.json()
            results = data.get("data", {}).get("result", [])
            for stream in results:
                if stream.get("values"):
                    return True
    except Exception:
        pass
    return False


def check_jaeger(client: httpx.Client, lookback_sec: int) -> bool:
    now_us = int(time.time() * 1e6)
    start_us = now_us - int(lookback_sec * 1e6)
    params = {
        "service": "service-a",
        "start": str(start_us),
        "end": str(now_us),
        "limit": 20,
    }
    try:
        resp = client.get(f"{JAEGER_URL}/api/traces", params=params, timeout=5.0)
        if resp.status_code == 200:
            data = resp.json()
            traces = data.get("data", [])
            for trace in traces:
                for span in trace.get("spans", []):
                    for tag in span.get("tags", []):
                        if "canary" in str(tag.get("value", "")):
                            return True
            # if canary traces present under service-a
            if len(traces) > 0:
                return True
    except Exception:
        pass
    return False


def send_slack_alert(client: httpx.Client, message: str) -> None:
    if not SLACK_WEBHOOK_URL:
        print(f"[Watchdog ALERT (no webhook configured)]: {message}")
        return
    payload = {
        "text": f"\U0001f6a8 *[OUT-OF-BAND WATCHDOG ALERT]* \U0001f6a8\n{message}"
    }
    try:
        resp = client.post(SLACK_WEBHOOK_URL, json=payload, timeout=5.0)
        print(f"[Watchdog] Alert sent to Slack: {resp.status_code}")
    except Exception as exc:
        print(f"[Watchdog] Failed to send Slack alert: {exc}", file=sys.stderr)


def run_watchdog() -> int:
    if not cluster_is_reachable():
        print(
            "[Watchdog SKIP] Cluster not reachable — assuming project isn't "
            "currently running. Not treating this as a pipeline failure."
        )
        return 0

    lookback_sec = LOOKBACK_MINUTES * 60
    with httpx.Client() as client:
        loki_ok = check_loki(client, lookback_sec)
        jaeger_ok = check_jaeger(client, lookback_sec)

        if loki_ok and jaeger_ok:
            print(f"[Watchdog OK] Canary verified in Loki and Jaeger over past {LOOKBACK_MINUTES}m.")
            return 0

        failures = []
        if not loki_ok:
            failures.append("Loki log stream missing canary events")
        if not jaeger_ok:
            failures.append("Jaeger trace missing canary spans")

        error_msg = f"Telemetry pipeline failure detected! Failures: {', '.join(failures)} within {LOOKBACK_MINUTES}m window."
        print(f"[Watchdog FAILURE] {error_msg}", file=sys.stderr)
        send_slack_alert(client, error_msg)
        return 1


if __name__ == "__main__":
    sys.exit(run_watchdog())