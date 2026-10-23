"""Chaos drills against the live dev environment. Every number printed is measured.

Scenarios:
    circuit       Force Service C errors; the circuit breaker in Service B should open and
                  fail fast (503 + Retry-After), then close again once C is healthy.
    pod-kill      Delete a Service C pod; measure how long until Kubernetes replaces it and
                  how many user requests failed in the meantime.
    db-exhaust    Hold DB connections with slow queries; watch DbPoolExhaustion and the breaker.
    log-buffer    Stop the Loki consumer longer than the alert window; TelemetryServiceDown must
                  fire, Kafka must buffer the logs, and they must reach Loki after recovery.

Usage:
    python scripts/chaos_drill.py --scenario all
    python scripts/chaos_drill.py --scenario circuit --json
"""

import argparse
import json
import os
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import httpx

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from scripts.canary_watchdog import find_logs_in_loki  # noqa: E402

SERVICE_A_URL = os.getenv("SERVICE_A_URL", "http://127.0.0.1:30080")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://127.0.0.1:30090")
ALERTMANAGER_URL = os.getenv("ALERTMANAGER_URL", "http://127.0.0.1:30093")
NAMESPACE = os.getenv("CHAOS_NAMESPACE", "dev")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "8"))
OUTAGE_SECONDS = int(os.getenv("LOG_OUTAGE_SECONDS", "150"))
TRIPS_QUERY = 'sum(increase(circuit_breaker_trips_total{{service="service-b"}}[{window}]))'


def _log(scenario: str, msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] [{scenario}] {msg}", flush=True)


# ---------- helpers ----------

def send_order_request(client: httpx.Client, scenario: str | None = None, request_id: str | None = None):
    """GET an order through Service A. Returns (status or 'TIMEOUT'/'ERROR', latency_ms, response)."""
    request_id = request_id or f"chaos-{uuid.uuid4().hex[:10]}"
    headers = {"X-Request-ID": request_id}
    if scenario:
        headers["X-Demo-Scenario"] = scenario
    start = time.perf_counter()
    try:
        resp = client.get(f"{SERVICE_A_URL}/api/orders/{request_id}", headers=headers, timeout=REQUEST_TIMEOUT)
    except httpx.TimeoutException:
        return "TIMEOUT", (time.perf_counter() - start) * 1000, None
    except httpx.HTTPError:
        return "ERROR", (time.perf_counter() - start) * 1000, None
    return resp.status_code, (time.perf_counter() - start) * 1000, resp


def is_circuit_rejection(resp: httpx.Response | None) -> bool:
    """The breaker's fast rejection is a 503 that carries Retry-After; other errors do not."""
    return resp is not None and resp.status_code == 503 and "Retry-After" in resp.headers


def query_prometheus(client: httpx.Client, query: str) -> float | None:
    """Return the first value of an instant query, or None if there is no result."""
    try:
        resp = client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=5)
        resp.raise_for_status()
        result = resp.json()["data"]["result"]
        return float(result[0]["value"][1]) if result else None
    except (httpx.HTTPError, KeyError, IndexError, ValueError):
        return None


def alertmanager_has_firing_alert(client: httpx.Client, alert_name: str) -> bool:
    try:
        resp = client.get(f"{ALERTMANAGER_URL}/api/v2/alerts", timeout=5)
        resp.raise_for_status()
        return any(
            a.get("labels", {}).get("alertname") == alert_name and a.get("status", {}).get("state") == "active"
            for a in resp.json()
        )
    except (httpx.HTTPError, ValueError):
        return False


def kubectl(*args: str, timeout: int = 60) -> subprocess.CompletedProcess:
    return subprocess.run(["kubectl", "-n", NAMESPACE, *args], capture_output=True, text=True, timeout=timeout)


# ---------- scenarios ----------

def run_circuit(client: httpx.Client) -> dict:
    _log("circuit", "Forcing Service C errors (X-Demo-Scenario: error)...")
    attempts = []
    for _ in range(10):
        status, ms, resp = send_order_request(client, scenario="error")
        attempts.append({"status": status, "latency_ms": round(ms, 1), "circuit_rejection": is_circuit_rejection(resp)})
        time.sleep(0.2)

    failed = [a["latency_ms"] for a in attempts if not a["circuit_rejection"]]
    rejected = [a["latency_ms"] for a in attempts if a["circuit_rejection"]]
    result = {
        "scenario": "circuit",
        "attempts": attempts,
        "circuit_opened": bool(rejected),
        "requests_before_open": len(failed),
        "avg_failure_latency_ms": round(sum(failed) / len(failed), 1) if failed else None,
        "avg_rejection_latency_ms": round(sum(rejected) / len(rejected), 1) if rejected else None,
    }

    _log("circuit", "Waiting for the recovery timeout, then sending a healthy request (half-open probe)...")
    time.sleep(6)
    status, _, _ = send_order_request(client)
    result["recovered_status"] = status
    # A 5 s open state can fall between 15 s scrapes, so check the trip counter, not the state gauge.
    time.sleep(20)
    result["trips_seen_by_prometheus"] = query_prometheus(client, TRIPS_QUERY.format(window="2m"))
    return result


def run_pod_kill(client: httpx.Client) -> dict:
    pods = kubectl("get", "pods", "-l", "app=service-c", "-o", "jsonpath={.items[*].metadata.name}").stdout.split()
    if not pods:
        return {"scenario": "pod-kill", "error": f"no service-c pod in namespace {NAMESPACE}"}
    victim = pods[0]
    _log("pod-kill", f"Deleting {victim} ({len(pods)} replica(s) running)...")

    start = time.time()
    kubectl("delete", "pod", victim, "--wait=false")
    statuses = []
    replacement_ready_s = None
    # Keep sending traffic until a NEW pod (not one of the survivors) is Ready.
    while replacement_ready_s is None and time.time() - start < 180:
        status, _, _ = send_order_request(client)
        statuses.append(status)
        ready = kubectl("get", "pods", "-l", "app=service-c", "-o",
                        "jsonpath={range .items[*]}{.metadata.name} {.status.conditions[?(@.type=='Ready')].status}{'\\n'}{end}").stdout
        new_ready = [line.split()[0] for line in ready.splitlines()
                     if line.strip() and line.split()[0] not in pods and line.endswith("True")]
        if new_ready:
            replacement_ready_s = round(time.time() - start, 1)
        time.sleep(0.5)

    return {
        "scenario": "pod-kill",
        "deleted_pod": victim,
        "replicas_before": len(pods),
        "replacement_ready_seconds": replacement_ready_s,
        "failed_user_requests": sum(1 for s in statuses if s != 200),
        "total_user_requests": len(statuses),
    }


def run_db_exhaust(client: httpx.Client, duration: int = 75, concurrency: int = 10) -> dict:
    _log("db-exhaust", f"{concurrency} concurrent slow requests for {duration}s against a pool of 3...")
    statuses: dict[str, int] = {}
    alert_fired_after = None
    start = time.time()

    def worker():
        while time.time() - start < duration:
            status, _, _ = send_order_request(client, scenario="slow")
            statuses[str(status)] = statuses.get(str(status), 0) + 1

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        for _ in range(concurrency):
            pool.submit(worker)
        while time.time() - start < duration:
            if alert_fired_after is None and alertmanager_has_firing_alert(client, "DbPoolExhaustion"):
                alert_fired_after = round(time.time() - start, 1)
            time.sleep(5)

    time.sleep(20)  # let Prometheus scrape the final counter values
    return {
        "scenario": "db-exhaust",
        "status_counts": statuses,
        "db_pool_alert_fired_after_seconds": alert_fired_after,
        "breaker_trips": query_prometheus(client, TRIPS_QUERY.format(window=f"{duration + 30}s")),
    }


def run_log_buffer(client: httpx.Client) -> dict:
    marker = f"chaos-buffered-{uuid.uuid4().hex[:8]}"
    lag_query = 'sum(kafka_consumergroup_lag{consumergroup="fluent-bit-loki-consumer"})'
    _log("log-buffer", f"Stopping fluent-bit-consumer for {OUTAGE_SECONDS}s...")
    kubectl("scale", "deployment/fluent-bit-consumer", "--replicas=0")
    start = time.time()

    send_order_request(client, request_id=marker)
    alert_fired_after = None
    peak_lag = 0.0
    while time.time() - start < OUTAGE_SECONDS:
        send_order_request(client)
        peak_lag = max(peak_lag, query_prometheus(client, lag_query) or 0)
        if alert_fired_after is None and alertmanager_has_firing_alert(client, "TelemetryServiceDown"):
            alert_fired_after = round(time.time() - start, 1)
        time.sleep(5)

    _log("log-buffer", "Restoring fluent-bit-consumer and waiting for the buffered logs to reach Loki...")
    kubectl("scale", "deployment/fluent-bit-consumer", "--replicas=1")
    restored = time.time()
    delivered_after = None
    while time.time() - restored < 180:
        if find_logs_in_loki(client, marker):
            delivered_after = round(time.time() - restored, 1)
            break
        time.sleep(3)

    return {
        "scenario": "log-buffer",
        "telemetry_alert_fired_after_seconds": alert_fired_after,
        "peak_consumer_lag": peak_lag,
        "buffered_log_delivered": delivered_after is not None,
        "delivered_seconds_after_restore": delivered_after,
    }


# ---------- report ----------

def print_report(results: list[dict]) -> None:
    print("\n" + "=" * 60 + "\n  CHAOS DRILL RESULTS (namespace: " + NAMESPACE + ")\n" + "=" * 60)
    for r in results:
        print(f"\n--- {r['scenario'].upper()} ---")
        if r.get("error"):
            print(f"  ERROR: {r['error']}")
            continue
        for key, value in r.items():
            if key not in ("scenario", "attempts"):
                print(f"  {key.replace('_', ' '):<38} {value}")
    print()


SCENARIOS = {"circuit": run_circuit, "pod-kill": run_pod_kill, "db-exhaust": run_db_exhaust, "log-buffer": run_log_buffer}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", choices=["all", *SCENARIOS], default="all")
    parser.add_argument("--json", action="store_true", help="print raw JSON results")
    args = parser.parse_args()

    if NAMESPACE == "prod":
        print("Refusing to run chaos drills against prod (fault injection is disabled there anyway).")
        return 2

    with httpx.Client() as client:
        try:
            client.get(f"{SERVICE_A_URL}/health", timeout=3).raise_for_status()
        except httpx.HTTPError as exc:
            print(f"Service A is not reachable at {SERVICE_A_URL} ({exc}). Is the dev environment running?")
            return 2

        names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
        results = []
        for name in names:
            results.append(SCENARIOS[name](client))
            time.sleep(10)  # let the system settle between scenarios

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print_report(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
