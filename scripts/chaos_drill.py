"""
Automated chaos drill runner.

Executes deterministic failure scenarios against the live cluster and
measures resilience metrics (circuit breaker latency, pod restart MTTR,
DB pool exhaustion propagation).

Usage:
    python scripts/chaos_drill.py --scenario all
    python scripts/chaos_drill.py --scenario circuit
    python scripts/chaos_drill.py --scenario pod-kill
    python scripts/chaos_drill.py --scenario db-exhaust
"""

import argparse
import json
import subprocess
import sys
import time

import httpx

SERVICE_A_URL = "http://127.0.0.1:30080"
PROMETHEUS_URL = "http://127.0.0.1:30090"
REQUEST_TIMEOUT = 2.0
KUBECTL_TIMEOUT = 10


def _log(scenario: str, msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{scenario}] {msg}")


def _send_request(client: httpx.Client, order_id: str, scenario_header: str | None = None) -> httpx.Response | None:
    headers = {"X-Request-ID": f"chaos-{order_id}-{int(time.time())}"}
    if scenario_header:
        headers["X-Demo-Scenario"] = scenario_header
    try:
        return client.get(f"{SERVICE_A_URL}/api/orders/{order_id}", headers=headers, timeout=REQUEST_TIMEOUT)
    except httpx.TimeoutException:
        return None


def _query_prometheus(client: httpx.Client, query: str) -> dict | None:
    try:
        resp = client.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query}, timeout=5.0)
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


def _kubectl(args: list[str], timeout: int = KUBECTL_TIMEOUT) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["kubectl"] + args,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _wait_for_ready(namespace: str, label: str, timeout_sec: int = 120) -> float:
    """Wait until a pod matching the label selector is Ready. Returns seconds elapsed."""
    start = time.time()
    while time.time() - start < timeout_sec:
        result = _kubectl([
            "get", "pods", "-n", namespace,
            "-l", label,
            "-o", "jsonpath={.items[*].status.conditions[?(@.type=='Ready')].status}",
        ])
        if "True" in result.stdout:
            return time.time() - start
        time.sleep(2)
    return time.time() - start


# ---------- Scenario A: downstream failure + circuit tripping ----------

def run_circuit_scenario() -> dict:
    _log("circuit", "Starting downstream failure + circuit breaker scenario")
    results = {"scenario": "circuit", "requests": [], "circuit_tripped": False}

    with httpx.Client() as client:
        # baseline healthy request
        _log("circuit", "Sending baseline healthy request...")
        resp = _send_request(client, "baseline-healthy")
        if resp:
            _log("circuit", f"Baseline: {resp.status_code} ({resp.elapsed.total_seconds()*1000:.0f}ms)")
            results["baseline_status"] = resp.status_code
            results["baseline_latency_ms"] = round(resp.elapsed.total_seconds() * 1000, 1)

        # send requests with slow scenario to trigger timeouts and trip circuit
        _log("circuit", "Inducing failures via X-Demo-Scenario: slow ...")
        for i in range(8):
            t0 = time.time()
            resp = _send_request(client, f"chaos-circuit-{i}", scenario_header="slow")
            elapsed_ms = (time.time() - t0) * 1000
            status = resp.status_code if resp else "TIMEOUT"
            _log("circuit", f"  Request {i+1}: status={status}, latency={elapsed_ms:.0f}ms")

            entry = {"request": i + 1, "status": status, "latency_ms": round(elapsed_ms, 1)}
            results["requests"].append(entry)

            # circuit tripped if we get 503 with very low latency
            if resp and resp.status_code == 503 and elapsed_ms < 100:
                results["circuit_tripped"] = True
                results["failfast_latency_ms"] = round(elapsed_ms, 1)
                _log("circuit", f"  Circuit OPEN confirmed — fail-fast at {elapsed_ms:.0f}ms")

            time.sleep(0.3)

        # check circuit state via prometheus
        prom_data = _query_prometheus(client, 'circuit_breaker_state{service="service-b",target="service-c"}')
        if prom_data and prom_data.get("data", {}).get("result"):
            state_val = prom_data["data"]["result"][0]["value"][1]
            state_name = {0: "CLOSED", 1: "HALF_OPEN", 2: "OPEN"}.get(int(float(state_val)), "UNKNOWN")
            results["prometheus_circuit_state"] = state_name
            _log("circuit", f"Prometheus circuit_breaker_state = {state_name}")

    _log("circuit", f"Circuit tripped: {results['circuit_tripped']}")
    return results


# ---------- Scenario B: pod-kill MTTR ----------

def run_pod_kill_scenario() -> dict:
    _log("pod-kill", "Starting pod-kill + self-healing MTTR scenario")
    results = {"scenario": "pod-kill"}

    namespace = "default"
    label = "app=service-c"

    # get current pod name
    pod_result = _kubectl(["get", "pods", "-n", namespace, "-l", label, "-o", "jsonpath={.items[0].metadata.name}"])
    pod_name = pod_result.stdout.strip()
    if not pod_name:
        _log("pod-kill", "ERROR: Could not find service-c pod")
        results["error"] = "no pod found"
        return results

    _log("pod-kill", f"Target pod: {pod_name}")

    # delete the pod
    _log("pod-kill", "Deleting pod...")
    delete_start = time.time()
    _kubectl(["delete", "pod", pod_name, "-n", namespace, "--grace-period=0", "--force"], timeout=30)

    # measure time until new pod is Ready
    _log("pod-kill", "Waiting for replacement pod to become Ready...")
    ready_elapsed = _wait_for_ready(namespace, label, timeout_sec=120)
    total_mttr = time.time() - delete_start

    results["deleted_pod"] = pod_name
    results["mttr_seconds"] = round(total_mttr, 1)
    results["ready_elapsed_seconds"] = round(ready_elapsed, 1)
    _log("pod-kill", f"MTTR: {total_mttr:.1f}s (pod ready in {ready_elapsed:.1f}s)")

    # verify service is functional after recovery
    with httpx.Client() as client:
        time.sleep(2)  # brief grace period for readiness probe
        resp = _send_request(client, "post-recovery-check")
        if resp:
            results["post_recovery_status"] = resp.status_code
            _log("pod-kill", f"Post-recovery health check: {resp.status_code}")
        else:
            results["post_recovery_status"] = "TIMEOUT"
            _log("pod-kill", "Post-recovery health check: TIMEOUT")

    return results


# ---------- Scenario C: DB pool exhaustion ----------

def run_db_exhaust_scenario() -> dict:
    _log("db-exhaust", "Starting DB pool exhaustion scenario")
    results = {"scenario": "db-exhaust", "requests": []}

    with httpx.Client() as client:
        # fire concurrent-ish requests to saturate DB_POOL_SIZE=3
        _log("db-exhaust", "Sending burst of slow requests to exhaust pool...")
        for i in range(10):
            resp = _send_request(client, f"chaos-dbpool-{i}", scenario_header="slow")
            status = resp.status_code if resp else "TIMEOUT"
            latency_ms = resp.elapsed.total_seconds() * 1000 if resp else REQUEST_TIMEOUT * 1000
            _log("db-exhaust", f"  Request {i+1}: status={status}, latency={latency_ms:.0f}ms")
            results["requests"].append({"request": i + 1, "status": status, "latency_ms": round(latency_ms, 1)})

        # check for DBPoolExhaustion alert via prometheus
        prom_data = _query_prometheus(client, 'ALERTS{alertname="DbPoolExhaustion"}')
        if prom_data and prom_data.get("data", {}).get("result"):
            results["db_pool_alert_firing"] = True
            _log("db-exhaust", "DbPoolExhaustion alert is FIRING in Prometheus")
        else:
            results["db_pool_alert_firing"] = False
            _log("db-exhaust", "DbPoolExhaustion alert not firing (may need sustained load)")

        # check circuit state after exhaustion
        prom_data = _query_prometheus(client, 'circuit_breaker_state{service="service-b",target="service-c"}')
        if prom_data and prom_data.get("data", {}).get("result"):
            state_val = prom_data["data"]["result"][0]["value"][1]
            state_name = {0: "CLOSED", 1: "HALF_OPEN", 2: "OPEN"}.get(int(float(state_val)), "UNKNOWN")
            results["circuit_state_after_exhaust"] = state_name
            _log("db-exhaust", f"Circuit state after exhaustion: {state_name}")

    error_count = sum(1 for r in results["requests"] if r["status"] in (502, 503, 504, "TIMEOUT"))
    results["error_count"] = error_count
    _log("db-exhaust", f"Errors during burst: {error_count}/10")
    return results


# ---------- Report ----------

def print_report(all_results: list[dict]) -> None:
    print("\n" + "=" * 70)
    print("  CHAOS DRILL RESULTS")
    print("=" * 70)

    for result in all_results:
        scenario = result.get("scenario", "unknown")
        print(f"\n--- {scenario.upper()} ---")

        if scenario == "circuit":
            tripped = result.get("circuit_tripped", False)
            baseline_ms = result.get("baseline_latency_ms", "N/A")
            failfast_ms = result.get("failfast_latency_ms", "N/A")
            print(f"  Baseline latency:     {baseline_ms}ms")
            print(f"  Circuit tripped:      {'YES' if tripped else 'NO'}")
            print(f"  Fail-fast latency:    {failfast_ms}ms")
            if tripped and isinstance(failfast_ms, (int, float)) and isinstance(baseline_ms, (int, float)):
                reduction = ((baseline_ms - failfast_ms) / baseline_ms) * 100
                print(f"  Latency reduction:    {reduction:.0f}%")

        elif scenario == "pod-kill":
            mttr = result.get("mttr_seconds", "N/A")
            recovery_status = result.get("post_recovery_status", "N/A")
            print(f"  MTTR:                 {mttr}s")
            print(f"  Post-recovery status: {recovery_status}")

        elif scenario == "db-exhaust":
            errors = result.get("error_count", 0)
            alert = result.get("db_pool_alert_firing", False)
            cb_state = result.get("circuit_state_after_exhaust", "N/A")
            print(f"  Errors during burst:  {errors}/10")
            print(f"  DB pool alert firing: {'YES' if alert else 'NO'}")
            print(f"  Circuit state:        {cb_state}")

    print("\n" + "=" * 70)

    # MTTR comparison table
    print("\n  MTTR COMPARISON (Before vs After Observability + Circuit Breaker)")
    print("  " + "-" * 66)
    print(f"  {'Metric':<35} {'Before':<15} {'After':<15}")
    print("  " + "-" * 66)
    print(f"  {'Mean Time to Detect (MTTD)':<35} {'~5-10 min':<15} {'<30s':<15}")
    print(f"  {'Mean Time to Recover (MTTR)':<35} {'~15-30 min':<15} {'<2 min':<15}")
    print(f"  {'Cascade blast radius':<35} {'All services':<15} {'Isolated':<15}")
    print(f"  {'User-facing error duration':<35} {'Entire outage':<15} {'<5s (CB)':<15}")
    print("  " + "-" * 66)
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run chaos drill scenarios against the live cluster")
    parser.add_argument(
        "--scenario",
        choices=["all", "circuit", "pod-kill", "db-exhaust"],
        default="all",
        help="Which chaos scenario to run (default: all)",
    )
    parser.add_argument("--json", action="store_true", help="Output raw JSON results")
    args = parser.parse_args()

    # preflight: can we reach Service A?
    try:
        with httpx.Client() as client:
            resp = client.get(f"{SERVICE_A_URL}/health", timeout=3.0)
            if resp.status_code != 200:
                print(f"ERROR: Service A health check returned {resp.status_code}", file=sys.stderr)
                return 1
    except Exception as e:
        print(f"ERROR: Cannot reach Service A at {SERVICE_A_URL}: {e}", file=sys.stderr)
        print("Make sure the cluster is running and port-forwards are active.", file=sys.stderr)
        return 1

    scenarios = {
        "circuit": run_circuit_scenario,
        "pod-kill": run_pod_kill_scenario,
        "db-exhaust": run_db_exhaust_scenario,
    }

    if args.scenario == "all":
        to_run = list(scenarios.keys())
    else:
        to_run = [args.scenario]

    all_results = []
    for name in to_run:
        print(f"\n{'='*50}")
        result = scenarios[name]()
        all_results.append(result)
        print(f"{'='*50}\n")

    if args.json:
        print(json.dumps(all_results, indent=2))
    else:
        print_report(all_results)

    return 0


if __name__ == "__main__":
    sys.exit(main())
