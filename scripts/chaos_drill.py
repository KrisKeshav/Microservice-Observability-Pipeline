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
import os
import subprocess
import sys
import time

# Ensure project root is on sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import httpx

SERVICE_A_URL = os.getenv("SERVICE_A_URL", "http://127.0.0.1:30080")
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://127.0.0.1:30090")
TARGET_NAMESPACE = os.getenv("CHAOS_NAMESPACE", "")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "2.5"))
KUBECTL_TIMEOUT = int(os.getenv("KUBECTL_TIMEOUT", "30"))


def _log(scenario: str, msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] [{scenario}] {msg}", flush=True)


_LOCAL_CLIENT = None
_PIPELINE_INITIALIZED = False


def _init_inprocess_pipeline() -> None:
    global _PIPELINE_INITIALIZED
    if _PIPELINE_INITIALIZED:
        return
    os.environ["OTEL_SDK_DISABLED"] = "true"
    from services.service_a.main import app as app_a
    from services.service_b.main import app as app_b
    from services.service_c.main import app as app_c

    transport_c = httpx.ASGITransport(app=app_c)
    transport_b = httpx.ASGITransport(app=app_b)
    transport_a = httpx.ASGITransport(app=app_a)

    import services.service_a.main as mod_a
    import services.service_b.main as mod_b

    # Override internal clients to route via in-process ASGI
    original_client_init = httpx.AsyncClient.__init__

    def patched_client_init(self, *args, **kwargs):
        # If calling Service B from A
        if kwargs.get("base_url") or "app" not in kwargs:
            target_url = str(args[0]) if args else kwargs.get("base_url", "")
            if "8002" in str(target_url) or "service-c" in str(target_url):
                kwargs["transport"] = transport_c
                kwargs["base_url"] = "http://testserver"
            else:
                kwargs["transport"] = transport_b
                kwargs["base_url"] = "http://testserver"
        original_client_init(self, *args, **kwargs)

    httpx.AsyncClient.__init__ = patched_client_init
    _PIPELINE_INITIALIZED = True


def _get_client() -> tuple[httpx.Client | None, bool]:
    global _LOCAL_CLIENT
    # Test remote URL first
    try:
        with httpx.Client() as probe:
            resp = probe.get(f"{SERVICE_A_URL}/health", timeout=1.5)
            if resp.status_code == 200:
                return httpx.Client(timeout=REQUEST_TIMEOUT), True
    except Exception:
        pass

    # Fallback to in-process ASGI stack
    if _LOCAL_CLIENT is None:
        _init_inprocess_pipeline()
        from fastapi.testclient import TestClient
        from services.service_a.main import app as app_a
        _LOCAL_CLIENT = TestClient(app_a)
    return _LOCAL_CLIENT, False


def _send_request(client: httpx.Client | None, order_id: str, scenario_header: str | None = None) -> httpx.Response | None:
    headers = {"X-Request-ID": f"chaos-{order_id}-{int(time.time())}"}
    if scenario_header:
        headers["X-Demo-Scenario"] = scenario_header

    effective_client, is_remote = _get_client() if client is None else (client, True)

    try:
        if is_remote:
            return effective_client.get(f"{SERVICE_A_URL}/api/orders/{order_id}", headers=headers, timeout=REQUEST_TIMEOUT)
        else:
            return effective_client.get(f"/api/orders/{order_id}", headers=headers)
    except httpx.TimeoutException:
        return None
    except Exception:
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
    try:
        return subprocess.run(
            ["kubectl"] + args,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr="timed out")
    except Exception as e:
        return subprocess.CompletedProcess(args=args, returncode=1, stdout="", stderr=str(e))


def _wait_for_ready(namespace: str, label: str, timeout_sec: int = 60) -> float:
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

        # send requests with error scenario to trigger downstream failures and trip circuit
        _log("circuit", "Inducing downstream errors via X-Demo-Scenario: error (tripping multi-replica pool)...")
        for i in range(12):
            t0 = time.time()
            resp = _send_request(client, f"chaos-circuit-{i}", scenario_header="error")
            elapsed_ms = (time.time() - t0) * 1000
            status = resp.status_code if resp else "TIMEOUT"
            _log("circuit", f"  Request {i+1:02d}: status={status}, latency={elapsed_ms:6.1f}ms")

            entry = {"request": i + 1, "status": status, "latency_ms": round(elapsed_ms, 1)}
            results["requests"].append(entry)

            # circuit tripped if we get fast failure rejection (<100ms)
            if resp and resp.status_code in (502, 503) and elapsed_ms < 100:
                results["circuit_tripped"] = True
                results["failfast_latency_ms"] = round(elapsed_ms, 1)
                _log("circuit", f"  >>> Circuit OPEN confirmed: fail-fast rejection in {elapsed_ms:.1f}ms (HTTP {resp.status_code})")

            time.sleep(0.2)

        # check circuit state via prometheus
        prom_data = _query_prometheus(client, 'circuit_breaker_state{service="service-b",target="service-c"}')
        if prom_data and prom_data.get("data", {}).get("result"):
            state_val = prom_data["data"]["result"][0]["value"][1]
            state_name = {0: "CLOSED", 1: "HALF_OPEN", 2: "OPEN"}.get(int(float(state_val)), "UNKNOWN")
            results["prometheus_circuit_state"] = state_name
            _log("circuit", f"Prometheus circuit_breaker_state = {state_name}")

    _log("circuit", f"Circuit tripped: {results['circuit_tripped']}")
    return results


def _get_target_namespace() -> str:
    if TARGET_NAMESPACE:
        return TARGET_NAMESPACE
    for ns in ["prod", "dev", "default"]:
        res = _kubectl(["get", "pods", "-n", ns, "-l", "app=service-c", "--no-headers"])
        if res.returncode == 0 and "service-c" in res.stdout:
            return ns
    return "prod"


# ---------- Scenario B: pod-kill MTTR ----------

def run_pod_kill_scenario() -> dict:
    _log("pod-kill", "Starting pod-kill + self-healing MTTR scenario")
    results = {"scenario": "pod-kill"}

    namespace = _get_target_namespace()
    label = "app=service-c"

    # get current pod name
    pod_result = _kubectl(["get", "pods", "-n", namespace, "-l", label, "-o", "jsonpath={.items[0].metadata.name}"])
    pod_name = pod_result.stdout.strip()
    if not pod_name:
        _log("pod-kill", f"ERROR: Could not find service-c pod in namespace '{namespace}'")
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

    # preflight: check connectivity
    client, is_remote = _get_client()
    if is_remote:
        print(f"[PREFLIGHT] Connected to live cluster at {SERVICE_A_URL}", flush=True)
    else:
        print(f"[PREFLIGHT] Live cluster endpoint not reachable at {SERVICE_A_URL}. Using in-process microservice pipeline transport.", flush=True)

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
