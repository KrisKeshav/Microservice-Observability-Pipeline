# Incident Response Runbook: On-Call Triage Guide

This runbook is for active incidents in the Microservice Observability Pipeline. It covers the four primary alerts, step-by-step triage workflows, and exact queries/dashboards to use during an incident.

The existing [Failure Trace Runbook](FAILURE_TRACE_RUNBOOK.md) covers *how to trace a specific failure request through the pipeline*. This document covers *how to respond when an alert fires*.

---

## Alert Quick Reference

| Alert | Severity | Fires When | Expected Action |
|---|---|---|---|
| `HighErrorRate` | critical | 5xx rate > 5% for 30s on any service | Identify failing service, check downstream dependencies |
| `DbPoolExhaustion` | critical | `db_pool_active >= db_pool_max` for 10s | Reduce load or scale Service C, check for connection leaks |
| `KafkaConsumerLagHigh` | warning | Consumer lag > 50 messages for 1m | Check consumer health, restart if stuck |
| `TelemetryServiceDown` | critical | Prometheus cannot scrape Loki/Jaeger/Fluent Bit/Kafka Exporter for 1m | Check pod status, restart if CrashLooping |
| `SLOFastBurn` | critical | Service A availability or latency budget burns >14.4x across 1h and 5m windows | Page; stop the budget burn and investigate immediately |
| `SLOSlowBurn` | warning | Service A availability or latency budget burns >6x across 6h and 30m windows | Investigate before the budget is exhausted |

---

## Incident 1: `HighErrorRate` — 5xx Error Spike

### When You Get Paged

An HTTP 5xx error rate above 5% is sustained on one of the services. Alertmanager fires to Slack.

### Triage Steps

1. **Open Alertmanager** at `http://localhost:30093` — confirm which service label is firing.

2. **Open Grafana** at `http://localhost:30300` → Dashboard **"Microservice Observability Pipeline"**:
   - Check the **Error Rate** panel for the affected service.
   - Note whether error rate is climbing, steady, or recovering.

3. **Identify the root service** — errors cascade upstream, so check from the bottom up:
   ```
   Service A (502/504) ← Service B (502/504) ← Service C (503/500) ← PostgreSQL
   ```
   If Service C is the origin, the 502/504 on A and B are symptoms, not root causes.

4. **Query Loki for error logs** (Grafana → Explore → Loki):
   ```logql
   {job="fluent-bit"} | json | levelname="ERROR" | service="<affected-service>"
   ```
   Look for the `event` field: `db_pool_exhausted`, `service_c_timeout`, `service_c_error`, etc.

5. **Pull a specific trace** from the error logs:
   - Copy the `trace_id` from any error log line.
   - Open Jaeger at `http://localhost:31686` and search by that Trace ID.
   - The span waterfall shows exactly which service/operation failed and how long it took.

6. **Check Circuit Breaker state**:
   ```promql
   circuit_breaker_state{service="service-b", target="service-c"}
   ```
   - `0` = CLOSED (normal), `1` = HALF_OPEN (recovering), `2` = OPEN (rejecting calls).
   - If OPEN, Service B is already protecting itself. The root cause is in Service C or below.

### Remediation

| Root Cause | Fix |
|---|---|
| Service C pod crash | Check `kubectl logs` and pod events. Restart if OOMKilled. |
| DB pool exhaustion | See Incident 2 below. |
| Service C timeout | Check PostgreSQL query latency, slow queries. |
| Network partition | Check `kubectl get endpoints service-c` for stale endpoint lists. |

---

## Incident 2: `DbPoolExhaustion` — Connection Pool Saturated

### When You Get Paged

Service C's `db_pool_active` gauge equals `db_pool_max` (3 by default). All incoming requests queue for a connection, causing cascading timeouts.

### Triage Steps

1. **Confirm in Grafana** → Dashboard → **DB Pool** panels:
   - `db_pool_active` should be at or near `db_pool_max`.
   - Check whether the spike is sustained or transient.

2. **Check upstream traffic**:
   ```promql
   rate(http_requests_total{handler="/internal/orders"}[1m])
   ```
   A sudden spike in request rate from Service B is the usual trigger.

3. **Inspect Service C logs** for connection wait patterns:
   ```logql
   {job="fluent-bit"} | json | service="service-c" | event="db_pool_exhausted"
   ```

4. **Check for leaked connections** — look for requests that started a DB operation but never returned:
   ```logql
   {job="fluent-bit"} | json | service="service-c" | event="db_query_start"
   ```
   Compare with `db_query_complete` count. A divergence means connections are stuck.

### Remediation

| Approach | Command / Action |
|---|---|
| Reduce upstream load | Scale down load test or rate-limit at Service A |
| Scale Service C horizontally | `kubectl scale deployment service-c --replicas=3` (each replica gets its own pool) |
| Increase pool size | Set `DB_POOL_SIZE` env var (tradeoff: more PostgreSQL connections) |
| Restart stuck pods | `kubectl rollout restart deployment service-c` |

### Note on DB_POOL_SIZE=3

This is intentionally small to demonstrate pool exhaustion under load. In production you'd size this based on `max_connections` in PostgreSQL divided by expected replica count.

---

## Incident 3: Circuit Breaker Tripped (`circuit_breaker_state = OPEN`)

### When You Notice

Service B starts returning `503 Service Unavailable` with `Retry-After` headers. The `circuit_breaker_trips_total` counter increments.

This isn't a separate Alertmanager alert (it's a resilience mechanism, not a failure) — but it shows up in the `HighErrorRate` alert because 503s contribute to the error rate.

### Triage Steps

1. **Confirm circuit state**:
   ```promql
   circuit_breaker_state{service="service-b", target="service-c"}
   ```

2. **Check rejection volume**:
   ```promql
   rate(circuit_breaker_rejections_total[1m])
   ```
   High rejection rate means traffic is still hitting Service B but being shed without touching Service C.

3. **The root cause is downstream** — investigate Service C:
   - Is the pod running? `kubectl get pods -l app=service-c`
   - Is it passing readiness probes? `kubectl describe pod <service-c-pod>`
   - What do the logs say? Use the `HighErrorRate` triage steps on Service C.

4. **Wait for automatic recovery**:
   - After `CIRCUIT_BREAKER_RECOVERY_TIMEOUT` (default: 5s), the breaker transitions to `HALF_OPEN`.
   - If the next probe request succeeds, it resets to `CLOSED`.
   - If it fails, it re-trips to `OPEN` for another recovery window.

### When to Intervene

- If the circuit stays `OPEN` for >1 minute and Service C looks healthy, check whether there's a network issue or DNS resolution problem between Service B and Service C.
- If Service C is genuinely down, focus on fixing Service C — the circuit breaker is doing its job.

---

## Incident 4: `KafkaConsumerLagHigh` — Telemetry Backpressure

### When You Get Paged

The `kafka_consumergroup_lag` metric exceeds 50 messages for >1 minute on the `app-logs` topic. This means log consumption is falling behind production.

### Triage Steps

1. **Identify the lagging consumer group**:
   ```promql
   kafka_consumergroup_lag{topic="app-logs"}
   ```

2. **Check consumer pods**:
   - `fluent-bit-consumer`: `kubectl logs -l app=fluent-bit-consumer --tail=50`
   - `anomaly-detector`: `kubectl logs -l app=anomaly-detector --tail=50`

3. **Check Loki ingestion health**:
   ```promql
   up{job="loki"}
   ```
   If Loki is down, the Fluent Bit consumer will back up.

4. **Look for error patterns** in the consumer:
   ```logql
   {job="fluent-bit"} |= "error" |= "consumer"
   ```

### Remediation

| Issue | Fix |
|---|---|
| Consumer pod stuck/crashed | `kubectl rollout restart deployment fluent-bit-consumer` |
| Loki unreachable | Check Loki pod status, PVC storage, restart if needed |
| Anomaly detector stuck | `kubectl rollout restart deployment anomaly-detector` |
| Sustained high throughput | Increase Kafka partitions + consumer replicas |

---

## Incident 5: `TelemetryServiceDown` - Telemetry Pipeline Down

### What It Means

Prometheus has been unable to scrape a telemetry component for one minute. Application traffic can still be healthy, but the ability to observe it is degraded. Treat this as urgent because it can mask a concurrent application incident.

### First Triage Steps

1. Identify the affected `job` label in Alertmanager or Grafana:
   ```promql
   up{job=~"loki|jaeger|fluent-bit-shipper|fluent-bit-consumer|kafka-exporter"}
   ```
2. Check the workload and its recent events:
   ```powershell
   kubectl get pods -n <namespace> -l app=<component>
   kubectl describe pod -n <namespace> <pod-name>
   kubectl logs -n <namespace> <pod-name> --previous
   ```
3. Confirm that Prometheus can reach the target again after the workload becomes Ready.

### Distinguishing the Component

| Affected job | Likely impact | First component-specific checks |
|---|---|---|
| `fluent-bit-shipper` | New container logs are not shipped to Kafka | Confirm DaemonSet pods on every node; inspect `/api/v1/health`; verify Kafka connectivity in Fluent Bit logs. |
| `fluent-bit-consumer` | Kafka log backlog grows and Loki stops receiving new records | Check `kafka_consumergroup_lag`, consumer logs, and the Loki endpoint. |
| `kafka-exporter` | Kafka telemetry is missing; log delivery may still function | Check exporter configuration first, then Kafka broker health. Do not infer a Kafka outage from exporter scrape failure alone. |
| `loki` | Logs cannot be queried or ingested | Check Loki readiness, memory/disk pressure, and consumer output errors. |
| `jaeger` | New traces are unavailable | Check collector/query readiness and OTLP connectivity. |

### Pipeline Chaos Drill

The `pipeline-component-kill` scenario deletes one Fluent Bit shipper pod, records `up{job=~"loki|jaeger|fluent-bit.*"}` before/during/after, waits for `TelemetryServiceDown` in Alertmanager, and measures recovery after Kubernetes recreates the pod.

```powershell
$env:CHAOS_NAMESPACE = "dev" # use the live target namespace
.venv\Scripts\python scripts/chaos_drill.py --scenario pipeline-component-kill --json
```

Alertmanager confirmation is automated. Slack delivery must be confirmed in `#observability-alerts` because the webhook is write-only and does not provide a delivery-status API to the drill.

---

## MTTR Comparison: Before vs After the Observability Stack

This table compares incident response with and without the full pipeline (structured logging → Kafka → Loki + Jaeger + Prometheus + Alertmanager + Circuit Breakers).

| Metric | Before (Manual SSH + grep) | After (Full Pipeline) |
|---|---|---|
| **Time to Detection** | 5–10 min (user reports, manual checks) | <30s (Alertmanager fires, Slack notification) |
| **Time to Identify Root Service** | 10–20 min (SSH into each pod, grep logs) | <1 min (Grafana dashboard, Loki query by request_id) |
| **Time to Root Cause** | 20–45 min (correlate timestamps manually) | <3 min (Jaeger trace waterfall, span-level error tags) |
| **Time to Recover** | 15–30 min (identify + fix + verify) | <2 min (circuit breaker isolates, K8s self-heals) |
| **Blast Radius During Outage** | All services cascade, full user impact | Isolated (circuit breaker sheds load at Service B) |
| **Evidence for Post-Mortem** | Fragmented, unreliable | Complete trace + log + metric timeline |

### What Changed

- **Detection**: Prometheus alert rules fire within 30s of threshold breach → Alertmanager → Slack.
- **Triage**: Structured JSON logs with `request_id` correlation across services mean one Loki query replaces SSH into three pods.
- **Trace correlation**: Jaeger shows the exact span that failed, with service, operation, duration, and error tags.
- **Resilience**: The circuit breaker in Service B means downstream failures don't cascade. Users see a fast 503 with `Retry-After` instead of hanging for 30s on a timeout.
- **Self-healing**: Kubernetes restarts crashed pods within seconds. The HPA scales Service C under sustained CPU load.

---

## Chaos Drill Execution

Run the automated chaos drill script to validate these metrics against the live cluster:

```powershell
# all scenarios
.venv\Scripts\python scripts/chaos_drill.py --scenario all

# individual scenarios
.venv\Scripts\python scripts/chaos_drill.py --scenario circuit
.venv\Scripts\python scripts/chaos_drill.py --scenario pod-kill
.venv\Scripts\python scripts/chaos_drill.py --scenario db-exhaust

# json output for CI integration
.venv\Scripts\python scripts/chaos_drill.py --scenario all --json
```

The drill script verifies:
- Circuit breaker trips after 3 consecutive downstream failures
- Fail-fast latency drops from ~500ms+ (timeout) to <5ms (rejection)
- Pod kill MTTR is under 60s with Kubernetes self-healing
- DB pool exhaustion is detected and isolated by the circuit breaker
