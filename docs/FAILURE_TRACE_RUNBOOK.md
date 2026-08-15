# Failure Tracing Runbook: End-to-End Incident Investigation

This runbook guides you through executing a full-pipeline load test against the Kubernetes cluster and capturing a complete end-to-end failure trace across **Service A -> Service B -> Service C -> PostgreSQL -> Grafana / Loki / Jaeger**.

---

## Step 1: Deploy the Full Pipeline on Kubernetes

Ensure your local Kubernetes cluster (Minikube or Docker Desktop K8s) is active, then deploy all services and observability infrastructure:

```powershell
# Run deployment script (builds images & applies manifests)
.\k8s\deploy.ps1

# Verify all pods are running
kubectl get pods
```

Wait until all pods (`service-a`, `service-b`, `service-c`, `anomaly-detector`, `kafka`, `loki`, `jaeger`, `fluent-bit-shipper`, `fluent-bit-consumer`, `postgres`, `grafana`) show status `Running`.

---

## Step 2: Launch the Full-Pipeline Load Test

Run the automated PowerShell load test script. This fires 100 concurrent virtual users against Service A via Locust:

```powershell
.\loadtest\full_pipeline_test.ps1
```

Or run Locust directly from terminal:

```powershell
cd loadtest
locust -f locustfile.py --headless -u 100 -r 20 --run-time 45s --host http://127.0.0.1:30080
```

### What Happens Under Load:
1. `Service A` receives hundreds of incoming order requests per second.
2. `Service B` forwards these requests to `Service C`.
3. `Service C` attempts to acquire a connection from its constrained pool (`DB_POOL_SIZE=3`).
4. Connections exhaust rapidly, causing `asyncpg.exceptions.InterfaceError` / `asyncio.TimeoutError`.
5. `Service C` logs `db_pool_exhausted` and returns `HTTP 503`.
6. `Service B` logs `service_c_error` / `service_c_timeout` and returns `HTTP 502/504`.
7. `Service A` returns `HTTP 502/504` to Locust.
8. Fluent Bit ships stdout logs to Kafka (`app-logs`).
9. Fluent Bit Consumer ingests logs into Loki; Anomaly Detector logs alert records into PostgreSQL.

---

## Step 3: Open Grafana Observability Dashboard

1. Open your browser and navigate to Grafana at [http://127.0.0.1:30300](http://127.0.0.1:30300) (Login: `admin` / `admin`).
2. Navigate to Dashboards -> **Microservice Observability Pipeline**.

Observe the dashboard panels:
* **Log Rate by Service**: Shows a massive spike in log throughput during the load test.
* **Database Anomaly Alerts**: Displays newly created anomaly alert records inserted by `anomaly-detector`.
* **Error Logs Stream**: Live view of `ERROR` level logs flowing through Loki.

---

## Step 4: Trace a Real Failure End-to-End

### 4.1 Locate a Failed Request ID
Look at the **Error Logs Stream** panel in Grafana or run a curl command to generate a deterministic failure with a known request ID:

```powershell
curl.exe -i -H "X-Request-ID: loadtest-fail-demo-001" -H "X-Demo-Scenario: slow" http://127.0.0.1:30080/api/orders/42
```

### 4.2 Filter Grafana Logs by Request ID
1. In the **Request ID Filter** text input at the top of the Grafana dashboard, enter: `loadtest-fail-demo-001`.
2. The **Unified Log Telemetry Stream** panel filters down to display the exact execution timeline across all services:
   - `service-a`: `request_received`
   - `service-b`: `request_received`
   - `service-c`: `db_query_start`
   - `service-c`: `db_pool_exhausted` (Level: ERROR)
   - `service-b`: `service_c_timeout` (Level: ERROR)
   - `service-a`: `service_b_timeout` (Level: ERROR)

### 4.3 Drill Down into Jaeger Distributed Trace
1. Expand the `db_pool_exhausted` log line in the Unified Log Telemetry Stream.
2. Locate the `trace_id` field.
3. Click the link next to it titled **View Trace in Jaeger** (or **TraceID**).
4. Grafana transitions directly into the Jaeger trace view:
   * **Span 1**: `service-a` (`GET /api/orders/{order_id}`) — Total Duration ~500ms+ (Status 504)
   * **Span 2**: `service-b` (`GET /internal/orders/{order_id}`)
   * **Span 3**: `service-c` (`GET /internal/validate/{order_id}`)
   * **Span 4**: `asyncpg` (`SELECT id, created_at FROM orders...`) — FAILED / TIMED OUT

---

## Step 5: Incident Resolution & Telemetry Verification

Verify that all signals across the observability stack reflect the incident state:

1. **Log Rate & Alerts**: Grafana Dashboard displays the log rate spike and `anomaly_alerts` table entries.
2. **Correlated Logs**: Unified Log Stream filtered by `request_id` highlights `service-c` `db_pool_exhausted`.
3. **Trace Waterfall**: Jaeger trace graph shows the 4-tier span hierarchy (`service-a` -> `service-b` -> `service-c` -> `asyncpg`).

This confirms end-to-end telemetry propagation: **Client HTTP error -> Container stdout -> Kafka -> Loki -> Anomaly Detector -> Jaeger trace correlation**.
