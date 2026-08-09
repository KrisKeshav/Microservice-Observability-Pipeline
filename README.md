# Local Microservice Observability Pipeline

The HTTP call chain is:

```text
Client -> Service A (:8000) -> Service B (:8001) -> Service C (:8002) -> Postgres
```

Each service emits structured JSON logs to stdout. Service A creates a `request_id` (unless supplied by the caller) and forwards it through `X-Request-ID`, so the same request can be correlated across all three services.

Service C connects to Postgres through a deliberately tiny connection pool (`DB_POOL_SIZE=3`). Under concurrent load the pool is exhausted, producing real 503 errors that are fully captured in the structured logs — no random-fail dice roll needed.

## Log Telemetry Pipeline Architecture

```text
Pod Stdout -> Fluent Bit Shipper (DaemonSet) -> Kafka (app-logs) -> Fluent Bit Consumer -> Loki -> Grafana (:30300)
```

1. **Fluent Bit Shipper**: Tails `/var/log/containers/*.log`, extracts container log payload & K8s metadata, and publishes to Kafka topic `app-logs`.
2. **Kafka Broker**: Acts as the high-throughput log buffer in KRaft mode.
3. **Fluent Bit Consumer**: Reads logs from Kafka topic `app-logs` and forwards them to Grafana Loki.
4. **Grafana Loki**: Stores log streams indexed by labels (`service`, `request_id`, `levelname`).
5. **Grafana**: Pre-configured with Loki data source at `http://127.0.0.1:30300` (admin / admin). Query logs by `{job="fluent-bit"}` or `{request_id="<uuid>"}`.

## Run with Kubernetes (Minikube / Docker Desktop)

```powershell
# Build container images and apply manifests via Kustomize
.\k8s\deploy.ps1

# Check rollout status
kubectl get pods,svc
```

### Accessing Service A in Kubernetes

Via NodePort (Port 30080):
```powershell
curl.exe http://127.0.0.1:30080/api/orders/42
```

Or using `kubectl port-forward`:
```powershell
kubectl port-forward svc/service-a 8000:8000
curl.exe http://127.0.0.1:8000/api/orders/42
```

### Clean up Kubernetes Resources

```powershell
kubectl delete -k k8s/
```

## Run with Docker Compose

```powershell
docker compose up --build -d
docker compose ps          # wait for all services to show "healthy"
```

### Create and query an order

```powershell
curl.exe -X POST http://127.0.0.1:8000/api/orders -H "Content-Type: application/json" -d "{\"order_id\": \"test-001\"}"
curl.exe http://127.0.0.1:8000/api/orders/test-001
```

### Deterministic demo scenarios

```powershell
# Successful request
curl.exe -i -H "X-Request-ID: demo-success-001" -H "X-Demo-Scenario: success" http://127.0.0.1:8000/api/orders/42

# C returns 500 -> B records a downstream error -> A returns 502
curl.exe -i -H "X-Request-ID: demo-error-001" -H "X-Demo-Scenario: error" http://127.0.0.1:8000/api/orders/42

# C runs pg_sleep(2) -> B times out -> A returns 504
curl.exe -i -H "X-Request-ID: demo-timeout-001" -H "X-Demo-Scenario: slow" http://127.0.0.1:8000/api/orders/42
```

## Load Test — Proving the Logging Works

Run Locust against the stack to trigger organic pool exhaustion:

```powershell
pip install locust
cd loadtest
locust --headless -u 50 -r 10 --run-time 30s --host http://127.0.0.1:8000
```

Then confirm the failures appear in the structured JSON logs:

```powershell
docker compose logs service-c | findstr "db_pool_exhausted"
```

You should see multiple JSON log lines with `"event": "db_pool_exhausted"` — real database failures with full request correlation, before Kafka/Loki/Jaeger exist.

### Configuration

| Variable                     | Service | Default | Purpose                               |
|------------------------------|---------|---------|---------------------------------------|
| `DATABASE_URL`               | C / AD  | -       | Postgres connection string            |
| `DB_POOL_SIZE`               | C       | `3`     | Max pool connections (keep low!)      |
| `DB_POOL_ACQUIRE_TIMEOUT`    | C       | `2.0`   | Seconds to wait for a pool slot       |
| `SERVICE_C_TIMEOUT_SECONDS`  | B       | `0.5`   | B's timeout calling C                 |
| `SERVICE_B_TIMEOUT_SECONDS`  | A       | `3`     | A's timeout calling B                 |
| `KAFKA_BOOTSTRAP_SERVERS`    | AD      | `localhost:9092` | Kafka broker endpoint               |
| `ANOMALY_WINDOW_SEC`         | AD      | `60`    | Sliding window size for rate evaluation |
| `ANOMALY_THRESHOLD`          | AD      | `5`     | Min errors in window to trigger alert |

## Step 5: Anomaly Detector (Second Kafka Consumer)

The `anomaly-detector` service acts as an independent consumer of the `app-logs` Kafka topic (using consumer group `anomaly-detector`). It proves Kafka's fan-out capabilities by evaluating error rate spikes over a sliding 60-second window.

### Query Anomaly Alerts

```powershell
# View generated alerts in Postgres
kubectl exec -it deploy/postgres -- psql -U orders -c "SELECT * FROM anomaly_alerts ORDER BY id DESC LIMIT 5;"

# Check anomaly detector logs
kubectl logs deploy/anomaly-detector --tail=20
```

## Run Locally (plain Python)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

You'll need a local Postgres running on port 5432 with database/user/password all set to `orders`, or set `DATABASE_URL` accordingly.

Start in four separate terminals:

```powershell
python -m uvicorn services.service_c.main:app --port 8002
python -m uvicorn services.service_b.main:app --port 8001
python -m uvicorn services.service_a.main:app --port 8000
python -m uvicorn services.anomaly_detector.main:app --port 8003
```

## Stop

```powershell
docker compose logs -f service-a service-b service-c anomaly-detector
docker compose down -v        # -v removes the pgdata volume
```
