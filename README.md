# Local Microservice Observability Pipeline

The HTTP call chain is:

```text
Client -> Service A (:8000) -> Service B (:8001) -> Service C (:8002) -> Postgres
```

Each service emits structured JSON logs to stdout. Service A creates a `request_id` (unless supplied by the caller) and forwards it through `X-Request-ID`, so the same request can be correlated across all three services.

Service C connects to Postgres through a deliberately tiny connection pool (`DB_POOL_SIZE=3`). Under concurrent load the pool is exhausted, producing real 503 errors that are fully captured in the structured logs — no random-fail dice roll needed.

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
| `DATABASE_URL`               | C       | -       | Postgres connection string            |
| `DB_POOL_SIZE`               | C       | `3`     | Max pool connections (keep low!)      |
| `DB_POOL_ACQUIRE_TIMEOUT`    | C       | `2.0`   | Seconds to wait for a pool slot       |
| `SERVICE_C_TIMEOUT_SECONDS`  | B       | `0.5`   | B's timeout calling C                 |
| `SERVICE_B_TIMEOUT_SECONDS`  | A       | `3`     | A's timeout calling B                 |

## Run Locally (plain Python)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

You'll need a local Postgres running on port 5432 with database/user/password all set to `orders`, or set `DATABASE_URL` accordingly.

Start in three separate terminals:

```powershell
python -m uvicorn services.service_c.main:app --port 8002
python -m uvicorn services.service_b.main:app --port 8001
python -m uvicorn services.service_a.main:app --port 8000
```

## Stop

```powershell
docker compose logs -f service-a service-b service-c
docker compose down -v        # -v removes the pgdata volume
```
