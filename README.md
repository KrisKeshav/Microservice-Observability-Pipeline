# Local Microservice Observability Pipeline — Step 1

The initial HTTP call chain is:

```text
Client -> Service A (:8000) -> Service B (:8001) -> Service C (:8002)
```

Each service emits structured JSON logs to stdout. Service A creates a `request_id` (unless supplied by the caller) and forwards it through `X-Request-ID`, so the same request can already be correlated across all three local terminals.

## Run locally (plain Python)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Start these in three separate PowerShell windows from the project root:

```powershell
python -m uvicorn services.service_c.main:app --port 8002
python -m uvicorn services.service_b.main:app --port 8001
python -m uvicorn services.service_a.main:app --port 8000
```

## Demonstrate the call chain

Use fixed request IDs to correlate the JSON logs in all three terminals:

```powershell
# Successful request
curl.exe -i -H "X-Request-ID: demo-success-001" -H "X-Demo-Scenario: success" http://127.0.0.1:8000/api/orders/42

# C returns 500 -> B records a downstream error -> A returns 502
curl.exe -i -H "X-Request-ID: demo-error-001" -H "X-Demo-Scenario: error" http://127.0.0.1:8000/api/orders/42

# C waits 1.5s -> B times out after 0.5s -> A returns 502
curl.exe -i -H "X-Request-ID: demo-timeout-001" -H "X-Demo-Scenario: slow" http://127.0.0.1:8000/api/orders/42
```

Without `X-Demo-Scenario`, C has a 25% intentional error rate and a 15% slow-response rate. The header is only for repeatable demonstrations.

The rates and timeouts can be adjusted using `SERVICE_C_FAILURE_RATE`, `SERVICE_C_SLOW_RATE`, `SERVICE_C_SLOW_RESPONSE_SECONDS`, `SERVICE_C_TIMEOUT_SECONDS` (Service B), and `SERVICE_B_TIMEOUT_SECONDS` (Service A).

## Run with Docker Compose

Docker Compose creates one private network for the three containers. Within that network, Service A reaches B at `http://service-b:8000`, and B reaches C at `http://service-c:8000`. Only Service A publishes a host port because it is the public entry point.

```powershell
docker compose up --build -d
docker compose ps
```

Wait until all three services show `healthy`, then use the same requests from above against `http://127.0.0.1:8000`. For example:

```powershell
curl.exe -i -H "X-Request-ID: compose-success-001" -H "X-Demo-Scenario: success" http://127.0.0.1:8000/api/orders/42
curl.exe -i -H "X-Request-ID: compose-error-001" -H "X-Demo-Scenario: error" http://127.0.0.1:8000/api/orders/42
curl.exe -i -H "X-Request-ID: compose-timeout-001" -H "X-Demo-Scenario: slow" http://127.0.0.1:8000/api/orders/42
```

Inspect structured logs by service or stop the stack when finished:

```powershell
docker compose logs -f service-a service-b service-c
docker compose down
```
