# Architecture

## The question this project answers

When a request fails or is slow, can I quickly answer:

1. **Are users affected?** → SLO alerts on the gateway (Service A)
2. **Which service is the problem?** → error rate and latency per service (Prometheus)
3. **Why?** → the trace shows where the time went; the logs show the error message
4. **Did it recover?** → the canary and the chaos drills measure it

## The services

```
Client → Service A → Service B → Service C → Postgres
```

| Service | Job | Behaviour worth knowing |
|---|---|---|
| A (gateway) | Public API | Creates a `request_id` if the client didn't send one. Turns downstream problems into clear status codes: 504 (B too slow), 503 (B unavailable or busy, passes on `Retry-After`), 502 (B returned an error). |
| B (orchestrator) | Calls C | Uses a **circuit breaker**. Retries a call only if the connection to C could not be opened. |
| C (data layer) | Talks to Postgres | Connection pool of only **3** connections, so load tests can exhaust it on purpose. In dev, the header `X-Demo-Scenario: error` or `slow` forces a failure or a 2 s query. |
| Anomaly detector | Reads logs from Kafka | Counts ERROR log lines per minute and raises an alert when there are too many. |

### Timeouts

Rule: **a caller must wait longer than the service it calls**, otherwise the caller gives up
first and never sees the real answer.

| Call | Timeout |
|---|---|
| C waiting for a free DB connection | 0.5 s, then 503 "pool exhausted" |
| B waiting for C | 3 s (the forced `slow` query takes about 2.2 s) |
| A waiting for B | 5 s (longer than B's 3 s) |

## How each signal travels

**Logs:** each service prints one JSON line per event to stdout. Every line has the
`request_id` and the `trace_id`.
`stdout → Fluent Bit (one per node) → Kafka topic app-logs → Fluent Bit consumer → Loki → Grafana`

- *Why Kafka in the middle?* If the consumer that writes to Loki stops, the logs wait in Kafka
  and are delivered when it comes back. The `log-buffer` drill tested exactly this: the consumer
  was stopped for 100 s, and a log line written during that time arrived in Loki 6.3 s after it
  restarted. Kafka also lets a second reader (the anomaly detector) read the same logs.
- *Why is `request_id` not a Loki label?* Loki builds an index from labels. One label value per
  request would create millions of tiny streams. So labels are only `job`, `name` (service) and
  `levelname`; the `request_id` is searched inside the text with `|= "<id>"`.
- Fluent Bit stores how far it has read each log file (`DB` setting), so after a restart it
  continues from there instead of starting over. (Standard Fluent Bit behaviour; I did not run a
  separate drill for the shipper.)

**Traces:** OpenTelemetry instruments FastAPI, the HTTP client (httpx) and the database driver
(asyncpg) automatically. The trace ID travels between services in the `traceparent` HTTP header.
Tested: one request produced one trace with 14 spans across A, B and C, including the SQL queries.

**Metrics:** every pod exposes `/metrics`. Prometheus finds the pods itself (Kubernetes service
discovery) and scrapes **each pod**, not the Service in front of them. *Why?* A Service sends
each scrape to a random pod, so with 2+ replicas the numbers would jump between pods and
`rate()` would be wrong.

- Health-check and scrape requests (`/health`, `/ready`, `/metrics`) are left out of the request
  metrics, so they don't make the error rate look better than it is.
- The latency histogram has a bucket at exactly 0.3 s, because the latency SLO is "under 300 ms"
  and Prometheus can only answer at bucket boundaries.

## Delivery (CI/CD and GitOps)

*Status: designed and checked locally (the YAML is valid), but not yet run on GitHub.*

```
push to main
  → CI: lint, unit tests, pip-audit, rule tests, render manifests
  → build each image once → Trivy scan → push to GHCR tagged with the commit SHA
  → CI commits the new tag into k8s/overlays/dev
  → Argo CD sees the change in Git and deploys dev

"Promote to prod" workflow (manual)
  → copies dev's tag into k8s/overlays/prod and opens a pull request
  → merge = approval → Argo CD deploys prod
```

Why it is built this way:
- **The scanned image is the deployed image.** It is built once; prod gets the same tag dev had.
- **Commit-SHA tags, never `latest`.** You always know which code is running, and you can go back.
- **CI never connects to the cluster.** It only changes Git; Argo CD pulls. No cluster passwords in CI.
- **Rollback = `git revert`** of the commit that changed the tag.

## dev vs prod

| | dev | prod |
|---|---|---|
| Image tag | set by CI on every push | set by the promotion pull request |
| Replicas | 1 | 2 for A and B; autoscaler minimum 2 for C |
| PodDisruptionBudget | no | yes (keeps 1 pod during node maintenance) |
| Fault injection header | on | off |
| Access | NodePorts or port-forward | port-forward only |

The old setup broke when both environments ran in one cluster. Argo CD showed
`provided port is already allocated` (NodePorts are cluster-wide) and `SharedResourceWarning`
(both environments created the same ClusterRole). Now prod has no NodePorts and all
permissions are namespaced Roles, so they shouldn't collide. *I checked this by rendering both
overlays; I have not yet run both together.*

Two more details:
- **Service C has no `replicas` field.** The autoscaler (HPA) decides the count. If Git also set
  it, Argo CD would keep resetting what the autoscaler chose.
- **Config files become ConfigMaps with a hash in the name** (`configMapGenerator`). When a file
  changes, the name changes, so the pods using it restart automatically. I saw this in testing:
  changing the Fluent Bit consumer config rolled its pod.

## The circuit breaker (Service B → C)

```
CLOSED ──3 failures in a row──▶ OPEN ──after 5 s──▶ HALF_OPEN ──test call works──▶ CLOSED
                                  ▲                     │
                                  └──test call fails────┘
```

- **OPEN:** B answers immediately with 503 and `Retry-After` instead of waiting for a broken C.
  In the drill these rejections took about 19 ms.
- **HALF_OPEN:** exactly one test request goes through; the rest are still rejected.
- **Retries:** only when the connection could not be opened (the request never reached C).
  Timeouts and 5xx are not retried, because retrying an overloaded service overloads it more.

## Health checks

| Probe | Endpoint | Rule |
|---|---|---|
| Liveness ("restart me if this fails") | `/health` | Only checks that the process answers. It must not check the database, otherwise a database outage would restart every pod. |
| Readiness ("send me traffic?") | `/ready` | C: database pool is open. Anomaly detector: Kafka reader thread is running. A and B: always ready; a broken C is handled by the circuit breaker. |

## Simplifications and what a real system would do

| Here | In production |
|---|---|
| One Kafka, one Loki, one Postgres | Replicated Kafka, Loki on object storage, managed Postgres |
| Monitoring stack copied per environment | One shared monitoring platform |
| Secrets created by a script | Secret manager (Vault, AWS Secrets Manager) via External Secrets Operator |
| Jaeger stores traces in memory | Persistent trace storage with sampling |
| Anomaly detector counts errors in a 60 s window | Alert on log-based metrics in Loki or Prometheus |
