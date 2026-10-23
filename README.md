# Microservice Observability Pipeline

A learning project. Three small Python services run on Kubernetes, and around them I built the
tools you need to see what they are doing and to notice when they break:

- **Logs** collected from every pod, buffered in Kafka, searchable in Loki
- **Traces** that follow one request through all three services (Jaeger)
- **Metrics and alerts** (Prometheus, Alertmanager), including SLOs and error budgets
- **A circuit breaker** so one failing service does not drag down the others
- **CI/CD with GitOps**: GitHub Actions builds and scans images, Argo CD deploys them
- **Drills** that break things on purpose and measure what happens

The services do very little on purpose. The interesting part is everything around them.

```
Client ─▶ Service A ─▶ Service B ─▶ Service C ─▶ Postgres
          (gateway)    (circuit     (database
                        breaker)     access)

Logs:    every pod ─▶ Fluent Bit ─▶ Kafka ─▶ Fluent Bit ─▶ Loki ─▶ Grafana
                                      └────▶ Anomaly detector ─▶ Alertmanager
Traces:  A, B, C ─▶ Jaeger
Metrics: Prometheus scrapes every pod ─▶ alert rules ─▶ Alertmanager ─▶ Slack
```

## What has actually been tested

I want this README to be honest about what is proven and what is only designed.

| Feature | Status | How it was checked |
|---|---|---|
| Unit tests (62) and lint | ✅ Passing | `pytest`, `ruff` |
| Alert and SLO rules | ✅ Passing | `promtool test rules` with fake traffic (see `docs/SLO.md`) |
| No known vulnerable Python packages | ✅ Passing | `pip-audit` locally (it found Starlette CVEs, fixed by upgrading) |
| Image scan | ✅ Passing | Trivy locally on the Service A image: no fixable HIGH/CRITICAL findings |
| Request path + tracing with Docker Compose | ✅ Works | One request produced one trace with 14 spans across A, B, C and the SQL queries |
| Full stack on Kubernetes (minikube) | ✅ Works | All pods ran with the hardened settings; all Prometheus targets up; all 26 rules loaded |
| Log line → Loki → its trace in Jaeger | ✅ Works | `scripts/canary_watchdog.py` |
| Circuit breaker, pod kill, DB pool, log buffering | ✅ Measured | `scripts/chaos_drill.py`; results in `docs/RUNBOOK.md` |
| Anomaly detector | ✅ Works | Wrote 2 alerts to Postgres during the drills |
| GitHub Actions pipeline (`ci.yml`, `promote.yml`) | ⏳ Not run yet | Needs a push to GitHub |
| Argo CD deploying the new manifests | ⏳ Not run yet | Needs the CI images to exist |
| dev and prod running side by side | ⏳ Not run yet | Checked only by rendering the manifests |
| Slack notifications | ⏳ Not tested | No Slack webhook configured |

The Kubernetes test ran the base manifests plus the dev fault-injection setting in a temporary
namespace, with locally built images. To fit on my laptop I lowered the CPU *requests* and
limited Service C to one replica during the test. That changes scheduling, not behaviour.
The Kubernetes run used images built *before* the security upgrade (FastAPI/Starlette); after the
upgrade I re-ran the unit tests and the Compose trace check, not the Kubernetes drills.

## Repository layout

```
services/            service_a, service_b, service_c, anomaly_detector (FastAPI apps)
common/              shared code: logging, tracing, metrics, circuit breaker, database
k8s/base/            all Kubernetes manifests and config files for one environment
k8s/overlays/dev     dev settings: image tags (written by CI), NodePorts, fault injection on
k8s/overlays/prod    prod settings: image tags (written by promotion PR), 2 replicas, PDBs
k8s/argocd/          Argo CD Applications, one per environment
scripts/             bootstrap.ps1, canary_watchdog.py, chaos_drill.py
.github/workflows/   ci.yml (build, test, scan, deploy to dev), promote.yml (dev → prod)
docs/                ARCHITECTURE, RUNBOOK, SLO, SECURITY
```

## Run it

### Option 1: Docker Compose (request path and traces only)

```powershell
docker compose up --build
curl http://localhost:8000/api/orders/demo-1
# open http://localhost:16686 (Jaeger) to see the trace
```

### Option 2: Kubernetes with Argo CD (the intended setup)

This is the designed flow. It has not been run end to end yet (see the table above).

1. Push to `main`. CI builds the four images, tags them with the commit SHA and pushes them to
   GitHub Container Registry. Then it writes that tag into `k8s/overlays/dev/kustomization.yaml`.
2. Make the four packages public in GitHub (Packages → settings) so minikube can pull them.
3. Prepare the cluster:
   ```powershell
   minikube start --cpus 4 --memory 8192
   minikube addons enable metrics-server     # the autoscaler needs CPU metrics
   .\scripts\bootstrap.ps1                   # creates the dev namespace, random secrets, Argo CD app
   ```
   Argo CD must already be installed (the script prints the install command if not).
4. Argo CD syncs `k8s/overlays/dev` from GitHub.
5. To release to prod: run `.\scripts\bootstrap.ps1 -Environments prod` once, then run the
   **Promote to prod** workflow in GitHub Actions and merge the pull request it opens.
   (Repo setting needed: *Actions → Allow GitHub Actions to create pull requests*.)

To open the UIs, use port-forwarding (this is what I used in testing):

```powershell
kubectl port-forward -n dev svc/service-a 30080:8000
kubectl port-forward -n dev svc/grafana 30300:3000      # login: admin / password printed by bootstrap
kubectl port-forward -n dev svc/prometheus 30090:9090
kubectl port-forward -n dev svc/alertmanager 30093:9093
kubectl port-forward -n dev svc/jaeger 31686:16686
kubectl port-forward -n dev svc/loki 30100:3100
```

### Check that it works

```powershell
.venv\Scripts\python scripts\canary_watchdog.py              # one request, followed through logs and traces
.venv\Scripts\python scripts\chaos_drill.py --scenario all   # the four drills (dev only)
```

## Development

```powershell
python -m venv .venv; .venv\Scripts\pip install -r requirements-dev.txt
$env:OTEL_SDK_DISABLED="true"; .venv\Scripts\python -m pytest
.venv\Scripts\python -m ruff check .
kubectl kustomize k8s/overlays/dev          # see the final Kubernetes YAML
```

## Limitations (known and accepted for a learning project)

- One instance each of Kafka, Loki and Postgres. If one fails, that part stops until Kubernetes restarts it.
- Jaeger keeps traces in memory only; a restart loses them.
- dev and prod are namespaces in one laptop cluster. A company would use separate clusters.
- Each environment runs its own copy of the monitoring tools, which is simple but wasteful.
- Secrets are created by a script, not by a secret manager such as Vault.
- No network policies, no encryption between services, Prometheus itself is not monitored.

More: [Architecture](docs/ARCHITECTURE.md) · [Runbook](docs/RUNBOOK.md) · [SLOs](docs/SLO.md) · [Security](docs/SECURITY.md)
