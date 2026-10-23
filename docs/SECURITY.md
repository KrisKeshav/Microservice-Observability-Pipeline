# Security

What is in place, what was actually checked, and what is missing.

## In the CI pipeline

*The pipeline is written but has not run on GitHub yet. The "checked locally" column shows
what I ran on my machine.*

| Check | What it does | Checked locally |
|---|---|---|
| Pinned versions | `requirements.txt` lists exact versions, so every build installs the same thing | – |
| `pip-audit` | Fails if a Python package has a known vulnerability | ✅ First run found 4 Starlette CVEs (pulled in by FastAPI). Fixed by upgrading FastAPI, Starlette and the Prometheus instrumentator; now "No known vulnerabilities found" |
| Trivy | Scans each image; fails on HIGH/CRITICAL issues that already have a fix | ✅ Service A image (after the dependency upgrade): no fixable HIGH/CRITICAL findings. The other three images share the same base and packages but were not scanned separately |
| Build once | The image that was scanned is the one that gets pushed and deployed | – |
| Commit-SHA tags | Images are never deployed as `latest` | – |
| Least privilege | The workflow token is read-only by default; only the push job may write packages, only the deploy job may write to the repo | – |
| Dependabot | Opens weekly pull requests for outdated Python packages, the base image and GitHub Actions | – |
| kube-score | Reports Kubernetes hardening gaps; **report only**, it does not fail the build | – |

Why "only issues that already have a fix" for Trivy: if no fixed version exists, nobody can act
on it, and the build would be blocked forever. Rebuilding pulls the newest `python:3.12-slim`
patch release, which picks up OS fixes once they exist.

## In Kubernetes

App containers (A, B, C, anomaly detector):
- run as a normal user (UID 10001), never root;
- have a read-only filesystem;
- cannot gain more privileges and have all Linux capabilities removed;
- use the default seccomp profile (blocks unusual system calls);
- do not get a Kubernetes API token, because they never talk to the API.

Tested: all of these pods started and passed their health checks with these settings on minikube.

Third-party pods use the same settings where the image allows it. Two exceptions:

| Pod | Exception | Why |
|---|---|---|
| Postgres | starts as root | Its startup script fixes file ownership on the volume, then switches to the `postgres` user. |
| Fluent Bit shipper | runs as root (but with no capabilities) | The log files on the node belong to root. |

Permissions are namespaced Roles: Fluent Bit and Prometheus can only list pods in their own
namespace.

## Secrets

- **No passwords in Git.** `scripts/bootstrap.ps1` generates random passwords and creates the
  `postgres-secret` and `app-secrets` Secrets directly in the cluster. Running it again keeps
  existing secrets (Postgres only reads its password once, on first start).
  Tested: it created the secrets, and a second run left them unchanged.
- Pods read secrets as environment variables. Grafana's database password is also an environment
  variable, not written in its config file.
- `compose.yaml` has a fixed local password. It is only for a laptop.

**Better for production:** the External Secrets Operator copying secrets from Vault or AWS
Secrets Manager, or Sealed Secrets (an encrypted secret that only the cluster can decrypt). The
manifests would stay the same; they only reference the Secret names.

## In the application

- **Fault injection is off unless switched on.** The `X-Demo-Scenario` header only works when
  `FAULT_INJECTION_ENABLED=true`, which only dev sets. Otherwise anyone could slow down or break
  prod by sending one header. (Unit-tested both ways.)
- Order IDs are URL-encoded before being passed to the next service, so an ID like
  `a?admin=true` cannot add parameters to internal calls. (Unit-tested.)

## Not done yet

- NetworkPolicies (which pod may talk to which)
- Encryption between services (mTLS, e.g. with a service mesh)
- Signing images and checking signatures before deploying
- Kafka authentication
- Pinning GitHub Actions to commit hashes instead of version tags
