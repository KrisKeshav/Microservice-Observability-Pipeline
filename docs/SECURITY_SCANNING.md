# CI Security Scanning

The CI workflow has three security controls after image build and before GHCR publication.

| Tool | Scope | Policy |
|---|---|---|
| Trivy | Each built service image | Fails on known HIGH or CRITICAL OS/library vulnerabilities; unfixed vulnerabilities are excluded intentionally because no remediation exists in the project image layer. |
| pip-audit | `requirements.txt` | Fails on known vulnerable Python packages. |
| kube-score | Rendered dev Kustomize manifests | Reports workload hardening gaps, including missing resource limits, non-root execution, and read-only root filesystems. |

## Findings and Accepted Risks

No live CI scan has been executed for Sprint 5 yet, so there are no verified current findings to classify. The next CI run must be captured here before declaring this sprint verified.

The repository currently has no vulnerability ignore file and no suppressed kube-score tests. Any failing Trivy or pip-audit result blocks the pipeline. Kube-score output is visible in the CI job logs so that infrastructure hardening gaps are reviewed explicitly rather than silently ignored.

## Required Evidence

Record the CI run URL or paste the output for all three tools here after the planned verification session. For every finding that remains, add one line in this format:

`Accepted risk — <finding>: <short business or technical reason and review owner>.`
