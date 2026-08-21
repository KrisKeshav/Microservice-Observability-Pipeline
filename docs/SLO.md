# Service Level Objectives and Error Budgets

This project measures SLOs at the Service A boundary because it is the user-facing API gateway and already exposes RED metrics to Prometheus.

## Objectives

| SLO | Objective | Good events | Error budget |
|---|---|---|---|
| Availability | 99% over a rolling 30 days | Service A HTTP requests that do not return 5xx | 1% of requests; approximately 7.2 hours in a 30-day month if the service is completely unavailable |
| Order-read latency | 95% over a rolling 30 days | `GET /api/orders/{order_id}` requests completing in 300ms or less | 5% of requests may exceed 300ms |

The availability SLO counts only 5xx responses as failures. Client errors (4xx) are valid responses and do not consume its availability budget. The latency SLO is calculated from `http_request_duration_seconds_bucket` for the normalized `handler="/api/orders/{order_id}"` label.

## Recording Rules

`k8s/base/monitoring/slo-rules.yaml` records 5m, 30m, 1h, 6h, and 30d error ratios. It converts each ratio into a burn rate by dividing it by the relevant budget (1% for availability, 5% for latency). Prometheus retains 35 days so the 30-day expressions have sufficient history.

`slo:service_a:error_budget_remaining:30d` is the availability-budget fraction remaining. Grafana displays it as a gauge together with the current burn rates and firing SLO alerts.

## Multi-window Burn-rate Alerts

The alerts use the Google SRE multi-window pattern: a short window detects current impact while a longer window reduces noise from a brief spike.

| Alert | Condition | Severity and handling |
|---|---|---|
| `SLOFastBurn` | Burn rate >14.4x in both 1h and 5m windows for 5m | `critical`; routed to the existing paging Slack channel |
| `SLOSlowBurn` | Burn rate >6x in both 6h and 30m windows for 30m | `warning`; routed to the lower-urgency SLO Slack channel |

At 14.4x, a 1% availability budget would be exhausted in roughly two days if the rate persisted. The fast alert is intended to page quickly; the slow alert prompts investigation before gradual degradation consumes the budget.

## Live Verification Procedure

The SLO rules must be verified against the running cluster, not merely parsed. Generate sustained Service A 5xx responses for more than the alert's combined evaluation window, then capture:

1. The Prometheus `ALERTS{alertname="SLOFastBurn"}` result.
2. The Alertmanager API/UI showing the alert as firing.
3. The corresponding critical Slack notification.

These live artifacts are deliberately pending until the planned verification session.
