# SLOs and error budgets

## Words first

- **SLI** (indicator): what you measure, e.g. "share of requests that did not fail".
- **SLO** (objective): the target, e.g. "99% over 30 days".
- **Error budget:** what the target allows to go wrong. 99% means 1% of requests may fail.
- **Burn rate:** how fast the budget is being used. 1 = exactly on schedule (the budget lasts
  30 days); 10 = ten times too fast (the budget is gone in 3 days).

## The two SLOs

Measured at Service A, because that is what users talk to.

| SLO | Target over 30 days | Counts as bad | Budget |
|---|---|---|---|
| Availability | 99% | a 5xx response | 1% of requests (about 7.2 hours if everything failed) |
| Latency | 95% | `GET /api/orders/{id}` slower than 300 ms | 5% of those requests |

4xx responses (client mistakes) do not count as failures.

## How Prometheus calculates it

File: `k8s/base/monitoring/slo-rules.yml`. For windows of 5m, 30m, 1h, 6h and 30d it records:

```
error ratio  = 5xx requests / all requests
latency bad  = 1 - (requests ≤ 0.3 s / all order reads)
burn rate    = ratio / budget          (budget = 0.01 or 0.05)
budget left  = 1 - (30-day error ratio / 0.01)
```

Two details I had to get right (both were bugs before):
- **`or vector(0)`**: Prometheus only creates the "5xx" series after the first 5xx happens.
  Without this, the error ratio would show *no data* instead of *0* on a healthy service.
- **A histogram bucket at 0.3 s**: Prometheus can only answer "how many requests were faster
  than X" when X is a bucket boundary. The library's default buckets were 0.1, 0.5 and 1 s, so a
  300 ms SLO could never be calculated.

When there is no traffic at all, the ratio is "0 divided by 0" and Grafana shows NaN. That just
means "no requests", not an error.

## The alerts

| Alert | Fires when | Severity |
|---|---|---|
| `SLOFastBurn` | burn rate > 14.4 in **both** the last 1h and the last 5m, for 2 minutes | critical |
| `SLOSlowBurn` | burn rate > 6 in **both** the last 6h and the last 30m, for 15 minutes | warning |

**Why two windows?** The long window makes sure it is a real problem, not a 10-second blip. The
short window makes sure it is still happening, so the alert clears soon after the fix.

**Why 14.4?** Burning 14.4× for one hour uses 2% of the monthly budget (14.4 × 1 h ÷ 720 h).
This threshold comes from the Google SRE Workbook.

## How I know the rules work

- **Rule unit tests** (`k8s/base/monitoring/rules-test.yml`, run with `promtool test rules`). They
  feed fake traffic in and check that:
  - 50% errors fires `HighErrorRate` after 1 minute, not before;
  - at very low traffic (0.1 requests/s) the error ratio is still correct;
  - with no errors ever, the ratio is 0 and the full budget remains;
  - 100% errors fires `SLOFastBurn` but not yet `SLOSlowBurn`.
- **Live:** during the `db-exhaust` drill on minikube, `SLOFastBurn` really fired and reached
  Alertmanager. The Slack message was not tested (no webhook configured).
