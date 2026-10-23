# Runbook

What to do when each alert fires. Commands use `-n dev`; use `-n prod` for prod.

## The basic loop for any incident

1. **Are users affected?** Grafana → *SLO / Error budget* row. Which alerts are firing?
2. **Which service?** *RED metrics* row. Usually the deepest broken service shows errors first,
   and every service that calls it shows errors too.
3. **Why?** Open *Error logs*, copy a `request_id` into the **Request ID** box at the top. You
   now see that one request's logs from A, B and C. Its `trace_id` opens the trace in Jaeger,
   which shows the failing or slow step.
4. **Fix**, then check that the error rate is back to normal and the alert resolves. Run
   `python scripts/canary_watchdog.py` to confirm logs and traces still flow.

I used this loop on the project itself: a 502 in testing led, through one request's logs, to
"pool exhausted" in Service C, and the same approach found a log field going missing in the
pipeline (see *Lessons from testing* below).

---

## SLOFastBurn (critical) / SLOSlowBurn (warning)

**Means:** Service A is using up its monthly error budget too fast: 14.4× the sustainable speed
(fast) or 6× (slow). See `docs/SLO.md`.

**Do:** check whether the availability or the latency SLO is burning, then follow the basic loop.
Usually `HighErrorRate` or `DbPoolExhaustion` fires at the same time and points to the cause.

## HighErrorRate (critical)

**Means:** more than 5% of a service's requests returned 5xx for 1 minute.

**What Service A's status code tells you:**
| A returns | Meaning |
|---|---|
| 503 with `Retry-After` | B's circuit breaker is open, so C is failing. |
| 504 | B didn't answer within 5 s. Look at the trace to see which step was slow. |
| 502 | B answered with an error. B's `service_c_error` log shows C's status code. |

```powershell
kubectl get pods -n dev                           # crashing or restarting pods?
kubectl logs -n dev deploy/service-c --tail=50
```

**Fix:** if a new version caused it, revert the deploy commit in Git; Argo CD goes back to the
previous image.

## DbPoolExhaustion (critical)

**Means:** all 3 connections in a Service C pod were busy for 30 s. New queries wait 0.5 s for a
connection, then fail with 503.

**Check:** Grafana *DB pool* panel (active = max); error logs show `db_pool_exhausted` with
`in_use` and `max` in the message. If `in_use` is lower than `max`, the pool was not full; opening
a new connection was slow instead.

**Fix:** short term, run more Service C pods (each pod has its own pool). Long term, find the slow
query in the traces.

**Expected side effect:** B's circuit breaker opens and closes repeatedly. That is the breaker
protecting the system: users get a fast 503 instead of waiting.

## TelemetryServiceDown (critical)

**Means:** Prometheus has not reached Loki, Jaeger, a Fluent Bit, kafka-exporter or Alertmanager
for 1 minute. We are partly blind.

```powershell
kubectl get pods -n dev -l app=<job>
kubectl describe pod -n dev -l app=<job>      # look for OOMKilled, image pull errors, failed probes
kubectl logs -n dev -l app=<job> --previous
```

**Are logs lost?**
- Fluent Bit **consumer** down: no. Logs wait in Kafka (tested, see drills below).
- Fluent Bit **shipper** down: logs stay in the files on the node and the shipper continues from
  its saved position when it restarts (Fluent Bit's documented behaviour; not drilled).
- **Loki** down: not tested. I would not promise zero loss here.
- **Jaeger** down: yes, traces from that time are lost (in-memory, no buffer).

## KafkaConsumerLagHigh (warning)

**Means:** more than 100 log messages have waited for a consumer for 2 minutes.
`fluent-bit-loki-consumer` lagging = logs in Grafana are late; check the consumer and Loki.
`anomaly-detector` lagging = check that pod's `/ready` and logs.

## AppTargetDown (warning)

**Means:** Prometheus cannot read `/metrics` from an app pod for 1 minute (usually a crash loop).
`kubectl describe pod` and `kubectl logs --previous` show why.

## AnomalyErrorSpike (warning, from the anomaly detector)

**Means:** 5 or more ERROR log lines in 60 s (then 5 minutes of quiet before it can fire again).
It usually fires together with `HighErrorRate`. Example error lines are saved in the
`anomaly_alerts` table and shown in Grafana; they are a good starting point for the basic loop.

---

## Drills and measured results

Run with `python scripts/chaos_drill.py --scenario <name>` against dev.

| Drill | What it does | Result (minikube test, 12 Oct 2026) |
|---|---|---|
| `circuit` | Sends requests that force C to fail | Breaker opened after 3 failures in a row; rejections took ~19 ms with `Retry-After`; after 5 s a normal request closed it again (200). |
| `pod-kill` | Deletes a Service C pod while sending traffic | 1 replica: new pod ready after 15.7 s, **12 of 14** requests failed. 2 replicas: new pod ready after 14.9 s, **0 of 18** failed. |
| `db-exhaust` | 10 parallel slow requests for 75 s (pool of 3) | `DbPoolExhaustion` reached Alertmanager after 65 s; the breaker opened about 40 times; most failures were fast 503s; `SLOFastBurn` also fired. |
| `log-buffer` | Stops the Loki consumer, then starts it again | Run with a 100 s outage: `TelemetryServiceDown` fired (detected at the last check, ~100 s); Kafka held up to 349 messages; the log line written during the outage reached Loki 6.3 s after restart. |

Notes on these numbers:
- They come from one run on a laptop. They show behaviour, not production performance.
- The `log-buffer` alert needs about 1.5 minutes (1 minute `for:` plus scrape and grouping
  delays), so 100 s was too tight. The script's default outage is now 150 s.
- "About 40" breaker trips comes from Prometheus `increase()`, which estimates, so it is not an
  exact whole number.

## Lessons from testing

- **A crash-looping StatefulSet pod is not replaced by a rollout.** After fixing Kafka's config,
  the old broken pod stayed. Fix: `kubectl delete pod kafka-0 -n dev`.
- **A fast state needs a counter, not a gauge.** The breaker is open for only 5 s and Prometheus
  scrapes every 15 s, so the state gauge can miss it. The drill uses `circuit_breaker_trips_total`.
- **"Pool exhausted" can also mean "slow to connect".** Once on an idle system, right after
  Postgres had restarted, a request failed with this error. My best explanation is that opening a
  new connection took longer than 0.5 s. I didn't prove it, so the error message now includes
  `in_use/max` to tell the two cases apart next time.
