# Load test against the dev environment, then show what the anomaly detector recorded.
param(
    [string]$TargetHost = "http://127.0.0.1:30080",
    [int]$Users = 50,
    [string]$Duration = "60s"
)

Write-Host "=== Load test: $Users users for $Duration against $TargetHost ===" -ForegroundColor Cyan
locust -f "$PSScriptRoot/locustfile.py" --headless -u $Users -r 10 --run-time $Duration --host $TargetHost --only-summary

Write-Host "`n=== Waiting 20s for logs to flow through Kafka and the anomaly detector ===" -ForegroundColor Cyan
Start-Sleep -Seconds 20

Write-Host "`n=== Latest anomaly alerts (dev) ===" -ForegroundColor Cyan
kubectl exec -n dev statefulset/postgres -- sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT detected_at, error_count, total_count, round(error_rate::numeric, 2) AS error_rate FROM anomaly_alerts ORDER BY id DESC LIMIT 5;"'

Write-Host "`nOpen Grafana at http://127.0.0.1:30300 to follow a failing request from metrics to logs to traces." -ForegroundColor Green
