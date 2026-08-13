# Full-Pipeline Kubernetes Load Test Automation

$targetHost = "http://127.0.0.1:30080"
$users = 100
$spawnRate = 20
$duration = "45s"

Write-Host "=== Checking Kubernetes Cluster Health ===" -ForegroundColor Cyan
if (Get-Command kubectl -ErrorAction SilentlyContinue) {
    $pods = kubectl get pods --no-headers 2>&1
    Write-Host "$pods"
} else {
    Write-Host "kubectl not found in PATH, skipping pod status check." -ForegroundColor Yellow
}

Write-Host "`n=== Launching Locust Load Test ($users Users against $targetHost) ===" -ForegroundColor Cyan
Push-Location "$PSScriptRoot"
try {
    locust -f locustfile.py --headless -u $users -r $spawnRate --run-time $duration --host $targetHost
} catch {
    Write-Host "Failed to run locust command. Ensure locust is installed in python environment." -ForegroundColor Red
}
Pop-Location

Write-Host "`n=== Waiting 10s for Telemetry Pipeline & Kafka Consumers to Flush ===" -ForegroundColor Cyan
Start-Sleep -Seconds 10

Write-Host "`n=== Checking Database Anomaly Alerts (PostgreSQL) ===" -ForegroundColor Cyan
if (Get-Command kubectl -ErrorAction SilentlyContinue) {
    try {
        kubectl exec deploy/postgres -- psql -U orders -c "SELECT id, detected_at, error_count, total_count, error_rate FROM anomaly_alerts ORDER BY id DESC LIMIT 5;"
    } catch {
        Write-Host "Could not query anomaly_alerts table directly via kubectl." -ForegroundColor Yellow
    }
}

Write-Host "`n=== Load Test Complete ===" -ForegroundColor Green
Write-Host "Open Grafana at http://127.0.0.1:30300 to inspect correlated logs and Jaeger traces." -ForegroundColor Green
