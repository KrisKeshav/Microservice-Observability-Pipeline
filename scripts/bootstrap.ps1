<#
.SYNOPSIS
  One-time cluster setup: namespaces, generated secrets, and the Argo CD applications.

.DESCRIPTION
  Secrets are generated here and created directly in the cluster, so no password is ever
  committed to Git. Re-running is safe: existing secrets are kept (Postgres only reads its
  password on first start, so regenerating it would lock the apps out of the database).

.EXAMPLE
  .\scripts\bootstrap.ps1
  .\scripts\bootstrap.ps1 -Environments dev,prod -SlackWebhookUrl https://hooks.slack.com/services/...
  .\scripts\bootstrap.ps1 -SkipArgoCD    # secrets only, e.g. before a manual kubectl apply -k
#>
param(
    [string[]]$Environments = @("dev"),
    [string]$SlackWebhookUrl = "",
    [switch]$SkipArgoCD
)

$ErrorActionPreference = "Stop"

function New-RandomPassword {
    $bytes = New-Object byte[] 24
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    # URL-safe so it can sit inside DATABASE_URL without escaping.
    return [Convert]::ToBase64String($bytes).Replace("+", "-").Replace("/", "_").TrimEnd("=")
}

function Test-Secret([string]$Name, [string]$Namespace) {
    kubectl get secret $Name -n $Namespace --ignore-not-found -o name | Select-String -Quiet $Name
}

Write-Host "Cluster context: $(kubectl config current-context)" -ForegroundColor Cyan

foreach ($ns in $Environments) {
    kubectl create namespace $ns --dry-run=client -o yaml | kubectl apply -f - | Out-Null

    if (Test-Secret "postgres-secret" $ns) {
        Write-Host "[$ns] secrets already exist - keeping them" -ForegroundColor Yellow
        continue
    }

    $dbPassword = New-RandomPassword
    $grafanaPassword = New-RandomPassword
    # Alertmanager needs a URL to start; without a real webhook, notifications fail harmlessly.
    $webhook = if ($SlackWebhookUrl) { $SlackWebhookUrl } else { "http://localhost:9/no-slack-configured" }

    kubectl create secret generic postgres-secret -n $ns `
        --from-literal=POSTGRES_USER=orders `
        --from-literal=POSTGRES_PASSWORD=$dbPassword `
        --from-literal=POSTGRES_DB=orders | Out-Null

    kubectl create secret generic app-secrets -n $ns `
        --from-literal=DATABASE_URL="postgresql://orders:$dbPassword@postgres:5432/orders" `
        --from-literal=GRAFANA_ADMIN_USER=admin `
        --from-literal=GRAFANA_ADMIN_PASSWORD=$grafanaPassword `
        --from-literal=SLACK_WEBHOOK_URL=$webhook | Out-Null

    Write-Host "[$ns] secrets created. Grafana login: admin / $grafanaPassword" -ForegroundColor Green
}

if ($SkipArgoCD) {
    Write-Host "Skipping Argo CD applications." -ForegroundColor Yellow
} elseif (kubectl get namespace argocd --ignore-not-found -o name) {
    foreach ($ns in $Environments) {
        kubectl apply -f "k8s/argocd/$ns-application.yaml"
    }
    Write-Host "Argo CD will now sync from GitHub. Watch it with: kubectl get applications -n argocd -w" -ForegroundColor Cyan
} else {
    Write-Host "Argo CD is not installed. Install it with:" -ForegroundColor Yellow
    Write-Host "  kubectl create namespace argocd"
    Write-Host "  kubectl apply -n argocd -f https://raw.githubusercontent.com/argoproj/argo-cd/stable/manifests/install.yaml"
    Write-Host "then re-run this script."
}
