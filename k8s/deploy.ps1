param (
    [string]$Overlay = "dev"
)

# Build docker images
docker build -t service-a:latest -f services/service_a/Dockerfile .
docker build -t service-b:latest -f services/service_b/Dockerfile .
docker build -t service-c:latest -f services/service_c/Dockerfile .
docker build -t anomaly-detector:latest -f services/anomaly_detector/Dockerfile .

# If minikube is active, load images into cluster
if (Get-Command minikube -ErrorAction SilentlyContinue) {
    minikube image load service-a:latest service-b:latest service-c:latest anomaly-detector:latest
}

# Apply chosen overlay
Write-Host "Applying k8s overlay: $Overlay..."
kubectl apply -k "k8s/overlays/$Overlay"
kubectl get pods,svc -n $Overlay
