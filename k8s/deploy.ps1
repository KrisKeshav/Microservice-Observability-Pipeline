# Build docker images
docker build -t service-a:latest -f services/service_a/Dockerfile .
docker build -t service-b:latest -f services/service_b/Dockerfile .
docker build -t service-c:latest -f services/service_c/Dockerfile .
docker build -t anomaly-detector:latest -f services/anomaly_detector/Dockerfile .

# If minikube is active, load images into cluster
if (Get-Command minikube -ErrorAction SilentlyContinue) {
    minikube image load service-a:latest service-b:latest service-c:latest anomaly-detector:latest
}

# Apply Kubernetes manifests
kubectl apply -k k8s/
kubectl get pods,svc
