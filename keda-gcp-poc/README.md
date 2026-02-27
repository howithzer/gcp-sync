# KEDA + GCP Pub/Sub Auto-Scaling POC

This directory contains the code and Kubernetes manifests needed to test KEDA (Kubernetes Event-driven Autoscaling) directly integrating with GCP Pub/Sub metrics from inside an AWS EKS cluster.

## Architecture
1. **Publisher (`publisher.py`)**: Runs locally. Rapidly fires 1,000 JSON messages to a GCP Topic to simulate a sudden, massive traffic spike.
2. **Consumer (`consumer/app.py`)**: Runs in an EKS Pod. Pulls messages using the GCP Python SDK. It introduces an artificial 1-second delay per message to guarantee it cannot keep up with the publisher, creating a backlog.
3. **KEDA (`kubernetes/scaledobject.yaml`)**: Runs in the EKS Control Plane. Hits the GCP Metrics API every 10 seconds. When it sees the 1,000 message backlog created by the Publisher, it scales the Consumer Deployment from 0 to 20 replica pods instantly.

---

## Execution Guide

### Phase 1: Preparation

1. **GCP Configuration**:
   - Ensure you have a GCP Pub/Sub Topic (`keda-poc-topic`) and Subscription (`keda-poc-sub`).
   - Create a Service Account JSON Key with the `roles/pubsub.viewer` & `roles/pubsub.subscriber` roles.
   - Base64 encode the JSON key file containing no newlines: 
     ```bash
     base64 -i my-gcp-key.json | tr -d '\n'
     ```
   - Paste the encoded string into `kubernetes/keda-auth.yaml`.
   - Update `kubernetes/deployment.yaml` and `kubernetes/scaledobject.yaml` with your real GCP Project ID and Subscription name if they differ from the defaults.

2. **Docker Build**:
   - Navigate to the `consumer/` directory.
   - Build the Dockerfile and push it to your AWS ECR registry.
   - Update `kubernetes/deployment.yaml` with the ECR image URI.

### Phase 2: Deployment

Run these from your AWS CloudShell connected to the EKS cluster. AWS CloudShell does not have `helm` installed by default.

**Option A: Install KEDA via Raw YAML (Easiest)**
```bash
kubectl apply --server-side -f https://github.com/kedacore/keda/releases/download/v2.16.1/keda-2.16.1.yaml
```

**Option B: Install Helm on CloudShell and deploy chart**
```bash
curl -fsSL -o get_helm.sh https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3
chmod 700 get_helm.sh
./get_helm.sh

helm repo add kedacore https://kedacore.github.io/charts
helm repo update
helm install keda kedacore/keda --namespace keda --create-namespace
```

2. **Deploy the POC**:
```bash
kubectl apply -f kubernetes/keda-auth.yaml
kubectl apply -f kubernetes/deployment.yaml
kubectl apply -f kubernetes/scaledobject.yaml
```

3. **Verify Scale-To-Zero**:
Check your running pods. 
```bash
kubectl get pods
```
Because KEDA defaults the deployment to `replicas: 0`, and the GCP topic currently has 0 waiting messages, **no pods should be running.** KEDA has suspended the deployment.

### Phase 3: The Load Test

Now, we trigger the autoscale event! Keep your EKS cluster open in one terminal monitoring exactly what KEDA does:
```bash
kubectl get pods -w
```

Open a second terminal on your local laptop, navigate to this directory, install the requirements, and blast the GCP Topic:
```bash
pip install google-cloud-pubsub
python publisher.py
```

### The Expected Result
1. The Publisher will instantly inject 1,000 messages into your GCP Topic.
2. Within 10 to 30 seconds, the KEDA controller running inside your EKS cluster will ping the GCP Metrics API using your Service Account JSON key.
3. KEDA will calculate the math: `1000 messages / 50 messages per pod = 20 pods required`.
4. In your first terminal running `kubectl get pods -w`, you will suddenly see Kubernetes violently spin up 20 `poc-consumer` replica pods simultaneously!
5. As the 20 pods grab messages from the queue (sleeping 1 second per message), the backlog will drop.
6. Check `kubectl get hpa`. It will dynamically track the GCP metric dropping in real-time.
7. Once the 1,000 messages are processed, KEDA will wait 60 seconds (the `cooldownPeriod`), and safely terminate all 20 pods, scaling the cluster back down to 0!
