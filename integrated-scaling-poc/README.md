# Phase 3: Integrated Multi-Topic Auto-Scaling POC

This directory contains the final architectural masterpiece: **Fully Automated Zero-Downtime EKS Autoscaling across Dynamic GCP Subscriptions.**

## The Architecture
We combined the **EKS ConfigMap Reloader POC** with the **KEDA Auto-Scaling POC**.

1. **The App (`consumer/app.py`)**: An asynchronous python worker that mounts a `ConfigMap` JSON array. It dynamically spins up a parallel gRPC background thread for *every* GCP topic listed in that array. Crucially, it uses strict `try/except` graceful degradation: if one topic stream receives a bad message, the app NACKs it back to GCP (or a DLQ) without crashing the pod, leaving the other streams completely unaffected.
2. **The Orchestrator (`lambda_autoscaler.py`)**: A purely automated AWS Lambda script. It executes a strict transactional EKS API patch. First, it completely rewrites the KEDA ScaledObject `triggers` array to encompass the new list of active topics. If KEDA succeeds, it patches the `ConfigMap`.
3. **The Result**: Stakater-Reloader sees the ConfigMap mutate, and triggers a rolling zero-downtime restart of your pods. The pods boot up, read the new configuration, and begin draining the new topics. KEDA is already waiting and actively auto-scaling all of them simultaneously!

---

## Execution Guide

### 1. Preparation & Build
*We assume your EKS cluster is running, Stakater-Reloader and KEDA are installed, and you have configured the `kubernetes/keda-auth.yaml` secret from Phase 2.*

Build the updated Phase 3 multi-topic consumer image and push to ECR:
```bash
cd consumer
export AWS_PROFILE=terraform-firehose 
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 487500748616.dkr.ecr.us-east-1.amazonaws.com
docker buildx build --platform linux/amd64 -t 487500748616.dkr.ecr.us-east-1.amazonaws.com/keda-multi-topic-worker:latest --push .
```

### 2. Base Deployment
Apply the foundational Kubernetes manifests into your EKS cluster:
```bash
kubectl apply -f kubernetes/keda-auth.yaml
kubectl apply -f kubernetes/configmap.yaml
kubectl apply -f kubernetes/deployment.yaml
kubectl apply -f kubernetes/scaledobject.yaml
```
*Run `kubectl get pods`. You should see 0 replicas.*

### 3. The Orchestration Test (Lambda)
Open a terminal locally on your Mac. We will run the Lambda script locally using your active AWS CLI credentials (no need to actually deploy it to AWS Lambda right now).

Open `lambda_autoscaler.py` in your IDE.
Uncomment the two extra dummy topics in the `DYNAMIC_TOPIC_SUBSCRIPTIONS` array at the top of the file so it looks like this:
```python
DYNAMIC_TOPIC_SUBSCRIPTIONS = [
    "projects/wired-sign-858/subscriptions/keda-poc-sub",
    "projects/wired-sign-858/subscriptions/another-sub-1",
    "projects/wired-sign-858/subscriptions/another-sub-2"
]
```

Run the orchestrator:
```bash
source .venv/bin/activate
export EKS_CLUSTER_NAME="gcp-sync-poc-test"
python lambda_autoscaler.py
```

### 4. Verification
Keep your AWS CloudShell open watching your pods and KEDA objects:
```bash
kubectl get pods -w
kubectl get scaledobject gcp-multi-topic-scaler -o yaml
```
You will instantly see the magic unfold:
1. The KEDA YAML will abruptly change, listing all 3 topics dynamically.
2. The initial pod deployment will attempt to roll over.
3. If you run the Phase 2 `publisher.py` to fill the old GCP topic, KEDA will *still* successfully auto-scale the newly restarted pods using the dynamic triggers array!
