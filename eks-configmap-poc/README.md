# EKS ConfigMap Live-Reloading Proof of Concept (POC)

This directory contains a standalone Proof of Concept to validate the automated AWS Lambda -> EKS API -> ConfigMap -> Stakater Reloader -> Python Pod rolling restart lifecycle, completely isolated from GCP and KEDA.

## Components

1.  **`app.py`**: A lightweight Python application that infinitely loops and prints the contents of a JSON file mounted at `/app/config/topics.json`. It gracefully catches `SIGTERM` signals for clean shutdowns.
2.  **`Dockerfile`**: A minimal image definition to containerize `app.py`.
3.  **`kubernetes.yaml`**: The native EKS definitions, including the `ConfigMap` payload, the `VolumeMount` mapping, and the crucial `reloader.stakater.com/auto: "true"` annotation on the Deployment.
4.  **`lambda_poc_patcher.py`**: A specialized, standalone version of the production Lambda script. It authenticates to your EKS cluster using local AWS SSO/IAM credentials and injects a "dummy" array of new topics directly into the ConfigMap via the EKS REST API.

---

## Execution Guide

### Phase 1: Deploy the Sandbox Pod

1.  **Build and Push the Image**:
    Build the Dockerfile and push it to your AWS ECR registry.
    ```bash
    docker build -t gcp-sync-poc:latest .
    # Push to your ECR...
    ```
2.  **Update the Kubernetes Manifest**:
    Edit `kubernetes.yaml` (Line 36) and replace the placeholder `image:` tag with your actual ECR URI.
3.  **Apply to EKS**:
    ```bash
    kubectl apply -f kubernetes.yaml
    ```
4.  **Observe the Baseline**:
    Open a terminal and tail the logs of the newly created pod.
    ```bash
    kubectl logs -f deployment/gcp-sync-streaming-deployment -n data-ingestion
    ```
    *Expected Output:* You should see the python app printing the single default topic (`projects/poc-project/subscriptions/order-stream`) every 10 seconds. Keep this terminal open!

### Phase 2: Execute the Lambda API Patch

1.  **Configure Environment**:
    Open a *second* terminal. Ensure you are authenticated to your AWS environment (e.g., `aws sso login`) so the `boto3` script can generate an EKS STS token.
    Export the name of your EKS cluster:
    ```bash
    export EKS_CLUSTER_NAME="my-dev-eks-cluster"
    ```
    *(Note: Ensure `EKS_REGION` and `NAMESPACE` inside the script match your environment).*
2.  **Run the Patcher**:
    ```bash
    python lambda_poc_patcher.py
    ```
    *Expected Output (Terminal 2):* The script will print that it generated the STS token, discovered the EKS Endpoint URL, constructed the JSON `PATCH` payload with the dummy topics (`NEW-test-topic-1` and `NEW-test-topic-2`), and successfully received an HTTP 200 response from the EKS Control Plane.

### Phase 3: Observe the Zero-Downtime Rolling Restart

Return to **Terminal 1** where the `kubectl logs -f` stream is running.

1.  **The Termination Event**:
    Almost immediately after the Lambda finishes patching the API, Stakater Reloader will detect the `MODIFIED` ConfigMap event inside the cluster. You will see the logs output:
    ```text
    [Shutdown] Received SIGTERM signal 15. Simulating graceful shutdown...
    [Shutdown] Clean exit completed.
    ```
    *(The `kubectl logs -f` stream will likely disconnect at this point because the Reloader controller has terminated the underlying container).*
2.  **The Re-Spawn**:
    Reloader instantly spins up a replacement pod. Run the log tailing command again:
    ```bash
    kubectl logs -f deployment/gcp-sync-streaming-deployment -n data-ingestion
    ```
3.  **Validation**:
    You will now see the *new* python application booting. It is reading from the exact same physical mount path (`/app/config/topics.json`), but because `kubelet` updated the file dynamically based on the Lambda's API payload, you will see the logs output:
    ```text
    [Active] Found 3 topics in config file:
      -> projects/poc-project/subscriptions/order-stream
      -> projects/poc-project/subscriptions/NEW-test-topic-1
      -> projects/poc-project/subscriptions/NEW-test-topic-2
    ```

**Success!** This proves that a Lambda securely firing JSON payloads at an EKS API endpoint can seamlessly trigger zero-downtime, file-based configuration reloads for thousands of dynamically scaled Python workloads.
