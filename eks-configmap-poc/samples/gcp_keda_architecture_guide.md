# Auto-Scaling GCP Pub/Sub to AWS SQS Sync Architecture

This document formally details the architecture, deployment lifecycle, and operational sequence for syncing data from Google Cloud Platform Pub/Sub to Amazon SQS using an Event-Driven, Auto-Scaling Kubernetes deployment on AWS EKS.

## System Components

1.  **GCP Pub/Sub**: The source of the streaming and batch event data.
2.  **AWS SQS**: The target destination for the flattened payloads.
3.  **Amazon EKS (Elastic Kubernetes Service)**: The runtime environment for the python synchronization pods.
4.  **KEDA (Kubernetes Event-driven Autoscaling)**: An operator that scales EKS Deployments based on external metrics (specifically, the backlog size of the GCP subscriptions).
5.  **Stakater Reloader**: A Kubernetes controller that watches for changes in ConfigMaps and triggers graceful rolling restarts of Deployments relative to those configs.
6.  **AWS Lambda (Configurator)**: A scheduled serverless function that acts as the "control plane." It periodically queries GCP for active subscriptions, groups them by business logic, and dynamically patches the EKS ConfigMaps and KEDA ScaledObjects.
7.  **Azure DevOps (ADO)**: The CI/CD pipeline responsible for the initial provisioning of the EKS resources via Helm.

---

## Operations Lifecycle

### Day 0: Initial Rollout (ADO Pipeline)

On Day 0, the infrastructure does not yet know which topics to consume. The focus is strictly on deploying the raw Kubernetes resources.

```mermaid
sequenceDiagram
    participant ADO as Azure DevOps (Helm)
    participant EKS as Amazon EKS API
    participant POD as Sync Pods

    Note over ADO, POD: Day 0: Baseline Infrastructure Provisioning

    ADO->>EKS: Helm Upgrade --install
    Note right of ADO: Contains Deployment, initial bare ConfigMap,<br/>and initial empty KEDA ScaledObject
    
    EKS-->>EKS: Save ConfigMap (topics: [])
    
    EKS->>EKS: Create Deployment (replicas: 1)
    Note right of EKS: Deployment has 'reloader.stakater.com/auto' annotation
    
    EKS->>POD: Schedule Initial Pod
    POD->>POD: Mount ConfigMap
    POD-->>POD: "No topics found. Awaiting updates."
```

---

### Day 1: Dynamic Topic patching (Lambda discovering new workloads)

On Day 1 (and repeatedly every `X` hours), the active Lambda scheduler executes. It identifies any newly created GCP subscriptions or removed subscriptions that match the expected prefix rules. It then patches the configuration directly via the Kubernetes REST API. This triggers the rolling restart.

```mermaid
sequenceDiagram
    participant GCP as GCP Pub/Sub API
    participant LMD as AWS Lambda (Control Plane)
    participant EKS as Amazon EKS API
    participant RL as Stakater Reloader
    participant POD as Python Sync Pods

    Note over GCP, POD: Day 1: Subscription Discovery & Configuration Patching
    
    LMD->>GCP: GET List Subscriptions
    GCP-->>LMD: ["topic-a-stream", "topic-b-stream"]
    
    LMD->>LMD: Filter & Group
    
    LMD->>EKS: PATCH ConfigMap <br/>(data: ["topic-a-stream", "topic-b-stream"])
    LMD->>EKS: PATCH KEDA ScaledObject <br/>(triggers: [topic-a, topic-b])
    
    par KEDA Update
        EKS-->>EKS: KEDA Controller registers new Subscriptions to monitor
    end
    
    par ConfigMap Update
        EKS-->>RL: Firing: ConfigMap Modified Event
        RL->>EKS: Trigger graceful Rolling Restart of Deployment
        
        EKS->>POD: SIGTERM sent to Old Pods
        Note right of POD: Old Pods execute clean NACK on partial messages<br/>and cancel current futures
        
        EKS->>POD: Start New Pods
        POD->>POD: Mount updated ConfigMap
        
        POD->>GCP: subscriber.subscribe("topic-a-stream")
        POD->>GCP: subscriber.subscribe("topic-b-stream")
        Note right of POD: Python app processes multiple streams concurrently
    end
```

---

### Day 2: Auto-Scaling Under Lasting Load (KEDA Math)

On Day 2, the workloads are active and topics are being processed. Suddenly, a massive spike of traffic hits `topic-a-stream`. KEDA recognizes this load across the grouping and scales the pods out. Because GCP handles fair-share streaming pull mechanics, the new pods immediately offload `topic-a` traffic without impacting `topic-b`.

```mermaid
sequenceDiagram
    participant GCP as GCP Pub/Sub (Metrics)
    participant KEDA as KEDA Controller
    participant HPA as EKS HPA (Autoscaler)
    participant EKS as Amazon EKS API
    participant POD as Sync Pods
    
    Note over GCP, POD: Day 2: Dynamic Auto-Scaling (Periodic every 30s)

    loop Metrics Reconciliation
        KEDA->>GCP: Query active backlog for "topic-a-stream"
        GCP-->>KEDA: Backlog: 25,000 messages
        
        KEDA->>GCP: Query active backlog for "topic-b-stream"
        GCP-->>KEDA: Backlog: 15 messages
        
        KEDA->>KEDA: Calculate Topic A Needed Pods: 50 (Max metric hit)
        KEDA->>KEDA: Calculate Topic B Needed Pods: 1
        
        KEDA->>KEDA: Math: MAX(50, 1) = Target 50 Pods
        
        KEDA->>HPA: Update External Metric (Target 50)
        HPA->>EKS: Patch Deployment Replicas to 50
        
        EKS->>POD: Launch 45 new identical Pods
        Note right of POD: Each new pod connects to BOTH Topic A and Topic B. GCP routes the 25k queue round-robin to all 50 pods.
    end
```

## Resilience, Limits, and Fault Isolation

As you consolidate workloads into shared deployment pods, protecting against "noisy neighbors" or poison-pill events becomes critical. Here is how this architecture handles constraints:

### 1. Fault Isolation (Segregation by Grouping)
- **Blast Radius Mitigation**: By segmenting topics into distinct groups (e.g., `streaming` vs `batch` vs `critical-tier-1`), you logically isolate failures. A malformed message (poison pill) crashing a pod in the `batch` deployment will completely disrupt all batch topics temporarily—but it will **zero impact** on the `critical-tier-1` deployment pods.
- **Poison Pill Handling**: The Python application must be wrapped in heavy `try/except` blocks (as seen in the `multi_topic_example.py`). If a specific message cannot be parsed, the Python app should `nack()` the message or forward it to a Dead Letter Queue (DLQ) without crashing the unified pod.

### 2. Resource Limits & Throttling
- **Memory (OOM) Management**: KEDA natively scales horizontally to prevent single-pod OOMs, but you must still define strictly bounded Kubernetes Resource Limits (e.g., `resources: limits: memory: 2Gi`) on the Deployment. The Python pub/sub client buffers messages locally; if it outpaces SQS, the pod could run out of memory. 
- **Backpressure via Flow Control**: The GCP Python SDK supports `flow_control`. You should implement limits (e.g., `max_messages=1000` or `max_bytes=50MB`) on the GCP subscriber client. The pod will automatically stop requesting messages from GCP if the local buffer hits this size, completely eliminating OOM risks regardless of how fast GCP can send data.

### 3. Scaling Constraints
- **Maximum API Quotas**: KEDA solves under-scaling, but runaway scaling can exhausted downstream APIs or VPC IP space. You control this heavily at the `ScaledObject` level using `maxReplicaCount`. For example, setting `maxReplicaCount: 50` guarantees the HPA will never expand the deployment beyond 50 pods, no matter how large the GCP backlog grows.
- **Fair-Share Hashing**: If Topic A has 10 Million messages and Topic B has 500, capping the max replicas ensures all pods stay active. GCP native load balancing uses fair-distribution; it will continue serving Topic B messages alongside Topic A's flood to all 50 active pods simultaneously.

---

## Summary of Responsibilities

- **DevOps/Helm**: Ensure the `reloader.stakater.com/auto` annotation is present on the unified Deployment template.
- **Python Engineer**: Ensure the python application catches `SIGTERM` and safely cancels or `nack()`s active processing messages so Reloader's rolling restart doesn't drop messages.
- **Platform Engineer (Lambda)**: Ensure the Lambda script creates a 1:1 parity between what it writes to the ConfigMap array and what `gcp-pubsub` Triggers it appends to the ScaledObject.
