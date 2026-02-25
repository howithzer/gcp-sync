# Architecture Proposal: GCP Pub/Sub to AWS SQS Sync via KEDA on EKS

This document explores the feasibility, design, and pros/cons of a unified sync architecture where a single Kubernetes Deployment (Pod) on EKS consumes from multiple GCP Pub/Sub subscriptions (using the StreamingPull API) and forwards messages to AWS SQS, scaling dynamically via KEDA.

## 1. Feasibility Assessment
**Is it feasible? Yes.**
- **Single Pod / Multiple Subscriptions**: The Google Cloud Python/Go SDKs are highly asynchronous. A single application can easily open `StreamingPull` connections to dozens of subscriptions concurrently using background threads or async tasks.
- **KEDA Scaling**: KEDA supports the `gcp-pubsub` scaler. KEDA can scale a single deployment based on the queue depth of *multiple* subscriptions by defining multiple triggers in a single `ScaledObject`. The HPA will calculate the metric for each trigger and scale the deployment based on the maximum required replica count among them.
- **Dynamic Topic Reloading**: This is the main challenge. Since the goal is "no new infrastructure services," the pods need a way to discover which topics to consume without requiring a new database or service.

## 2. Options for Dynamic Subscription Discovery

The proposed workflow is: A scheduled Lambda runs periodically (e.g., every 4 hours), fetches the active list of topics, and writes them to a centralized state. The pods then react to this state change.

### Option A: ConfigMap + Reloader (Rolling Restart) **[User Preference]**
The Lambda updates a Kubernetes `ConfigMap` via the Kubernetes API. You install [stakater/Reloader](https://github.com/stakater/Reloader) on the EKS cluster, which watches the ConfigMap and triggers a rolling update of your Deployment when the data changes.
- **How it works**: The Python app is simple. It reads the config file *once* on startup, creates `StreamingPull` futures for those topics, and runs forever.
- **Pros**: 
  - Extremely simple application code (no dynamic thread management).
  - Guarantees a clean slate and avoids memory leaks.
- **Cons / Risks**: 
  - **Duplicate Processing**: When pods are terminated during a rolling restart, any active `StreamingPull` connections are severed. If messages were pulled but not yet acknowledged (processed and submitted to SQS), GCP Pub/Sub will redeliver them to the new pods. Your pipeline *must* be strictly idempotent.
  - **Scale Churn**: If KEDA has scaled your deployment to 50 pods, Reloader will execute a rolling restart across all 50 pods. This can cause temporary backlog spikes during the transition.

### Option B: EFS or ConfigMap + File Watcher (Dynamic Reloading)
The Lambda writes the topic list to a shared EFS volume (or a ConfigMap mounted as a volume). The Python application uses a library like `watchdog` to monitor the file for changes.
- **How it works**: When the file changes, the Python app parses the new list. It identifies *new* topics and spawns new `StreamingPull` tasks for them. It identifies *removed* topics and gracefully cancels their specific `StreamingPull` futures.
- **Pros**: 
  - Zero pod restarts. Existing topics continue processing without interruption.
  - No surge of unacknowledged, redelivered messages.
- **Cons**: 
  - More complex Python codebase. You have to write a reliable async manager that can cancel specific tasks without tearing down the whole application.
  - (If using EFS) Managing EFS CSI drivers and networking overhead for a tiny config file might be overkill compared to a native ConfigMap. *Note: Kubernetes natively updates mounted ConfigMap files automatically (usually takes 1-2 minutes). A file watcher works perfectly on a mounted ConfigMap without needing EFS.*

## 3. High-Level Architecture Model

```text
┌─────────────────────────┐         ┌──────────────────────────────────────────────────┐        ┌───────────────────┐
│       GCP Pub/Sub       │         │                    AWS EKS                       │        │      AWS SQS      │
│                         │         │                                                  │        │                   │
│  [Sub: Topic_A_Stream] ─┼────────►│  ┌────────────────────────────────────────────┐  │───────►│  [Queue: Topic_A] │
│                         │         │  │ Deployment: gcp-sqs-sync-streaming         │  │        │                   │
│  [Sub: Topic_B_Stream] ─┼────────►│  │  ├─ Thread 1: StreamingPull(Topic_A)       │  │───────►│  [Queue: Topic_B] │
│                         │         │  │  ├─ Thread 2: StreamingPull(Topic_B)       │  │        │                   │
│                         │         │  │  └─ Thread N: FileWatcher (ConfigMap)      │  │        │                   │
│                         │         │  └────────────────────────────────────────────┘  │        │                   │
│                         │         │                        ▲                         │        │                   │
│                         │         │                        │                         │        │                   │
│                         │         │  ┌────────────────────────────────────────────┐  │        │                   │
│                         │         │  │ KEDA ScaledObject                          │  │        │                   │
│                         │ ◄───────┼──│  ├─ Trigger 1: GCP Sub Topic_A Backlog     │  │        │                   │
│                         │         │  │  └─ Trigger 2: GCP Sub Topic_B Backlog     │  │        │                   │
│                         │         │  └────────────────────────────────────────────┘  │        │                   │
└─────────────────────────┘         └──────────────────────────────────────────────────┘        └───────────────────┘
```

## 4. Pros and Cons of this Architecture

### Pros (Why it's a good idea)
1. **Resource Efficiency**: Instead of 50 pods for 50 low-volume topics, you can run 2-3 pods that handle all 50. This drastically reduces EKS node overhead, idle CPU/Mem requests, and IP address consumption.
2. **Simplified CI/CD**: You only deploy one application image and one Helm chart. Adding a topic is a data-configuration change, not a new application deployment.
3. **No New Infra**: Entirely leverages existing EKS, KEDA, ConfigMaps, and native cloud SDKs.
4. **Logical Grouping**: You can deploy one release for `streaming-fast` and a separate deployment for `batch-slow` by just passing a different config file to each.

### Cons (Risks to mitigate)
1. **Poison Pills & Blast Radius**: If Topic A receives heavily malformed data that causes the Python process to crash (OOM or unhandled exception), the pod restarts. This interrupts consumption for Topic B, C, and D as well. 
   - *Mitigation*: Extremely defensive exception handling per-thread.
2. **KEDA Scaling Imbalance**: KEDA calculates "scale" based on the max metric. If Topic A has a backlog of 1 million, KEDA might scale the deployment to 50 pods. ALL 50 pods will then connect to Topic A, *and* Topic B, *and* Topic C. Topic B might only have 10 messages, but it will have 50 active StreamingPull connections polling it.
   - *Mitigation*: GCP PubSub distributes messages fairly. It's not a dealbreaker, but it causes unnecessary outbound connections.
3. **Connection Limits**: GCP limits the number of concurrent streaming pull connections per project/subscription. For thousands of topics across hundreds of pods, you might hit API quotas.

## 5. Next Steps for Implementation
If we want to proceed and build a prototype in this `gcp-sync` folder, we would need to:
1. Write the Python codebase using `google-cloud-pubsub` and `boto3`.
2. Implement an `asyncio` or ThreadPool based manager that loads a local JSON/YAML config.
3. Implement a dynamic reload mechanism (e.g., watching `config.yaml` for changes).
4. Simulate the KEDA and ConfigMap kubernetes manifests for deployment.

Let's discuss! Does Option A (ConfigMap) fit your operational model, and do you want to look at the codebase you currently have to see how we can adapt it for multi-topic concurrency?
