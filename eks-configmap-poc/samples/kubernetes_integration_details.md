# Kubernetes Integration Details: APIs, ConfigMaps, and Pods

This document explains the mechanism by which a Lambda patching the EKS API translates into a physical file update inside a running Pod, and how that integrates seamlessly with `stakater/Reloader` without the Lambda needing to know about EFS or the pod's filesystem.

## 1. The API Patch (AWS Lambda -> EKS Control Plane)

The Lambda script does **not** write a file to a disk or EFS. 

Instead, it sends a JSON HTTP `PATCH` payload directly to the **EKS Control Plane API**. It essentially says:
> "Hey Kubernetes, please update the ConfigMap object named `gcp-sync-streaming-configmap` with this new JSON array."

The EKS Control Plane validates this request, authorizes the IAM token, and saves the updated JSON array into its internal state database (`etcd`).

## 2. The Volume Mount (EKS Control Plane -> EC2 Worker Node)

Because the streaming `deployment.yaml` contains a **VolumeMount** referencing the ConfigMap, the `kubelet` agent running on your EKS worker nodes is actively watching the cluster state for changes to that ConfigMap.

When `kubelet` sees the ConfigMap update:
- It takes the raw JSON data out of the ConfigMap object.
- It automatically creates a physical file on the EC2 Node's underlying filesystem.
- It mounts that file directly into your Python Pod's container at the specified `mountPath` (e.g., `/app/config/topics.json`).

## 3. The Python Application (Pod -> File System)

The Python application inside the pod (e.g., `multi_topic_example.py`) has **no idea** that Kubernetes, ConfigMaps, or Lambdas exist. It does not need the AWS SDK or the Kubernetes API Client.

It simply reads from the local file system using standard Python I/O operations:

```python
with open("/app/config/topics.json", 'r') as f:
    config_data = json.load(f)
```

As far as the code is concerned, it is just reading a local JSON file that magically appeared.

## 4. The Rolling Restart (Stakater Reloader)

While `kubelet` updates the file under the hood, our Python application is configured to only read the config file *once* on startup to initialize its concurrent GCP Subscriptions. It doesn't continuously poll the file (which avoids locking issues and CPU waste).

This is where **Stakater Reloader** ties everything together:

1. Reloader runs in the cluster and listens to the Kubernetes API for `MODIFIED` events on any ConfigMap.
2. When the Lambda updates the ConfigMap, Reloader intercepts the event.
3. Reloader safely sends a `SIGTERM` signal to the old version of the Python Pod.
4. The Python Pod catches the signal and gracefully drains or `nack()`s active messages to ensure no data is lost.
5. Reloader finishes terminating the pod and boots up a new Pod instance to replace it.
6. Upon startup, the *new* Pod reads the *newly updated* file from `kubelet` containing the newly patched topics.

This end-to-end flow achieves perfectly dynamic configuration updates across thousands of pods solely through native Kubernetes API objects—no external databases, EFS volumes, or complex file-locking required.
