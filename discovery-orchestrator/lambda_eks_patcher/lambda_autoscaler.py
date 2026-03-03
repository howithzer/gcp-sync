import os
import json
import base64
import urllib.request
import urllib.error
import ssl
import boto3
from botocore.signers import RequestSigner

# -------------------------------------------------------------------
# Phase 4: Final Step Function Integration
# This Lambda script forces a transactional atomic update on the EKS cluster:
# 1. Updates KEDA Auto-Scaling Rules for the discovered topics.
# 2. Updates the Pod ConfigMap so the pods consume the topics.
# -------------------------------------------------------------------

EKS_CLUSTER_NAME = os.getenv("EKS_CLUSTER_NAME", "gcp-sync-poc-test")
EKS_REGION = os.getenv("EKS_REGION", "us-east-1")
NAMESPACE = os.getenv("NAMESPACE", "default")
CONFIGMAP_NAME = "gcp-multi-topic-configmap"
SCALEDOBJECT_NAME = "gcp-multi-topic-scaler"
DEPLOYMENT_NAME = "gcp-multi-topic-consumer"

def get_eks_token(cluster_name):
    """Generates AWS STS token for authenticating against EKS."""
    session = boto3.Session()
    client = session.client('sts', region_name=EKS_REGION)
    signer = RequestSigner(
        client.meta.service_model.service_id,
        EKS_REGION, 'sts', 'v4',
        session.get_credentials(), session.events
    )
    
    params = {
        'method': 'GET',
        'url': f'https://sts.{EKS_REGION}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15',
        'body': {},
        'headers': {'x-k8s-aws-id': cluster_name},
        'context': {}
    }
    
    signed_url = signer.generate_presigned_url(
        params, region_name=EKS_REGION, expires_in=60, operation_name=''
    )
    return 'k8s-aws-v1.' + base64.urlsafe_b64encode(signed_url.encode('utf-8')).decode('utf-8').rstrip('=')

def get_eks_cluster_info(cluster_name):
    """Retrieves the EKS Endpoint URL and base64 CA Certificate."""
    client = boto3.client('eks', region_name=EKS_REGION)
    cluster = client.describe_cluster(name=cluster_name)['cluster']
    return cluster['endpoint'], cluster['certificateAuthority']['data']

def call_eks_api(endpoint, ca_data, token, path, method, payload, content_type="application/merge-patch+json"):
    """Executes the REST HTTP request to the EKS Control Plane."""
    url = f"{endpoint}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": content_type
    }
    
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    
    ca_bytes = base64.b64decode(ca_data)
    ssl_context = ssl.create_default_context(cadata=ca_bytes.decode('utf-8'))
    
    response = urllib.request.urlopen(req, context=ssl_context, timeout=10)
    return json.loads(response.read().decode('utf-8'))

def patch_keda_autoscaler(endpoint, ca_data, token, topics):
    """
    Dynamically generates the KEDA Target Triggers list and patches the CRD.
    We always send the FULL triggers array — merge-patch replaces the array entirely,
    which is exactly what we want (rebuild from the current discovered topic list).
    KEDA CRDs only support: merge-patch+json, json-patch+json, apply-patchyaml.
    """
    triggers = []
    for topic in topics:
        triggers.append({
            "type": "gcp-pubsub",
            "metadata": {
                "subscriptionName": topic,
                "subscriptionSize": "50"
            },
            "authenticationRef": {
                "name": "gcp-keda-trigger-auth"
            }
        })
        
    payload = {"spec": {"triggers": triggers}}
    path = f"/apis/keda.sh/v1alpha1/namespaces/{NAMESPACE}/scaledobjects/{SCALEDOBJECT_NAME}"
    
    print(f"--> [STEP 1] Patching KEDA ScaledObject with {len(triggers)} triggers...")
    # KEDA CRDs accept merge-patch+json (not strategic-merge-patch)
    call_eks_api(endpoint, ca_data, token, path, "PATCH", payload,
                 content_type="application/merge-patch+json")
    print("--> [SUCCESS] KEDA Autoscaler updated.")

def patch_pod_configmap(endpoint, ca_data, token, topics):
    """
    Updates the ConfigMap. Core Kubernetes objects support strategic-merge-patch+json,
    which is proven working in the eks-configmap-poc.
    """
    payload = {"data": {"topics.json": json.dumps({"topics": topics})}}
    path = f"/api/v1/namespaces/{NAMESPACE}/configmaps/{CONFIGMAP_NAME}"
    
    print(f"--> [STEP 2] Patching ConfigMap '{CONFIGMAP_NAME}' with {len(topics)} topics...")
    call_eks_api(endpoint, ca_data, token, path, "PATCH", payload,
                 content_type="application/strategic-merge-patch+json")
    print("--> [SUCCESS] ConfigMap patched.")

def restart_deployment(endpoint, ca_data, token):
    """
    Triggers a rolling restart of the consumer Deployment — equivalent to:
      kubectl rollout restart deployment/gcp-multi-topic-consumer
    Patches the pod template annotation with the current UTC timestamp.
    Kubernetes detects the change and cycles all pods with the new ConfigMap content.
    No Stakater Reloader required!
    """
    import datetime
    now = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "spec": {
            "template": {
                "metadata": {
                    "annotations": {
                        "kubectl.kubernetes.io/restartedAt": now
                    }
                }
            }
        }
    }
    path = f"/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{DEPLOYMENT_NAME}"
    
    print(f"--> [STEP 3] Triggering rolling restart of Deployment '{DEPLOYMENT_NAME}'...")
    call_eks_api(endpoint, ca_data, token, path, "PATCH", payload,
                 content_type="application/strategic-merge-patch+json")
    print("--> [SUCCESS] Rolling restart triggered. Pods will now mount the new topic config.")


def _get_subscriptions(event):
    """Unwraps the Step Function Payload envelope and returns the subscriptions list."""
    effective = event.get('Payload', event)
    return effective.get('subscriptions', [])

def _connect_eks():
    """Authenticates to EKS and returns (endpoint, ca_data, token)."""
    endpoint, ca_data = get_eks_cluster_info(EKS_CLUSTER_NAME)
    token = get_eks_token(EKS_CLUSTER_NAME)
    return endpoint, ca_data, token

# ===========================================================================
# Handler 1: keda_handler
# Registered as: lambda_autoscaler.keda_handler
# Step Function State: PatchEKSKEDA
# Only patches the KEDA ScaledObject. Passes subscriptions forward so the
# Step Function Wait state can relay them to configmap_handler.
# ===========================================================================
def keda_handler(event, context):
    print("=== [Stage 2a] KEDA ScaledObject Patcher ===")
    subscriptions = _get_subscriptions(event)
    if not subscriptions:
        print("No subscriptions received. Skipping.")
        return {"status": "SKIPPED", "subscriptions": []}
    try:
        endpoint, ca_data, token = _connect_eks()
        patch_keda_autoscaler(endpoint, ca_data, token, subscriptions)
        # Pass subscriptions forward — Step Function relays this to configmap_handler after 15s wait
        return {"status": "SUCCESS", "subscriptions": subscriptions}
    except urllib.error.HTTPError as e:
        raise Exception(f"KEDA PATCH FAILED: {e.read().decode('utf-8')}")

# ===========================================================================
# Handler 2: configmap_handler
# Registered as: lambda_autoscaler.configmap_handler
# Step Function State: PatchConfigMap (invoked AFTER a 15s Wait state)
# KEDA has had time to reconcile the new ScaledObject before pods restart.
# ===========================================================================
def configmap_handler(event, context):
    print("=== [Stage 2b] ConfigMap Patcher + Rolling Restart ===")
    subscriptions = _get_subscriptions(event)
    if not subscriptions:
        print("No subscriptions received. Skipping.")
        return {"status": "SKIPPED"}
    try:
        endpoint, ca_data, token = _connect_eks()
        patch_pod_configmap(endpoint, ca_data, token, subscriptions)
        restart_deployment(endpoint, ca_data, token)
        return {"status": "SUCCESS", "patched_topics": len(subscriptions)}
    except urllib.error.HTTPError as e:
        raise Exception(f"CONFIGMAP PATCH FAILED: {e.read().decode('utf-8')}")

# ===========================================================================
# Legacy combined handler — kept for local dry-run testing only
# ===========================================================================
def lambda_handler(event, context):
    result = keda_handler(event, context)
    if result.get('status') != 'SKIPPED':
        configmap_handler(event, context)
    return result

if __name__ == "__main__":
    lambda_handler({}, None)
