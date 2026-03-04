import os
import json
import base64
import time
import urllib.request
import urllib.error
import ssl
import boto3
from botocore.signers import RequestSigner

# -------------------------------------------------------------------
# Phase 4: Group-Isolated EKS Patcher
# K8s resource names are derived from the group parameter at runtime:
#   gcp-scaler-{group}, gcp-configmap-{group}, gcp-consumer-{group}
# -------------------------------------------------------------------

EKS_CLUSTER_NAME = os.getenv("EKS_CLUSTER_NAME", "gcp-sync-poc-test")
EKS_REGION       = os.getenv("EKS_REGION", "us-east-1")
NAMESPACE        = os.getenv("NAMESPACE", "default")

# Athena — used to flip topic status PENDING/REMOVED -> ACTIVE after successful EKS patching
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "gcp_sync_db")
ATHENA_TABLE    = os.getenv("ATHENA_TABLE", "subscription_registry")
ATHENA_OUTPUT   = os.getenv("ATHENA_OUTPUT_LOC", "s3://YOUR-BUCKET/ddl/")

def _resource_names(group):
    """Returns the K8s resource names for a given group."""
    return {
        "scaledobject": f"gcp-scaler-{group}",
        "configmap":    f"gcp-configmap-{group}",
        "deployment":   f"gcp-consumer-{group}",
    }

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

def patch_keda_autoscaler(endpoint, ca_data, token, subscriptions, group):
    """
    Rebuilds the KEDA ScaledObject triggers for the given group.
    Uses merge-patch+json to replace the entire triggers array.
    """
    names = _resource_names(group)
    scaledobject_name = names["scaledobject"]
    triggers = [
        {
            "type": "gcp-pubsub",
            "metadata": {
                "subscriptionName": sub.split("/")[-1],
                "subscriptionSize": "5"
            }
        }
        for sub in subscriptions
    ]
    path = f"/apis/keda.sh/v1alpha1/namespaces/{NAMESPACE}/scaledobjects/{scaledobject_name}"
    call_eks_api(endpoint, ca_data, token, path, "PATCH",
                 {"spec": {"triggers": triggers}},
                 content_type="application/merge-patch+json")
    print(f"--> [SUCCESS] KEDA ScaledObject '{scaledobject_name}' patched with {len(subscriptions)} trigger(s).")

def patch_pod_configmap(endpoint, ca_data, token, subscriptions, group):
    """
    Patches the ConfigMap for the given group with the current topic list.
    Uses strategic-merge-patch+json (correct type for core Kubernetes objects).
    """
    names = _resource_names(group)
    configmap_name = names["configmap"]
    topics_json = json.dumps({"topics": [sub.split("/")[-1] for sub in subscriptions]})
    path = f"/api/v1/namespaces/{NAMESPACE}/configmaps/{configmap_name}"
    call_eks_api(endpoint, ca_data, token, path, "PATCH",
                 {"data": {"topics.json": topics_json}},
                 content_type="application/strategic-merge-patch+json")
    print(f"--> [SUCCESS] ConfigMap '{configmap_name}' patched with {len(subscriptions)} topic(s).")

def restart_deployment(endpoint, ca_data, token, group):
    """
    Triggers a rolling restart of the consumer Deployment for the given group
    by patching the pod template annotation with the current timestamp.
    """
    from datetime import datetime, timezone
    names = _resource_names(group)
    deployment_name = names["deployment"]
    restart_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = f"/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{deployment_name}"
    call_eks_api(endpoint, ca_data, token, path, "PATCH",
                 {"spec": {"template": {"metadata": {"annotations": {
                     "kubectl.kubernetes.io/restartedAt": restart_ts
                 }}}}},
                 content_type="application/strategic-merge-patch+json")
    print(f"--> [SUCCESS] Rolling restart triggered for Deployment '{deployment_name}'.")


def _get_subscriptions(event):
    """Unwraps the Step Function Payload envelope and returns the subscriptions list."""
    effective = event.get('Payload', event)
    return effective.get('subscriptions', [])

def _connect_eks():
    """Authenticates to EKS and returns (endpoint, ca_data, token)."""
    endpoint, ca_data = get_eks_cluster_info(EKS_CLUSTER_NAME)
    token = get_eks_token(EKS_CLUSTER_NAME)
    return endpoint, ca_data, token

def mark_topics_active(subscriptions, group):
    """
    After successful EKS patching:
    - PENDING topics in this group → ACTIVE  (newly onboarded)
    - REMOVED topics in this group → deleted from Athena perspective we leave as REMOVED
      (they no longer appear in the ACTIVE list so KEDA/ConfigMap won't reference them)
    Ops query: SELECT * FROM registry WHERE status IN ('PENDING','REMOVED')
    """
    if not subscriptions:
        return
    athena = boto3.client('athena', region_name=EKS_REGION)
    names_csv = ", ".join([f"'{s}'" for s in subscriptions])
    query = f"""
    UPDATE {ATHENA_DATABASE}.{ATHENA_TABLE}
    SET status = 'ACTIVE', last_seen_ts = current_timestamp
    WHERE subscription_name IN ({names_csv})
    AND usage_group = '{group}'
    AND status = 'PENDING'
    """
    resp = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    exec_id = resp['QueryExecutionId']
    while True:
        state = athena.get_query_execution(QueryExecutionId=exec_id)['QueryExecution']['Status']['State']
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(1)
    print(f"--> [STATUS] Marked {len(subscriptions)} topics ACTIVE in group '{group}' (state={state}).")

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
    group = event.get('Payload', event).get('group', 'group1')
    print(f"Group: '{group}' | Subscriptions: {len(subscriptions)}")
    if not subscriptions:
        print("No subscriptions received. Skipping.")
        return {"status": "SKIPPED", "subscriptions": [], "group": group}
    try:
        endpoint, ca_data, token = _connect_eks()
        patch_keda_autoscaler(endpoint, ca_data, token, subscriptions, group)
        return {"status": "SUCCESS", "subscriptions": subscriptions, "group": group}
    except urllib.error.HTTPError as e:
        raise Exception(f"KEDA PATCH FAILED [{group}]: {e.read().decode('utf-8')}")

# ===========================================================================
# Handler 2: configmap_handler
# Registered as: lambda_autoscaler.configmap_handler
# Step Function State: PatchConfigMap (invoked AFTER a 15s Wait state)
# KEDA has had time to reconcile the new ScaledObject before pods restart.
# ===========================================================================
def configmap_handler(event, context):
    print("=== [Stage 2b] ConfigMap Patcher + Rolling Restart ===")
    subscriptions = _get_subscriptions(event)
    group = event.get('Payload', event).get('group', 'group1')
    print(f"Group: '{group}' | Subscriptions: {len(subscriptions)}")
    if not subscriptions:
        print("No subscriptions received. Skipping.")
        return {"status": "SKIPPED", "group": group}
    try:
        endpoint, ca_data, token = _connect_eks()
        patch_pod_configmap(endpoint, ca_data, token, subscriptions, group)
        restart_deployment(endpoint, ca_data, token, group)
        mark_topics_active(subscriptions, group)
        return {"status": "SUCCESS", "patched_topics": len(subscriptions), "group": group}
    except urllib.error.HTTPError as e:
        raise Exception(f"CONFIGMAP PATCH FAILED [{group}]: {e.read().decode('utf-8')}")


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
