"""
EKS Patcher — Group-Isolated Kubernetes Reconciler
====================================================

This Lambda is invoked twice per pipeline run by the Step Function:

  PatchEKSKEDA  → keda_handler()
      Rebuilds the KEDA ScaledObject triggers for the target group using the
      current list of active GCP Pub/Sub subscriptions. KEDA uses this list
      to decide how many consumer pod replicas to schedule.

  [15-second Wait]
      Gives KEDA time to reconcile the new ScaledObject before pods are cycled.

  PatchConfigMap → configmap_handler()
      Rebuilds the ConfigMap so consumer pods know which subscriptions to pull
      from. Triggers a rolling restart so pods pick up the new config.
      On success, flips the Iceberg registry status: PENDING → ACTIVE.

Group Isolation
---------------
Each group (baseline, group1, group2, ...) owns a discrete set of Kubernetes
resources. Resource names are derived from the group parameter at runtime:

    gcp-scaler-{group}     — KEDA ScaledObject
    gcp-configmap-{group}  — ConfigMap consumed by pods
    gcp-consumer-{group}   — Deployment running the consumer workers

The group is passed by EventBridge as the Step Function input payload and
propagated through every state.

Topic lifecycle in the Iceberg registry
---------------------------------------
    PENDING  → topic was discovered; not yet patched into this group's K8s infra
    ACTIVE   → topic is live in KEDA and ConfigMap for this group
    REMOVED  → topic was deleted from GCP or its backing topic was removed;
               excluded from the next rebuild automatically

Moving a topic between groups
------------------------------
Run a single SQL UPDATE in Athena:

    UPDATE gcp_sync_db.subscription_registry
    SET usage_group = 'group1'
    WHERE subscription_name = 'projects/.../subscriptions/my-sub';

The next baseline run removes it from baseline's KEDA/ConfigMap.
The next group1 run adds it to group1's KEDA/ConfigMap.
No code changes or redeployment required.
"""

import os
import json
import base64
import time
import urllib.request
import urllib.error
import ssl
import boto3
from botocore.signers import RequestSigner
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Configuration (injected by Terraform via Lambda environment variables)
# ---------------------------------------------------------------------------

EKS_CLUSTER_NAME = os.getenv("EKS_CLUSTER_NAME", "gcp-sync-poc-test")
EKS_REGION       = os.getenv("EKS_REGION", "us-east-1")
NAMESPACE        = os.getenv("NAMESPACE", "default")

ATHENA_DATABASE  = os.getenv("ATHENA_DATABASE", "gcp_sync_db")
ATHENA_TABLE     = os.getenv("ATHENA_TABLE", "subscription_registry")
ATHENA_OUTPUT    = os.getenv("ATHENA_OUTPUT_LOC", "s3://YOUR-BUCKET/ddl/")


# ---------------------------------------------------------------------------
# K8s Resource Naming
# ---------------------------------------------------------------------------

def _resource_names(group):
    """Returns the Kubernetes resource names for the given group."""
    return {
        "scaledobject": f"gcp-scaler-{group}",
        "configmap":    f"gcp-configmap-{group}",
        "deployment":   f"gcp-consumer-{group}",
    }


# ---------------------------------------------------------------------------
# EKS Authentication
# ---------------------------------------------------------------------------

def _get_eks_token(cluster_name):
    """
    Generates a pre-signed STS URL encoded as a Kubernetes bearer token.
    This is the standard AWS IAM authenticator approach for EKS API access.
    """
    session = boto3.Session()
    sts_client = session.client('sts', region_name=EKS_REGION)
    signer = RequestSigner(
        sts_client.meta.service_model.service_id,
        EKS_REGION, 'sts', 'v4',
        session.get_credentials(), session.events
    )
    signed_url = signer.generate_presigned_url(
        {
            'method': 'GET',
            'url': f'https://sts.{EKS_REGION}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15',
            'body': {},
            'headers': {'x-k8s-aws-id': cluster_name},
            'context': {}
        },
        region_name=EKS_REGION,
        expires_in=60,
        operation_name=''
    )
    return 'k8s-aws-v1.' + base64.urlsafe_b64encode(
        signed_url.encode('utf-8')
    ).decode('utf-8').rstrip('=')


def _get_eks_cluster_info(cluster_name):
    """Returns (endpoint_url, base64_ca_cert) for the EKS cluster."""
    client = boto3.client('eks', region_name=EKS_REGION)
    cluster = client.describe_cluster(name=cluster_name)['cluster']
    return cluster['endpoint'], cluster['certificateAuthority']['data']


def _connect_eks():
    """Authenticates to EKS and returns (endpoint, ca_data, token)."""
    endpoint, ca_data = _get_eks_cluster_info(EKS_CLUSTER_NAME)
    token = _get_eks_token(EKS_CLUSTER_NAME)
    return endpoint, ca_data, token


# ---------------------------------------------------------------------------
# EKS API Client
# ---------------------------------------------------------------------------

def _call_eks_api(endpoint, ca_data, token, path, method, payload, content_type):
    """
    Makes a single authenticated REST call to the EKS Kubernetes API server.

    Args:
        endpoint     : EKS cluster API endpoint URL
        ca_data      : Base64-encoded cluster CA certificate
        token        : STS-signed bearer token
        path         : Kubernetes API path (e.g. /api/v1/namespaces/default/configmaps/foo)
        method       : HTTP method — GET, PATCH, etc.
        payload      : Request body as a Python dict (serialized to JSON internally)
        content_type : Patch type — merge-patch+json for CRDs, strategic-merge-patch+json for core objects
    """
    url = f"{endpoint}{path}"
    data = json.dumps(payload).encode('utf-8')
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": content_type
    }
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    ca_bytes = base64.b64decode(ca_data)
    ssl_context = ssl.create_default_context(cadata=ca_bytes.decode('utf-8'))
    response = urllib.request.urlopen(req, context=ssl_context, timeout=10)
    return json.loads(response.read().decode('utf-8'))


# ---------------------------------------------------------------------------
# Kubernetes Patching
# ---------------------------------------------------------------------------

def _patch_keda_autoscaler(endpoint, ca_data, token, subscriptions, group):
    """
    Replaces the KEDA ScaledObject trigger list for this group with the current
    set of active subscriptions. KEDA reconciles and scales pods accordingly.

    merge-patch+json replaces the entire triggers array — no manual cleanup needed
    when subscriptions are added or removed.
    """
    names = _resource_names(group)
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
    path = f"/apis/keda.sh/v1alpha1/namespaces/{NAMESPACE}/scaledobjects/{names['scaledobject']}"
    _call_eks_api(endpoint, ca_data, token, path, "PATCH",
                  {"spec": {"triggers": triggers}},
                  content_type="application/merge-patch+json")
    print(f"--> KEDA ScaledObject '{names['scaledobject']}' rebuilt with {len(subscriptions)} trigger(s).")


def _patch_configmap(endpoint, ca_data, token, subscriptions, group):
    """
    Overwrites the ConfigMap data for this group with the current subscription list.
    Consumer pods read this file at /app/config/topics.json to know which
    GCP Pub/Sub subscriptions to pull messages from.
    """
    names = _resource_names(group)
    topics_json = json.dumps({"topics": [sub.split("/")[-1] for sub in subscriptions]})
    path = f"/api/v1/namespaces/{NAMESPACE}/configmaps/{names['configmap']}"
    _call_eks_api(endpoint, ca_data, token, path, "PATCH",
                  {"data": {"topics.json": topics_json}},
                  content_type="application/strategic-merge-patch+json")
    print(f"--> ConfigMap '{names['configmap']}' rebuilt with {len(subscriptions)} topic(s).")


def _restart_deployment(endpoint, ca_data, token, group):
    """
    Triggers a rolling restart of the consumer Deployment by stamping a
    'restartedAt' annotation on the pod template. Kubernetes detects the
    metadata change and cycles pods one by one with zero downtime.
    """
    names = _resource_names(group)
    restart_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = f"/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{names['deployment']}"
    _call_eks_api(endpoint, ca_data, token, path, "PATCH",
                  {"spec": {"template": {"metadata": {"annotations": {
                      "kubectl.kubernetes.io/restartedAt": restart_ts
                  }}}}},
                  content_type="application/strategic-merge-patch+json")
    print(f"--> Rolling restart triggered for Deployment '{names['deployment']}'.")


# ---------------------------------------------------------------------------
# Registry Update
# ---------------------------------------------------------------------------

def _mark_topics_active(subscriptions, group):
    """
    After EKS is fully patched, flips PENDING → ACTIVE for this group's
    subscriptions in the Iceberg registry. REMOVED topics are left as-is;
    they were already excluded from the rebuild.

    Ops can query unhealthy topics with:
        SELECT * FROM registry WHERE status IN ('PENDING', 'REMOVED')
    """
    if not subscriptions:
        return
    athena = boto3.client('athena', region_name=EKS_REGION)
    names_csv = ", ".join([f"'{s}'" for s in subscriptions])
    resp = athena.start_query_execution(
        QueryString=f"""
            UPDATE {ATHENA_DATABASE}.{ATHENA_TABLE}
            SET status = 'ACTIVE', last_seen_ts = current_timestamp
            WHERE subscription_name IN ({names_csv})
            AND usage_group = '{group}'
            AND status = 'PENDING'
        """,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    exec_id = resp['QueryExecutionId']
    while True:
        state = athena.get_query_execution(
            QueryExecutionId=exec_id
        )['QueryExecution']['Status']['State']
        if state in ('SUCCEEDED', 'FAILED', 'CANCELLED'):
            break
        time.sleep(1)
    print(f"--> Registry updated: {len(subscriptions)} topic(s) ACTIVE in group '{group}'.")


# ---------------------------------------------------------------------------
# Helper: Step Function payload unwrapper
# ---------------------------------------------------------------------------

def _unpack_event(event):
    """
    The Step Function wraps Lambda outputs inside a 'Payload' envelope.
    This helper normalises both direct invocations and Step Function calls.
    Returns (subscriptions, group).
    """
    payload = event.get('Payload', event)
    return payload.get('subscriptions', []), payload.get('group', 'baseline')


# ---------------------------------------------------------------------------
# Lambda Entry Points
# ---------------------------------------------------------------------------

def keda_handler(event, context):
    """
    Step Function state: PatchEKSKEDA
    Rebuilds the KEDA ScaledObject for the target group. Passes the subscription
    list and group forward in its output so the next state (PatchConfigMap) has
    the same context after the 15-second wait.
    """
    print("=== EKS Patcher: Stage 1 — KEDA ScaledObject ===")
    subscriptions, group = _unpack_event(event)
    print(f"Group: '{group}' | Topics: {len(subscriptions)}")

    if not subscriptions:
        print("No subscriptions for this group. Skipping KEDA patch.")
        return {"status": "SKIPPED", "subscriptions": [], "group": group}

    try:
        endpoint, ca_data, token = _connect_eks()
        _patch_keda_autoscaler(endpoint, ca_data, token, subscriptions, group)
        return {"status": "SUCCESS", "subscriptions": subscriptions, "group": group}
    except urllib.error.HTTPError as e:
        raise Exception(f"KEDA patch failed for group '{group}': {e.read().decode('utf-8')}")


def configmap_handler(event, context):
    """
    Step Function state: PatchConfigMap
    Invoked after the 15-second WaitForKEDAReconcile state. Rebuilds the
    ConfigMap, triggers a rolling pod restart, then marks all PENDING
    subscriptions for this group as ACTIVE in the Iceberg registry.
    """
    print("=== EKS Patcher: Stage 2 — ConfigMap + Restart ===")
    subscriptions, group = _unpack_event(event)
    print(f"Group: '{group}' | Topics: {len(subscriptions)}")

    if not subscriptions:
        print("No subscriptions for this group. Skipping ConfigMap patch.")
        return {"status": "SKIPPED", "group": group}

    try:
        endpoint, ca_data, token = _connect_eks()
        _patch_configmap(endpoint, ca_data, token, subscriptions, group)
        _restart_deployment(endpoint, ca_data, token, group)
        _mark_topics_active(subscriptions, group)
        return {"status": "SUCCESS", "patched_topics": len(subscriptions), "group": group}
    except urllib.error.HTTPError as e:
        raise Exception(f"ConfigMap patch failed for group '{group}': {e.read().decode('utf-8')}")
