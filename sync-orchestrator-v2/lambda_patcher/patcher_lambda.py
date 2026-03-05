"""
GCP Sync — K8s Patcher Service (v2)
=====================================

Single responsibility: reconcile Kubernetes resources for ONE group.
No GCP API calls. No registry writes except marking PENDING → ACTIVE.

Triggered by the Patching Step Function via EventBridge on a group-specific
schedule (e.g., every 4 hours), OR on-demand by the Discovery Lambda when
new PENDING topics are found.

Step Function flow
------------------
  EventBridge {group: "baseline"}
       ↓
  PatchKEDA  → keda_handler()
       Reads registry for the group, rebuilds the KEDA ScaledObject triggers.
       Passes {group, subscriptions} forward.
       ↓
  Wait 15s
       Gives KEDA time to reconcile the new ScaledObject before pods restart.
       ↓
  PatchConfigMap → configmap_handler()
       Rebuilds the ConfigMap (topic list pods read at /app/config/topics.json).
       Triggers a rolling restart of the consumer Deployment.
       Marks all PENDING subscriptions for this group as ACTIVE in the registry.

K8s resource naming (derived from group at runtime)
----------------------------------------------------
  gcp-scaler-{group}     — KEDA ScaledObject
  gcp-configmap-{group}  — ConfigMap
  gcp-consumer-{group}   — Deployment

Moving a topic between groups (no code change required)
-------------------------------------------------------
  Run in Athena:
      UPDATE gcp_sync_db.subscription_registry
      SET usage_group = 'group1'
      WHERE subscription_name = 'projects/.../subscriptions/my-sub';

  The next baseline patch removes it from baseline's KEDA/ConfigMap.
  The next group1 patch adds it to group1's KEDA/ConfigMap.
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
# Configuration
# ---------------------------------------------------------------------------

EKS_CLUSTER_NAME = os.getenv("EKS_CLUSTER_NAME", "gcp-sync-poc-test")
EKS_REGION       = os.getenv("EKS_REGION", "us-east-1")
NAMESPACE        = os.getenv("NAMESPACE", "default")

ATHENA_DATABASE  = os.getenv("ATHENA_DATABASE", "gcp_sync_db")
ATHENA_TABLE     = os.getenv("ATHENA_TABLE", "subscription_registry")
ATHENA_OUTPUT    = os.getenv("ATHENA_OUTPUT_LOC", "s3://YOUR-BUCKET/patcher/")


# ---------------------------------------------------------------------------
# Registry Read
# ---------------------------------------------------------------------------

def _read_group_subscriptions(group):
    """
    Reads the current non-REMOVED subscriptions for this group from the registry.
    This is the desired state — what KEDA and ConfigMap should reflect.
    """
    athena = boto3.client('athena', region_name=EKS_REGION)
    resp = athena.start_query_execution(
        QueryString=f"""
            SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
            WHERE usage_group = '{group}' AND status != 'REMOVED'
        """,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT}
    )
    exec_id = resp['QueryExecutionId']
    while True:
        state = athena.get_query_execution(
            QueryExecutionId=exec_id
        )['QueryExecution']['Status']['State']
        if state == 'SUCCEEDED':
            break
        if state in ('FAILED', 'CANCELLED'):
            raise Exception(f"Athena read failed for group '{group}'")
        time.sleep(1)

    rows = athena.get_query_results(QueryExecutionId=exec_id)['ResultSet']['Rows']
    subs = [r['Data'][0]['VarCharValue'] for r in rows[1:]]
    print(f"Group '{group}': {len(subs)} subscription(s) in registry.")
    return subs


def _mark_topics_active(subscriptions, group):
    """
    After successful K8s patching, flips PENDING → ACTIVE for this group.
    REMOVED topics are left as-is (they were excluded from the rebuild).
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
    print(f"Marked PENDING → ACTIVE for {len(subscriptions)} topic(s) in group '{group}'.")


# ---------------------------------------------------------------------------
# EKS Authentication
# ---------------------------------------------------------------------------

def _get_eks_token(cluster_name):
    """Generates a pre-signed STS URL as a Kubernetes bearer token."""
    session = boto3.Session()
    sts = session.client('sts', region_name=EKS_REGION)
    signer = RequestSigner(
        sts.meta.service_model.service_id, EKS_REGION, 'sts', 'v4',
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
        region_name=EKS_REGION, expires_in=60, operation_name=''
    )
    return 'k8s-aws-v1.' + base64.urlsafe_b64encode(
        signed_url.encode('utf-8')
    ).decode('utf-8').rstrip('=')


def _get_eks_cluster_info(cluster_name):
    """Returns (endpoint_url, base64_ca_cert) for the cluster."""
    cluster = boto3.client('eks', region_name=EKS_REGION).describe_cluster(
        name=cluster_name
    )['cluster']
    return cluster['endpoint'], cluster['certificateAuthority']['data']


def _connect_eks():
    """Authenticates to EKS. Returns (endpoint, ca_data, token)."""
    endpoint, ca_data = _get_eks_cluster_info(EKS_CLUSTER_NAME)
    token = _get_eks_token(EKS_CLUSTER_NAME)
    return endpoint, ca_data, token


# ---------------------------------------------------------------------------
# EKS API Client
# ---------------------------------------------------------------------------

def _call_eks_api(endpoint, ca_data, token, path, method, payload, content_type):
    """Makes an authenticated REST call to the EKS Kubernetes API server."""
    url = f"{endpoint}{path}"
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": content_type
        }
    )
    ssl_ctx = ssl.create_default_context(cadata=base64.b64decode(ca_data).decode('utf-8'))
    return json.loads(urllib.request.urlopen(req, context=ssl_ctx, timeout=10).read())


# ---------------------------------------------------------------------------
# Kubernetes Patching
# ---------------------------------------------------------------------------

def _resource_names(group):
    return {
        "scaledobject": f"gcp-scaler-{group}",
        "configmap":    f"gcp-configmap-{group}",
        "deployment":   f"gcp-consumer-{group}",
    }


def _patch_keda(endpoint, ca_data, token, subscriptions, group):
    """
    Replaces the KEDA ScaledObject trigger list for this group.
    merge-patch+json replaces the entire triggers array — no cleanup needed
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
                  "application/merge-patch+json")
    print(f"--> KEDA '{names['scaledobject']}' rebuilt with {len(subscriptions)} trigger(s).")


def _patch_configmap(endpoint, ca_data, token, subscriptions, group):
    """Updates the ConfigMap with the current topic list for this group."""
    names = _resource_names(group)
    topics_json = json.dumps({"topics": [sub.split("/")[-1] for sub in subscriptions]})
    path = f"/api/v1/namespaces/{NAMESPACE}/configmaps/{names['configmap']}"
    _call_eks_api(endpoint, ca_data, token, path, "PATCH",
                  {"data": {"topics.json": topics_json}},
                  "application/strategic-merge-patch+json")
    print(f"--> ConfigMap '{names['configmap']}' rebuilt with {len(subscriptions)} topic(s).")


def _restart_deployment(endpoint, ca_data, token, group):
    """
    Rolling restart via restartedAt annotation on the pod template.
    Kubernetes cycles pods one by one — zero downtime.
    """
    names = _resource_names(group)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    path = f"/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{names['deployment']}"
    _call_eks_api(endpoint, ca_data, token, path, "PATCH",
                  {"spec": {"template": {"metadata": {"annotations": {
                      "kubectl.kubernetes.io/restartedAt": ts
                  }}}}},
                  "application/strategic-merge-patch+json")
    print(f"--> Rolling restart triggered for '{names['deployment']}'.")


# ---------------------------------------------------------------------------
# Lambda Entry Points
# ---------------------------------------------------------------------------

def _unpack(event):
    """Normalises direct invocations and Step Function Payload envelopes."""
    payload = event.get('Payload', event)
    return payload.get('group', 'baseline'), payload.get('subscriptions', [])


def keda_handler(event, context):
    """
    Step Function state: PatchKEDA
    Reads the group's desired subscriptions from the registry, then rebuilds
    the KEDA ScaledObject. Passes group + subscription list to the next state.
    """
    print("=== Patcher: Stage 1 — KEDA ScaledObject ===")
    group, _ = _unpack(event)
    print(f"Group: '{group}'")

    subscriptions = _read_group_subscriptions(group)

    if not subscriptions:
        print(f"Group '{group}' has no subscriptions. Skipping.")
        return {"status": "SKIPPED", "group": group, "subscriptions": []}

    try:
        endpoint, ca_data, token = _connect_eks()
        _patch_keda(endpoint, ca_data, token, subscriptions, group)
        return {"status": "SUCCESS", "group": group, "subscriptions": subscriptions}
    except urllib.error.HTTPError as e:
        raise Exception(f"KEDA patch failed [{group}]: {e.read().decode()}")


def configmap_handler(event, context):
    """
    Step Function state: PatchConfigMap  (invoked after 15s WaitForKEDA)
    Rebuilds the ConfigMap, triggers a rolling pod restart, then marks all
    PENDING subscriptions ACTIVE in the Iceberg registry.
    """
    print("=== Patcher: Stage 2 — ConfigMap + Restart ===")
    group, subscriptions = _unpack(event)
    print(f"Group: '{group}' | Topics: {len(subscriptions)}")

    if not subscriptions:
        print(f"Group '{group}' has no subscriptions. Skipping.")
        return {"status": "SKIPPED", "group": group}

    try:
        endpoint, ca_data, token = _connect_eks()
        _patch_configmap(endpoint, ca_data, token, subscriptions, group)
        _restart_deployment(endpoint, ca_data, token, group)
        _mark_topics_active(subscriptions, group)
        return {"status": "SUCCESS", "group": group, "patched_topics": len(subscriptions)}
    except urllib.error.HTTPError as e:
        raise Exception(f"ConfigMap patch failed [{group}]: {e.read().decode()}")
