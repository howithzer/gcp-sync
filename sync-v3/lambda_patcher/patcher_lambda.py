"""
GCP Sync — K8s Patcher Service (v3)
=====================================

Single responsibility: reconcile Kubernetes resources for ONE group.
No GCP API calls. No registry writes except marking PENDING → ACTIVE.

Triggered by the Patching Step Function via EventBridge on a group-specific
schedule (e.g., every 4 hours), OR on-demand by the Discovery Lambda when
new PENDING topics are found.

Step Function flow
------------------
  EventBridge {group: "baseline"}
       |
  PatchKEDA  → keda_handler()
       Reads registry for the group, rebuilds the KEDA ScaledObject triggers.
       Creates the ScaledObject if it does not yet exist.
       Passes {group, subscriptions} forward.
       |
  Wait 15s
       Gives KEDA time to reconcile the new ScaledObject before pods restart.
       |
  PatchConfigMap → configmap_handler()
       Creates or updates the ConfigMap (topic list pods read at startup).
       Verifies the target Deployment exists before touching it.
       Triggers a rolling restart of the consumer Deployment.
       Polls Deployment status until all pods are Running and Ready
       (or raises TimeoutError if rollout does not complete in time).
       Marks all PENDING subscriptions for this group as ACTIVE.

Changes from v2
---------------
  - Deployment existence check: configmap_handler verifies the Deployment
    exists via a GET before issuing the restart PATCH. Raises a clear error
    if it is missing rather than sending a 404 PATCH into the void.
  - Rollout confirmation: after triggering the rolling restart,
    _wait_for_rollout() polls the Deployment status until:
      • updatedReplicas == spec.replicas
      • readyReplicas   == spec.replicas
      • unavailableReplicas == 0
      • Progressing condition reason == 'NewReplicaSetAvailable'
    Only then are PENDING topics marked ACTIVE.  If the rollout does not
    complete within ROLLOUT_TIMEOUT_SECONDS the Lambda raises TimeoutError
    so the Step Function can retry or alert.
  - Create-or-update for KEDA ScaledObject and ConfigMap: keda_handler and
    configmap_handler now check resource existence (GET) and fall back to
    a full POST (create) if the resource is missing, making the patcher
    idempotent on first bootstrap.
  - Athena polling timeout: all Athena waits enforce ATHENA_POLL_TIMEOUT.
  - SQL sanitisation: subscription names are escaped before interpolation.
  - Configurable KEDA parameters: KEDA_SUBSCRIPTION_SIZE, KEDA_MIN_REPLICAS,
    KEDA_MAX_REPLICAS, KEDA_POLLING_INTERVAL are read from env vars.

K8s resource naming (derived from group at runtime)
----------------------------------------------------
  gcp-scaler-{group}     — KEDA ScaledObject
  gcp-configmap-{group}  — ConfigMap
  gcp-consumer-{group}   — Deployment
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
ATHENA_POLL_TIMEOUT = int(os.getenv("ATHENA_POLL_TIMEOUT", "120"))

# KEDA ScaledObject parameters — tune per workload
KEDA_SUBSCRIPTION_SIZE  = os.getenv("KEDA_SUBSCRIPTION_SIZE", "5")
KEDA_MIN_REPLICAS       = int(os.getenv("KEDA_MIN_REPLICAS", "0"))
KEDA_MAX_REPLICAS       = int(os.getenv("KEDA_MAX_REPLICAS", "10"))
KEDA_POLLING_INTERVAL   = int(os.getenv("KEDA_POLLING_INTERVAL", "30"))

# Rolling restart confirmation — Lambda timeout must exceed this
ROLLOUT_TIMEOUT_SECONDS = int(os.getenv("ROLLOUT_TIMEOUT_SECONDS", "240"))


# ---------------------------------------------------------------------------
# Registry Read / Write
# ---------------------------------------------------------------------------

def _read_group_subscriptions(group):
    """
    Reads current non-REMOVED subscriptions for this group from the registry.
    This is the desired state — what KEDA and ConfigMap should reflect.
    """
    athena = boto3.client('athena', region_name=EKS_REGION)
    
    # Use ExecutionParameters to parameterize the query securely
    resp = athena.start_query_execution(
        QueryString=f"""
        SELECT subscription_name FROM {ATHENA_DATABASE}.{ATHENA_TABLE}
        WHERE usage_group = ? AND status != 'REMOVED'
        """,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT},
        ExecutionParameters=[group]
    )
    
    exec_id = resp['QueryExecutionId']
    deadline = time.time() + ATHENA_POLL_TIMEOUT

    while time.time() < deadline:
        result = athena.get_query_execution(QueryExecutionId=exec_id)
        status = result['QueryExecution']['Status']
        state  = status['State']
        if state == 'SUCCEEDED':
            break
        if state in ('FAILED', 'CANCELLED'):
            reason = status.get('StateChangeReason', 'no reason returned')
            raise RuntimeError(f"Athena query {exec_id} {state}: {reason}")
        time.sleep(2)
    else:
        athena.stop_query_execution(QueryExecutionId=exec_id)
        raise TimeoutError(f"Athena query {exec_id} timed out.")
        
    rows = athena.get_query_results(QueryExecutionId=exec_id)['ResultSet']['Rows']
    subs = [r['Data'][0]['VarCharValue'] for r in rows[1:]]
    print(f"Group '{group}': {len(subs)} subscription(s) in registry.")
    return subs


def _mark_topics_active(subscriptions, group):
    """
    After successful K8s patching and rollout confirmation, flips
    PENDING → ACTIVE for this group's subscriptions.
    """
    if not subscriptions:
        return
    athena     = boto3.client('athena', region_name=EKS_REGION)
    names_csv  = ", ".join("?" for _ in subscriptions)
    
    flat_params = list(subscriptions)
    flat_params.append(group)
    
    resp = athena.start_query_execution(
        QueryString=f"""
        UPDATE {ATHENA_DATABASE}.{ATHENA_TABLE}
        SET status = 'ACTIVE', last_seen_ts = current_timestamp
        WHERE subscription_name IN ({names_csv})
        AND usage_group = ?
        AND status = 'PENDING'
        """,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': ATHENA_OUTPUT},
        ExecutionParameters=flat_params
    )
    
    exec_id = resp['QueryExecutionId']
    deadline = time.time() + ATHENA_POLL_TIMEOUT

    while time.time() < deadline:
        result = athena.get_query_execution(QueryExecutionId=exec_id)
        status = result['QueryExecution']['Status']
        state  = status['State']
        if state == 'SUCCEEDED':
            break
        if state in ('FAILED', 'CANCELLED'):
            reason = status.get('StateChangeReason', 'no reason returned')
            raise RuntimeError(f"Athena query {exec_id} {state}: {reason}")
        time.sleep(2)
    else:
        athena.stop_query_execution(QueryExecutionId=exec_id)
        raise TimeoutError(f"Athena query {exec_id} timed out.")
        
    print(f"Marked PENDING → ACTIVE for {len(subscriptions)} topic(s) in group '{group}'.")


# ---------------------------------------------------------------------------
# EKS Authentication
# ---------------------------------------------------------------------------

def _get_eks_token(cluster_name):
    """Generates a pre-signed STS URL as a Kubernetes bearer token (expires 60s)."""
    session = boto3.Session()
    sts     = session.client('sts', region_name=EKS_REGION)
    signer  = RequestSigner(
        sts.meta.service_model.service_id, EKS_REGION, 'sts', 'v4',
        session.get_credentials(), session.events
    )
    signed_url = signer.generate_presigned_url(
        {
            'method':  'GET',
            'url':     f'https://sts.{EKS_REGION}.amazonaws.com/?Action=GetCallerIdentity&Version=2011-06-15',
            'body':    {},
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
    """Authenticates to EKS. Returns (endpoint, ca_data, token_func)
    where token_func generates fresh signed STS tokens on demand.
    """
    endpoint, ca_data = _get_eks_cluster_info(EKS_CLUSTER_NAME)
    token_func        = lambda: _get_eks_token(EKS_CLUSTER_NAME)
    return endpoint, ca_data, token_func


# ---------------------------------------------------------------------------
# EKS API Client
# ---------------------------------------------------------------------------

def _call_eks_api(endpoint, ca_data, token_func, path, method="GET", payload=None, content_type=None):
    """
    Makes an authenticated REST call to the EKS Kubernetes API server.
    payload=None → no request body (used for GET / existence checks).
    Raises urllib.error.HTTPError on non-2xx responses (caller handles 404).
    Dynamically requests a new STS token from token_func() to prevent 60s
    STS token expiration during long rollout-wait polling loops.
    """
    url  = f"{endpoint}{path}"
    data = json.dumps(payload).encode('utf-8') if payload is not None else None

    headers = {
        "Authorization": f"Bearer {token_func()}",
        "Accept":        "application/json",
    }
    if data and content_type:
        headers["Content-Type"] = content_type

    req     = urllib.request.Request(url, data=data, method=method, headers=headers)
    ssl_ctx = ssl.create_default_context(cadata=base64.b64decode(ca_data).decode('utf-8'))
    return json.loads(urllib.request.urlopen(req, context=ssl_ctx, timeout=15).read())


def _resource_exists(endpoint, ca_data, token_func, path):
    """Returns True if the resource at path exists (GET → 200), False on 404."""
    try:
        _call_eks_api(endpoint, ca_data, token_func, path)
        return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        raise


# ---------------------------------------------------------------------------
# Resource name helpers
# ---------------------------------------------------------------------------

def _resource_names(group):
    return {
        "scaledobject": f"gcp-scaler-{group}",
        "configmap":    f"gcp-configmap-{group}",
        "deployment":   f"gcp-consumer-{group}",
    }


# ---------------------------------------------------------------------------
# Kubernetes Patching — KEDA ScaledObject
# ---------------------------------------------------------------------------

def _patch_or_create_keda(endpoint, ca_data, token_func, subscriptions, group):
    """
    Rebuilds the KEDA ScaledObject trigger list for the group.
    - If the ScaledObject already exists: PATCH (merge-patch replaces the
      entire triggers array, so removed subscriptions are cleaned up).
    - If it does not exist yet: POST a full ScaledObject definition so the
      patcher is idempotent on first bootstrap.
    """
    names    = _resource_names(group)
    triggers = [
        {
            "type": "gcp-pubsub",
            "metadata": {
                "subscriptionName": sub.split("/")[-1],
                "subscriptionSize": KEDA_SUBSCRIPTION_SIZE,
            }
        }
        for sub in subscriptions
    ]

    scaledobject_path      = f"/apis/keda.sh/v1alpha1/namespaces/{NAMESPACE}/scaledobjects/{names['scaledobject']}"
    scaledobject_list_path = f"/apis/keda.sh/v1alpha1/namespaces/{NAMESPACE}/scaledobjects"

    if _resource_exists(endpoint, ca_data, token_func, scaledobject_path):
        _call_eks_api(
            endpoint, ca_data, token_func,
            scaledobject_path, "PATCH",
            {"spec": {"triggers": triggers}},
            "application/merge-patch+json"
        )
        print(f"--> KEDA '{names['scaledobject']}' updated with {len(triggers)} trigger(s).")
    else:
        body = {
            "apiVersion": "keda.sh/v1alpha1",
            "kind":       "ScaledObject",
            "metadata": {
                "name":      names['scaledobject'],
                "namespace": NAMESPACE,
            },
            "spec": {
                "scaleTargetRef": {
                    "apiVersion": "apps/v1",
                    "kind":       "Deployment",
                    "name":       names['deployment'],
                },
                "minReplicaCount": KEDA_MIN_REPLICAS,
                "maxReplicaCount": KEDA_MAX_REPLICAS,
                "pollingInterval": KEDA_POLLING_INTERVAL,
                "triggers":        triggers,
            }
        }
        _call_eks_api(
            endpoint, ca_data, token_func,
            scaledobject_list_path, "POST",
            body, "application/json"
        )
        print(f"--> KEDA '{names['scaledobject']}' created with {len(triggers)} trigger(s).")


# ---------------------------------------------------------------------------
# Kubernetes Patching — ConfigMap
# ---------------------------------------------------------------------------

def _patch_or_create_configmap(endpoint, ca_data, token_func, subscriptions, group):
    """
    Rebuilds the ConfigMap with the current topic list for the group.
    - If the ConfigMap exists: PATCH data.topics.json in place.
    - If it does not exist: POST a new ConfigMap so bootstrap is automatic.
    """
    names       = _resource_names(group)
    topics_json = json.dumps({"topics": [sub.split("/")[-1] for sub in subscriptions]})
    cm_path      = f"/api/v1/namespaces/{NAMESPACE}/configmaps/{names['configmap']}"
    cm_list_path = f"/api/v1/namespaces/{NAMESPACE}/configmaps"

    if _resource_exists(endpoint, ca_data, token_func, cm_path):
        _call_eks_api(
            endpoint, ca_data, token_func,
            cm_path, "PATCH",
            {"data": {"topics.json": topics_json}},
            "application/strategic-merge-patch+json"
        )
        print(f"--> ConfigMap '{names['configmap']}' updated with {len(subscriptions)} topic(s).")
    else:
        body = {
            "apiVersion": "v1",
            "kind":       "ConfigMap",
            "metadata": {
                "name":      names['configmap'],
                "namespace": NAMESPACE,
            },
            "data": {"topics.json": topics_json},
        }
        _call_eks_api(
            endpoint, ca_data, token_func,
            cm_list_path, "POST",
            body, "application/json"
        )
        print(f"--> ConfigMap '{names['configmap']}' created with {len(subscriptions)} topic(s).")


# ---------------------------------------------------------------------------
# Kubernetes Patching — Deployment Restart
# ---------------------------------------------------------------------------

def _restart_deployment(endpoint, ca_data, token_func, group):
    """
    Triggers a rolling restart via the restartedAt annotation on the pod
    template — identical to what `kubectl rollout restart` does under the hood.

    Raises a descriptive RuntimeError if the Deployment does not exist,
    rather than sending a 404 PATCH silently.
    """
    names      = _resource_names(group)
    deploy_path = f"/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{names['deployment']}"

    if not _resource_exists(endpoint, ca_data, token_func, deploy_path):
        raise RuntimeError(
            f"Deployment '{names['deployment']}' not found in namespace '{NAMESPACE}'. "
            f"Create the Deployment before running the patcher."
        )

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    _call_eks_api(
        endpoint, ca_data, token_func,
        deploy_path, "PATCH",
        {"spec": {"template": {"metadata": {"annotations": {
            "kubectl.kubernetes.io/restartedAt": ts
        }}}}},
        "application/strategic-merge-patch+json"
    )
    print(f"--> Rolling restart triggered for '{names['deployment']}' (restartedAt={ts}).")


def _wait_for_rollout(endpoint, ca_data, token_func, group, timeout=None):
    """
    Polls the Deployment status until the rolling restart is fully complete:
      - updatedReplicas  == spec.replicas  (all pods on the new ReplicaSet)
      - readyReplicas    == spec.replicas  (all pods passing readiness probes)
      - unavailableReplicas == 0           (no pods down)
      - Progressing condition reason == 'NewReplicaSetAvailable'

    Polls every 10 seconds. Raises TimeoutError if the rollout does not
    complete within the deadline (ROLLOUT_TIMEOUT_SECONDS, default 240s).

    Note: if KEDA has scaled the Deployment to 0 replicas there are no pods
    to wait for; the function returns immediately in that case.
    """
    if timeout is None:
        timeout = ROLLOUT_TIMEOUT_SECONDS

    names       = _resource_names(group)
    deploy_path = f"/apis/apps/v1/namespaces/{NAMESPACE}/deployments/{names['deployment']}"
    deadline    = time.time() + timeout

    print(f"Waiting up to {timeout}s for rollout of '{names['deployment']}' to complete...")

    while time.time() < deadline:
        deploy        = _call_eks_api(endpoint, ca_data, token_func, deploy_path)
        spec_replicas = deploy.get('spec', {}).get('replicas', 1)

        if spec_replicas == 0:
            print(f"  Deployment '{names['deployment']}' is at 0 replicas — nothing to wait for.")
            return

        status       = deploy.get('status', {})
        updated      = status.get('updatedReplicas', 0)
        ready        = status.get('readyReplicas', 0)
        unavailable  = status.get('unavailableReplicas', 0)

        conditions   = {c['type']: c for c in status.get('conditions', [])}
        progressing  = conditions.get('Progressing', {})
        available    = conditions.get('Available', {})

        print(
            f"  updated={updated}/{spec_replicas} ready={ready} "
            f"unavailable={unavailable} "
            f"progressing_reason={progressing.get('reason', '?')} "
            f"available={available.get('status', '?')}"
        )

        rollout_done = (
            updated     == spec_replicas
            and ready       == spec_replicas
            and unavailable == 0
            and progressing.get('reason') == 'NewReplicaSetAvailable'
            and available.get('status')   == 'True'
        )

        if rollout_done:
            print(f"Rollout of '{names['deployment']}' complete.")
            return

        time.sleep(10)

    raise TimeoutError(
        f"Rollout of '{names['deployment']}' did not complete within {timeout}s. "
        f"Inspect pod events: kubectl get events -n {NAMESPACE} "
        f"--field-selector involvedObject.name={names['deployment']}"
    )


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
    Reads desired subscriptions from the registry for the group, then creates
    or updates the KEDA ScaledObject. Passes group + subscription list forward.
    """
    print("=== Patcher: Stage 1 — KEDA ScaledObject ===")
    group, _ = _unpack(event)
    print(f"Group: '{group}'")

    subscriptions = _read_group_subscriptions(group)

    if not subscriptions:
        print(f"Group '{group}' has no subscriptions. Skipping.")
        return {"status": "SKIPPED", "group": group, "subscriptions": []}

    try:
        endpoint, ca_data, token_func = _connect_eks()
        _patch_or_create_keda(endpoint, ca_data, token_func, subscriptions, group)
        return {"status": "SUCCESS", "group": group, "subscriptions": subscriptions}
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"KEDA patch failed [{group}]: {e.read().decode()}") from e


def configmap_handler(event, context):
    """
    Step Function state: PatchConfigMap  (invoked after 15s WaitForKEDA)

    1. Creates or updates the ConfigMap with the current topic list.
    2. Verifies the Deployment exists (raises RuntimeError if not).
    3. Triggers a rolling restart via restartedAt annotation.
    4. Polls Deployment status until all pods are Ready (rollout confirmation).
    5. Only then marks PENDING subscriptions ACTIVE in the Iceberg registry.

    Lambda timeout must exceed ROLLOUT_TIMEOUT_SECONDS (default 240s).
    Set the Lambda timeout to at least 300s in Terraform.
    """
    print("=== Patcher: Stage 2 — ConfigMap + Restart + Rollout Confirmation ===")
    group, subscriptions = _unpack(event)
    print(f"Group: '{group}' | Topics: {len(subscriptions)}")

    if not subscriptions:
        print(f"Group '{group}' has no subscriptions. Skipping.")
        return {"status": "SKIPPED", "group": group}

    try:
        endpoint, ca_data, token_func = _connect_eks()
        _patch_or_create_configmap(endpoint, ca_data, token_func, subscriptions, group)
        _restart_deployment(endpoint, ca_data, token_func, group)
        _wait_for_rollout(endpoint, ca_data, token_func, group)
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"ConfigMap/restart patch failed [{group}]: {e.read().decode()}") from e

    # Only mark ACTIVE after pods are confirmed healthy
    _mark_topics_active(subscriptions, group)

    return {"status": "SUCCESS", "group": group, "patched_topics": len(subscriptions)}
