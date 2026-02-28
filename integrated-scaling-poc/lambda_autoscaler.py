import os
import json
import base64
import urllib.request
import urllib.error
import ssl
import boto3
from botocore.signers import RequestSigner

# -------------------------------------------------------------------
# Phase 3: Integrated Autoscaling Orchestrator
# This Lambda script forces a transactional atomic update:
# 1. Updates KEDA Auto-Scaling Rules First
# 2. Updates the Pod ConfigMap Second
# -------------------------------------------------------------------

EKS_CLUSTER_NAME = os.getenv("EKS_CLUSTER_NAME", "gcp-sync-poc-test")
EKS_REGION = os.getenv("EKS_REGION", "us-east-1")
NAMESPACE = os.getenv("NAMESPACE", "default")
CONFIGMAP_NAME = "gcp-multi-topic-configmap"
SCALEDOBJECT_NAME = "gcp-multi-topic-scaler"

# The dynamic payload of topics we want the cluster to process right now
DYNAMIC_TOPIC_SUBSCRIPTIONS = [
    "projects/wired-sign-858/subscriptions/keda-poc-sub",
    # If you create these and uncomment them, KEDA instantly scales for them too!
    # "projects/wired-sign-858/subscriptions/another-sub-1",
    # "projects/wired-sign-858/subscriptions/another-sub-2"
]

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

def call_eks_api(endpoint, ca_data, token, path, method, payload):
    """Executes the REST HTTP request to the EKS Control Plane."""
    url = f"{endpoint}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/merge-patch+json" # Merge patch for simpler partial updates
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
    Because KEDA needs a specific block for every topic, we build it in a loop!
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
    
    print(f"--> [STEP 1] Patching KEDA ScaledObject with {len(triggers)} discrete triggers...")
    call_eks_api(endpoint, ca_data, token, path, "PATCH", payload)
    print("--> [SUCCESS] KEDA Autoscaler successfully updated. It is now watching the new queue depths.")

def patch_pod_configmap(endpoint, ca_data, token, topics):
    """Updates the ConfigMap causing Stakater-Reloader to transparently restart the apps."""
    payload = {"data": {"topics.json": json.dumps({"topics": topics})}}
    path = f"/api/v1/namespaces/{NAMESPACE}/configmaps/{CONFIGMAP_NAME}"
    
    print(f"--> [STEP 2] Patching ConfigMap with {len(topics)} topics...")
    call_eks_api(endpoint, ca_data, token, path, "PATCH", payload)
    print("--> [SUCCESS] ConfigMap Patched. The EKS Pods are now rolling restarts to mount the new config.")

def lambda_handler(event, context):
    print("===================================================================")
    print(f"Starting Multi-Topic KEDA Patcher -> Cluster: {EKS_CLUSTER_NAME}")
    print("===================================================================")
    
    try:
        endpoint, ca_data = get_eks_cluster_info(EKS_CLUSTER_NAME)
        token = get_eks_token(EKS_CLUSTER_NAME)
    except Exception as e:
        print(f"[FATAL] Failed to authenticate to EKS: {e}")
        return {"statusCode": 500, "body": str(e)}

    # DEFENSIVE TRANSACTION LOGIC
    # We enforce strict ordering. If KEDA fails to patch, the script crashes, 
    # preventing the configmap from being updated with orphaned topics.
    try:
        # 1. Update the Autoscaler
        patch_keda_autoscaler(endpoint, ca_data, token, DYNAMIC_TOPIC_SUBSCRIPTIONS)
        
        # 2. Update the App Config
        patch_pod_configmap(endpoint, ca_data, token, DYNAMIC_TOPIC_SUBSCRIPTIONS)
        
        return {"statusCode": 200, "body": "Transactional Patch Complete! EKS is scaling and rolling pods."}
        
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')
        print(f"\n[FATAL] EKS API Transaction Aborted! -> HTTP {e.code}: {body}")
        
    except Exception as e:
        print(f"\n[FATAL] Unexpected error aborted transaction: {e}")

if __name__ == "__main__":
    lambda_handler(None, None)
