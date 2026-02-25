import os
import json
import base64
import urllib.request
import urllib.error
import ssl
import boto3
from botocore.signers import RequestSigner

# -------------------------------------------------------------------
# EKS POC Configuration
# Replace these with your actual EKS cluster details
# -------------------------------------------------------------------
EKS_CLUSTER_NAME = os.getenv("EKS_CLUSTER_NAME", "my-dev-eks-cluster")
EKS_REGION = os.getenv("EKS_REGION", "us-east-1")
NAMESPACE = os.getenv("NAMESPACE", "data-ingestion")
CONFIGMAP_NAME = "gcp-sync-streaming-configmap"

def get_eks_token(cluster_name):
    """Generates AWS STS token for authenticating against EKS."""
    session = boto3.Session()
    client = session.client('sts', region_name=EKS_REGION)
    signer = RequestSigner(
        client.meta.service_model.service_id,
        EKS_REGION, 'sts', 'v4',
        session.get_credentials(), session.events
    )
    url = signer.generate_presigned_url(
        {'Action': 'GetCallerIdentity', 'Version': '2011-06-15'},
        region_name=EKS_REGION, expires_in=60, operation_name=''
    )
    return 'k8s-aws-v1.' + base64.urlsafe_b64encode(url.encode('utf-8')).decode('utf-8').rstrip('=')

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
        "Content-Type": "application/strategic-merge-patch+json"
    }
    
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    
    ca_bytes = base64.b64decode(ca_data)
    ssl_context = ssl.create_default_context(cadata=ca_bytes.decode('utf-8'))
    
    response = urllib.request.urlopen(req, context=ssl_context, timeout=10)
    return json.loads(response.read().decode('utf-8'))

def lambda_handler(event, context):
    """
    POC Lambda entry point. Wait for manual trigger, authenticates with EKS,
    and injects DUMMY logic into the ConfigMap to trigger the Reloader bounce.
    """
    print(f"Starting EKS POC Patcher against cluster '{EKS_CLUSTER_NAME}'...")
    
    # 1. Authenticate to EKS
    try:
        endpoint, ca_data = get_eks_cluster_info(EKS_CLUSTER_NAME)
        token = get_eks_token(EKS_CLUSTER_NAME)
        print(f"Successfully authenticated. EKS Endpoint: {endpoint}")
    except Exception as e:
        print(f"Failed to authenticate to EKS: {e}")
        return {"statusCode": 500, "body": str(e)}
        
    # 2. Define the Dummy Topics to inject
    # When this Lambda runs, the pod should restart and start printing these instead!
    dummy_topics = [
        "projects/poc-project/subscriptions/order-stream",
        "projects/poc-project/subscriptions/NEW-test-topic-1",
        "projects/poc-project/subscriptions/NEW-test-topic-2"
    ]
    
    # 3. Patch the ConfigMap
    try:
        cm_payload = {"data": {"topics.json": json.dumps({"topics": dummy_topics})}}
        path = f"/api/v1/namespaces/{NAMESPACE}/configmaps/{CONFIGMAP_NAME}"
        
        print(f"Patching ConfigMap '{CONFIGMAP_NAME}' with {len(dummy_topics)} topics...")
        call_eks_api(endpoint, ca_data, token, path, "PATCH", cm_payload)
        
        print("Success! ConfigMap patched. Reloader should now trigger a pod restart.")
        return {"statusCode": 200, "body": "ConfigMap Patched successfully."}
        
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')
        print(f"EKS API Error: {e.code} - {body}")
        return {"statusCode": e.code, "body": body}
    except Exception as e:
        print(f"Unexpected error: {e}")
        return {"statusCode": 500, "body": str(e)}

# For local testing from your laptop using AWS CLI credentials:
if __name__ == "__main__":
    lambda_handler(None, None)
