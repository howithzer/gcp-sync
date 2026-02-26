import os
import json
import base64
import urllib.request
import urllib.error
import ssl
import boto3
from google.cloud import pubsub_v1
from botocore.signers import RequestSigner

# Environment configuration required for Lambda
EKS_CLUSTER_NAME = os.getenv("EKS_CLUSTER_NAME", "my-eks-cluster")
EKS_REGION = os.getenv("EKS_REGION", "us-east-1")
NAMESPACE = os.getenv("NAMESPACE", "data-ingestion")

# GCP Project ID
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "my-gcp-project")

def get_eks_token(cluster_name):
    """
    Generates a short-lived AWS IAM authentication token for the EKS API.
    This replicates the behavior of `aws eks get-token`.
    """
    session = boto3.Session()
    client = session.client('sts', region_name=EKS_REGION)
    service_id = client.meta.service_model.service_id
    
    signer = RequestSigner(
        service_id,
        EKS_REGION,
        'sts',
        'v4',
        session.get_credentials(),
        session.events
    )
    
    params = {
        'Action': 'GetCallerIdentity',
        'Version': '2011-06-15'
    }
    
    url = signer.generate_presigned_url(
        params,
        region_name=EKS_REGION,
        expires_in=60,
        operation_name=''
    )
    
    return 'k8s-aws-v1.' + base64.urlsafe_b64encode(url.encode('utf-8')).decode('utf-8').rstrip('=')

def get_eks_cluster_info(cluster_name):
    """
    Retrieves the EKS Cluster API Endpoint and Certificate Authority.
    """
    client = boto3.client('eks', region_name=EKS_REGION)
    response = client.describe_cluster(name=cluster_name)
    cluster = response['cluster']
    return cluster['endpoint'], cluster['certificateAuthority']['data']

def get_active_gcp_topics():
    """
    Uses the native google-cloud-pubsub SDK to fetch all active subscriptions.
    Requires GOOGLE_APPLICATION_CREDENTIALS environment variable.
    """
    subscriber = pubsub_v1.SubscriberClient()
    project_path = f"projects/{GCP_PROJECT_ID}"
    
    topics = []
    # Paginate through all subscriptions in the project
    for subscription in subscriber.list_subscriptions(request={"project": project_path}):
        topics.append(subscription.name)
        
    return topics

def call_eks_api(endpoint, ca_data, token, path, method, payload=None, content_type="application/json"):
    """
    Helper function to execute REST API calls against the EKS Control Plane.
    """
    url = f"{endpoint}{path}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }
    
    if payload:
        headers["Content-Type"] = content_type
        data = json.dumps(payload).encode('utf-8')
    else:
        data = None
        
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    
    # Configure SSL context using the EKS CA Certificate
    ca_bytes = base64.b64decode(ca_data)
    ssl_context = ssl.create_default_context(cadata=ca_bytes.decode('utf-8'))
    
    try:
        response = urllib.request.urlopen(req, context=ssl_context, timeout=10)
        return json.loads(response.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8')
        print(f"EKS API HTTPError: {e.code} - {body}")
        raise
    except Exception as e:
        print(f"EKS API Error: {str(e)}")
        raise

def lambda_handler(event, context):
    """
    Production Lambda Handler.
    """
    print("Starting EKS GCP Topic Sync...")
    
    # 1. Fetch live EKS connection data and authenticate via IAM Role
    endpoint, ca_data = get_eks_cluster_info(EKS_CLUSTER_NAME)
    token = get_eks_token(EKS_CLUSTER_NAME)
    
    # 2. Fetch live topics from GCP
    all_topics = get_active_gcp_topics()
    print(f"Discovered {len(all_topics)} total subscriptions in GCP.")
    
    # --- Example Business Logic Grouping ---
    streaming_topics = [t for t in all_topics if "-stream" in t]
    batch_topics = [t for t in all_topics if "-batch" in t]
    # ---------------------------------------
    
    # 3. Patch Streaming Group
    if streaming_topics:
        print(f"Patching Streaming Group with {len(streaming_topics)} topics.")
        
        # Patch ConfigMap (Targeted EXPLICITLY by the URL path ending in /configmaps/gcp-sync-streaming-configmap)
        try:
            print("Attempting to patch ConfigMap: gcp-sync-streaming-configmap...")
            cm_payload = {"data": {"topics.json": json.dumps({"topics": streaming_topics})}}
            call_eks_api(
                endpoint, ca_data, token,
                path=f"/api/v1/namespaces/{NAMESPACE}/configmaps/gcp-sync-streaming-configmap",
                method="PATCH",
                payload=cm_payload,
                content_type="application/strategic-merge-patch+json"
            )
            print("Successfully patched ConfigMap.")
        except Exception as e:
            print(f"CRITICAL: Failed to patch ConfigMap. Pods will not receive updated topics! Error: {e}")
            
        # Patch KEDA ScaledObject (Targeted EXPLICITLY by the URL path ending in /scaledobjects/gcp-sync-streaming-scaler)
        try:
            print("Attempting to patch ScaledObject: gcp-sync-streaming-scaler...")
            triggers = [{"type": "gcp-pubsub", "metadata": {"subscriptionName": t.split("/")[-1], "subscriptionSize": "500", "credentialsFromEnv": "GOOGLE_APPLICATION_CREDENTIALS"}} for t in streaming_topics]
            so_payload = {"spec": {"triggers": triggers}}
            call_eks_api(
                endpoint, ca_data, token,
                path=f"/apis/keda.sh/v1alpha1/namespaces/{NAMESPACE}/scaledobjects/gcp-sync-streaming-scaler",
                method="PATCH",
                payload=so_payload,
                content_type="application/merge-patch+json"
            )
            print("Successfully patched ScaledObject.")
        except Exception as e:
            print(f"CRITICAL: Failed to patch ScaledObject. KEDA autoscale math may be incorrect! Error: {e}")
        
    return {
        "statusCode": 200,
        "body": "Successfully synced GCP subscriptions to EKS."
    }
