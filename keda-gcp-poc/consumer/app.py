import os
import time
import signal
import sys
from google.cloud import pubsub_v1

# -------------------------------------------------------------------
# KEDA POC Consumer
# This app runs inside EKS and pulls via GCP StreamingPull.
# It intentionally processes messages very slowly to force KEDA
# to scale out the deployment horizontally to handle the backlog!
# -------------------------------------------------------------------

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "poc-project")
SUBSCRIPTION_ID = os.getenv("GCP_SUBSCRIPTION_ID", "keda-poc-sub")

# Global variables for clean shutdown
IS_SHUTTING_DOWN = False
SUBSCRIBER = None
STREAMING_PULL_FUTURE = None

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    """
    Called asynchronously by the GCP SDK background thread whenever a message arrives.
    """
    if IS_SHUTTING_DOWN:
        # If KEDA is scaling us DOWN because the queue is empty, and a stray message 
        # hits this dying pod, immediately throw it back in the queue for a live pod.
        message.nack()
        return
        
    poc_id = "Unknown"
    try:
        # We don't actually care about the payload, just the queue depth
        payload = message.data.decode("utf-8")
        poc_id = payload[:30] + "..." if len(payload) > 30 else payload
    except Exception:
        pass

    print(f"[{time.strftime('%X')}] Pod {os.getenv('HOSTNAME', 'local')} received message: {poc_id}")
    
    # -------------------------------------------------------------------
    # THE KEDA MAGIC DELAY
    # By sleeping 1 second per message, a single pod can only process 
    # 60 messages a minute. If the publisher dumps 1,000 messages instantly, 
    # KEDA will realize this pod is too slow and spawn 19 more pods to help!
    # -------------------------------------------------------------------
    time.sleep(1.0) 
    
    # Acknowledge the message so GCP removes it from the backlog, lowering the KEDA metric
    message.ack()

def shutdown_handler(signum, frame):
    """Intercepts KEDA/HPA scale-down signals to close the GCP stream gracefully."""
    print(f"\n[{time.strftime('%X')}] Received SIGTERM {signum}. KEDA is scaling down this pod...")
    
    global IS_SHUTTING_DOWN
    IS_SHUTTING_DOWN = True
    
    if STREAMING_PULL_FUTURE:
        STREAMING_PULL_FUTURE.cancel()
        
    if SUBSCRIBER:
        SUBSCRIBER.close()
        
    print(f"[{time.strftime('%X')}] GCP stream closed. Shutting down cleanly.")
    sys.exit(0)

def main():
    global SUBSCRIBER, STREAMING_PULL_FUTURE
    
    # Bind K8s termination signals
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    SUBSCRIBER = pubsub_v1.SubscriberClient()
    subscription_path = SUBSCRIBER.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    print(f"[{time.strftime('%X')}] Starting KEDA POC Consumer Pod: {os.getenv('HOSTNAME', 'local')}")
    print(f"[{time.strftime('%X')}] Opening StreamingPull to {subscription_path}...")
    
    # Open the asynchronous gRPC stream
    STREAMING_PULL_FUTURE = SUBSCRIBER.subscribe(subscription_path, callback=callback)

    # Block the main thread forever while the background GCP threads pull messages
    try:
        STREAMING_PULL_FUTURE.result()
    except TimeoutError:
        STREAMING_PULL_FUTURE.cancel()
    except Exception as e:
        print(f"[{time.strftime('%X')}] Consumer loop broke: {e}")
        
if __name__ == "__main__":
    main()
