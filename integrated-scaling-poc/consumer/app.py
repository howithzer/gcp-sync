import os
import json
import time
import signal
import sys
from google.cloud import pubsub_v1

# -------------------------------------------------------------------
# Phase 3: Integrated Multi-Topic Consumer
# Reads multiple topics from the injected ConfigMap.
# If ONE topic receives a poison message, it fails gracefully without
# crashing the pod or taking down the other active streams!
# -------------------------------------------------------------------

CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config/topics.json")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "wired-sign-858")

IS_SHUTTING_DOWN = False
SUBSCRIBER = None
STREAM_FUTURES = []

def callback(message: pubsub_v1.subscriber.message.Message, subscription_name: str) -> None:
    """
    Message handler. Wrapped in try/except for Graceful Degradation.
    """
    global IS_SHUTTING_DOWN
    
    if IS_SHUTTING_DOWN:
        message.nack()
        return

    try:
        # 1. Parse Payload
        payload = message.data.decode("utf-8")
        
        # 2. Simulated "Poison Message" Logic for Testing
        # If we see "POISON" in the payload, we purposefully throw a fatal Exception
        if "POISON" in payload:
            raise ValueError("Invalid schema detected in payload!")
            
        poc_id = payload[:30] + "..." if len(payload) > 30 else payload
        print(f"[{time.strftime('%X')}] [SUCCESS] Pod {os.getenv('HOSTNAME', 'local')} processed message '{poc_id}' from {subscription_name}")
        
        # 3. Simulate work (forces KEDA to scale)
        time.sleep(1.0) 
        
        # 4. Acknowledge the message so GCP removes it from the backlog
        message.ack()
        
    except Exception as e:
        # -------------------------------------------------------------------
        # GRACEFUL DEGRADATION:
        # We catch the exception, log it uniquely, and NACK the message.
        # GCP will throw it back in the queue (or eventually into a DLQ),
        # but the pod itself DOES NOT CRASH. The other topics continue pulling!
        # -------------------------------------------------------------------
        print(f"[{time.strftime('%X')}] 🚨 [ERROR] Topic {subscription_name} failed. Reason: {e}")
        message.nack()


def load_topics_from_config():
    """Reads the JSON array of GCP subscriptions injected by KEDA/Stakater."""
    try:
        with open(CONFIG_PATH, 'r') as f:
            data = json.load(f)
            return data.get("topics", [])
    except Exception as e:
        print(f"[{time.strftime('%X')}] FATAL: Could not read ConfigMap at {CONFIG_PATH}: {e}")
        sys.exit(1)

def shutdown_handler(signum, frame):
    """Graceful disconnection from ALL active GCP streams."""
    global IS_SHUTTING_DOWN
    print(f"\n[{time.strftime('%X')}] SIGTERM {signum} received. Safe shutdown initiated.")
    IS_SHUTTING_DOWN = True
    
    for future in STREAM_FUTURES:
        future.cancel()
        
    if SUBSCRIBER:
        SUBSCRIBER.close()
        
    print(f"[{time.strftime('%X')}] All {len(STREAM_FUTURES)} GCP Streams closed. Shutting down cleanly.")
    sys.exit(0)

def main():
    global SUBSCRIBER, STREAM_FUTURES
    
    # 1. Read topics.json configmap
    topics = load_topics_from_config()
    if not topics:
        print(f"[{time.strftime('%X')}] No topics found in {CONFIG_PATH}. Exiting.")
        sys.exit(0)
        
    # Bind K8s termination signals
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    SUBSCRIBER = pubsub_v1.SubscriberClient()

    print(f"[{time.strftime('%X')}] KEDA Phase 3 Multi-Topic Consumer Pod: {os.getenv('HOSTNAME', 'local')}")
    print(f"[{time.strftime('%X')}] Discovered {len(topics)} topics in config block. Opening parallel streams...")
    
    # 2. Open a dedicated asynchronous gRPC stream for EVERY topic in the list
    for topic_path in topics:
        def callback_wrapper(msg, sub=topic_path):
            return callback(msg, sub)
            
        future = SUBSCRIBER.subscribe(topic_path, callback=callback_wrapper)
        STREAM_FUTURES.append(future)
        print(f"  -> Stream Connected: {topic_path.split('/')[-1]}")

    print(f"[{time.strftime('%X')}] Ready! Now processing {len(topics)} topics simultaneously.")

    # 3. Block forever while the background GCP threads pull messages
    try:
        for future in STREAM_FUTURES:
            future.result()
    except TimeoutError:
        print("Stream future timed out")
    except Exception as e:
        print(f"[{time.strftime('%X')}] Consumer loop broke: {e}")
        
    shutdown_handler(15, None)

if __name__ == "__main__":
    main()
