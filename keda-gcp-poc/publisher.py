import os
import json
import time
from google.cloud import pubsub_v1

# -------------------------------------------------------------------
# KEDA POC Publisher
# Run this locally to artificially inflate the GCP Pub/Sub backlog!
# -------------------------------------------------------------------

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "poc-project")
TOPIC_ID = os.getenv("GCP_TOPIC_ID", "keda-poc-topic")

def publish_messages(num_messages=1000):
    """
    Rapidly publishes N dummy JSON messages to the specified Pub/Sub topic to simulate a traffic spike.
    """
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    
    print(f"[{time.strftime('%X')}] Preparing to publish {num_messages} messages to {topic_path}...")
    
    futures = []
    
    for i in range(1, num_messages + 1):
        message = {
            "poc_id": f"keda-test-{i}",
            "payload": "This is a dummy message to test KEDA scale-from-zero!",
            "timestamp": time.time()
        }
        
        # Pub/Sub expects bytes
        data = json.dumps(message).encode("utf-8")
        
        # Publish asynchronously
        future = publisher.publish(topic_path, data)
        futures.append(future)
        
        if i % 100 == 0:
            print(f"  -> Dispatched {i} messages...")
            
    print(f"[{time.strftime('%X')}] Waiting for all publish confirmations from GCP...")
    
    # Block until all messages are successfully acknowledged by GCP
    pub_count = 0
    for future in futures:
        future.result()
        pub_count += 1
        
    print(f"[{time.strftime('%X')}] SUCCESS! {pub_count} messages are now sitting in the GCP topic.")
    print("\nNext Step:")
    print("  1. Watch your EKS cluster with: kubectl get pods -w")
    print("  2. KEDA will detect the queue depth spike and scale your deployment UP!")

if __name__ == "__main__":
    # You can change this number via an environment variable if you want a bigger spike
    target_messages = int(os.getenv("MESSAGE_COUNT", "1000"))
    publish_messages(target_messages)
