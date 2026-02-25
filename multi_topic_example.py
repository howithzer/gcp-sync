import time
import json
import os
import signal
import sys
from collections import OrderedDict
from datetime import datetime, timezone

# --- MOCKS FOR USER'S CUSTOM IMPORTS ---
# from logger import get_logger
# from gcp_connection import get_pubsub_subscriber
# from send_sqs import send_to_sqs

# Mock logger for demonstration
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gcp-sync")

# Replace with your project ID
project_id = os.getenv('PROJECT_ID', 'my-gcp-project')
env = os.getenv('ENV', 'dev_')

# Global exit flag for graceful shutdown (to handle Reloader SIGTERMs safely)
IS_SHUTTING_DOWN = False

def get_pubsub_subscriber():
    from google.cloud import pubsub_v1
    return pubsub_v1.SubscriberClient()

def send_to_sqs(payload):
    # Mocking user's SQS module
    return True, {"MessageId": "mock-sqs-id"}

# --- END MOCKS ---

def callback(message):
    """
    Handles incoming Pub/Sub messages.
    (This is identical to your existing callback processing logic)
    """
    if IS_SHUTTING_DOWN:
        # If the pod is spinning down, NACK immediately so GCP re-delivers it to a new pod quickly
        message.nack()
        return

    message_id = message.message_id
    publish_time = message.publish_time.isoformat()
    data = message.data.decode("utf-8")
    
    try:
        data_json = json.loads(data)
    except Exception as e:
        logger.error(f"Failed to parse message data as JSON: {e}")
        message.nack()
        return
        
    try:
        correlation_id = None
        idempotency_key_resource = None
        period_reference = None
        
        if isinstance(data_json, dict) and "_metadata" in data_json:
            meta = data_json["_metadata"]
            correlation_id = meta.get("correlationId")
        
        payload_section = data_json.get("payload", {})
        for value in payload_section.values():
            if isinstance(value, list) and value:
                first_item = value[0]
                nested_meta = first_item.get("_metadata", {})
                idempotency_key_resource = nested_meta.get("idempotencyKeyResource")
                period_reference = nested_meta.get("periodReference")
                break
                
        if isinstance(data_json, dict):
            orig_metadata = data_json.get("_metadata", {})
            ordered_payload = OrderedDict()
            
            if isinstance(orig_metadata, dict):
                event = orig_metadata.get("event", {})
                event_type = event.get("eventType", "unknown")
                ordered_payload["topic"] = f"{env}{event_type}"
            
            ordered_payload["message_id"] = message_id
            ordered_payload["publish_time"] = publish_time
            ordered_payload["correlation_id"] = correlation_id
            ordered_payload["idempotency_key"] = idempotency_key_resource
            ordered_payload["period_reference"] = period_reference
            ordered_payload["dl_processing_time"] = int(datetime.now(timezone.utc).timestamp())
            
            for k, v in data_json.items():
                if k not in ordered_payload:
                    ordered_payload[k] = v
            ordered_payload["_metadata"] = orig_metadata
                
        payload = json.dumps(ordered_payload)
        
        # Send to AWS
        flag, response = send_to_sqs(payload)
        
    except Exception as e:
        logger.error(f"Error while extracting fields: {e}")
        message.nack()
        return

    if flag:
        message.ack()
    else:
        logger.error("All retry attempts failed. Nacking message.")
        message.nack()


def start_multi_bridge(subscription_ids):
    """
    Subscribes to MULTIPLE Pub/Sub topics concurrently.
    Because the GCP PubSub Subscriber client relies on gRPC, it already has an
    internal asynchronous ThreadPool. We do NOT need to write complex asyncio loops.
    We just need to initiate the subscription for each topic and store the futures!
    """
    subscriber = get_pubsub_subscriber()
    futures = []
    
    logger.info(f"Initializing bridge for {len(subscription_ids)} subscriptions.")
    
    for sub_id in subscription_ids:
        sub_path = subscriber.subscription_path(project_id, sub_id)
        try:
            # Subscribe returns a StreamingPullFuture instantly. It does NOT block.
            streaming_pull_future = subscriber.subscribe(sub_path, callback=callback)
            futures.append(streaming_pull_future)
            logger.info(f"Subscribed to {sub_path}")
        except Exception as e:
            logger.error(f"Failed to subscribe to {sub_path}: {e}")

    if not futures:
        logger.error("No valid subscriptions active. Exiting.")
        return

    def shutdown_handler(signum, frame):
        """
        Catches the SIGTERM signal thrown by Reloader when it wants to do a rolling restart.
        Cancels all active listener futures so we don't hold onto messages indefinitely.
        """
        global IS_SHUTTING_DOWN
        IS_SHUTTING_DOWN = True
        logger.info("\n[Shutdown] Received shutdown signal from Kubernetes. Cancelling listeners...")
        for future in futures:
            future.cancel()

    # Bind the Reloader/Kubernetes termination signals
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    logger.info("All subscriptions active. Main thread entering sleep loop...")
    
    try:
        # Instead of 24 hours, sleep indefinitely. If Reloader forces an update, 
        # it will send SIGTERM and jump to the shutdown_handler.
        while not IS_SHUTTING_DOWN:
            time.sleep(1)
    except Exception as e:
        logger.error(f"Exception in main bridge loop: {e}")
    finally:
        logger.info("Waiting for background streams to close cleanly...")
        for future in futures:
            try:
                future.result(timeout=10) # Block until the thread explicitly exits
            except Exception as e:
                # Expected when cancelled
                pass
        logger.info("Shutdown complete.")

if __name__ == "__main__":
    # --- DYNAMIC CONFIGMAP DISCOVERY ---
    # Instead of pulling one `os.getenv("SUBSCRIPTION_ID")`, we read from the ConfigMap
    CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config/topics.json")
    
    try:
        with open(CONFIG_PATH, 'r') as f:
            config_data = json.load(f)
            topics_to_consume = config_data.get("topics", [])
    except FileNotFoundError:
        logger.error(f"ConfigMap file not found at {CONFIG_PATH}. Defaulting to empty.")
        topics_to_consume = []
        
    if topics_to_consume:
        start_multi_bridge(topics_to_consume)
    else:
        logger.error("No topics defined in ConfigMap to consume. Exiting pod.")
        sys.exit(1)
