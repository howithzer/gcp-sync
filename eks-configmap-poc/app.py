import os
import time
import json
import signal
import sys

# Simulation of the ConfigMap path in the container
# Default is /app/config/topics.json based on standard EKS volume mount path
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config/topics.json")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "10"))

# Global shutdown flag
IS_SHUTTING_DOWN = False

def shutdown_handler(signum, frame):
    """
    Catches the SIGTERM signal thrown by Kubernetes or Reloader during a rolling restart.
    """
    global IS_SHUTTING_DOWN
    IS_SHUTTING_DOWN = True
    print(f"\n[Shutdown] Received SIGTERM signal {signum}. Simulating graceful shutdown...")
    time.sleep(2) # Give the app a couple seconds to cleanly drop connections
    print("[Shutdown] Clean exit completed.")
    sys.exit(0)

def main():
    # Bind the Kubernetes termination signals
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    print(f"[Init] Starting ConfigMap Proof of Concept app...")
    print(f"[Init] Watching path: {CONFIG_PATH}")
    
    # In a real app, you'd only read the config ONCE at startup because Reloader
    # handles restarting the pod when the ConfigMap changes. 
    # However, to prove the Volume Mount updates physically on disk, we will loop and read it.
    
    while not IS_SHUTTING_DOWN:
        try:
            with open(CONFIG_PATH, 'r') as f:
                data = json.load(f)
                topics = data.get("topics", [])
                
            print(f"[Active] Found {len(topics)} topics in config file:")
            for t in topics:
                print(f"  -> {t}")
        except FileNotFoundError:
            print(f"[Warning] Config file not found at {CONFIG_PATH}. Waiting for Volume Mount...")
        except Exception as e:
            print(f"[Error] Failed to parse config file: {e}")
            
        print(f"[Sleep] Waiting {POLL_INTERVAL} seconds before next check...\n")
        time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
