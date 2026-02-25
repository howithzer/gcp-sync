import os
import signal
import sys
import time
import json
import asyncio
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Simulate a config file mounted via ConfigMap
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config/topics.json")

class TopicConfigHandler(FileSystemEventHandler):
    def __init__(self, callback):
        self.callback = callback

    def on_modified(self, event):
        # Kubernetes ConfigMaps update via symlinks, so we watch the directory
        if not event.is_directory and event.src_path.endswith("topics.json"):
            print(f"[Watcher] Detected modification in {event.src_path}")
            self.callback()

class PubSubSyncManager:
    def __init__(self):
        self.active_topics = set()
        self.tasks = {}
        self.running = True

    def load_config(self):
        try:
            with open(CONFIG_PATH, 'r') as f:
                data = json.load(f)
                return set(data.get("topics", []))
        except Exception as e:
            print(f"[Error] Failed to load config: {e}")
            return set()

    async def streaming_pull_mock(self, topic):
        print(f"[Task] Starting STREAMING PULL for topic: {topic}")
        try:
            while self.running:
                # Simulate active listening
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            print(f"[Task] STOPPING stream for topic: {topic}")
            raise

    async def reconcile_topics(self):
        current_topics = self.load_config()
        
        # Identify new topics
        new_topics = current_topics - self.active_topics
        for topic in new_topics:
            print(f"[Manager] Found new topic: {topic}")
            task = asyncio.create_task(self.streaming_pull_mock(topic))
            self.tasks[topic] = task

        # Identify removed topics
        removed_topics = self.active_topics - current_topics
        for topic in removed_topics:
            print(f"[Manager] Found removed topic: {topic}. Cancelling task...")
            self.tasks[topic].cancel()
            del self.tasks[topic]

        self.active_topics = current_topics
        print(f"[Status] Currently active topics ({len(self.active_topics)}): {self.active_topics}")

    def trigger_reconcile(self):
        # Thread-safe trigger for the asyncio loop
        loop = asyncio.get_running_loop()
        loop.call_soon_threadsafe(asyncio.create_task, self.reconcile_topics())

    def shutdown(self, signum, frame):
        print(f"\n[Shutdown] Received signal {signum}. Gracefully shutting down...")
        self.running = False
        for topic, task in self.tasks.items():
            task.cancel()
        sys.exit(0)

async def main():
    manager = PubSubSyncManager()
    
    # Handle Reloader's rolling restart signals
    signal.signal(signal.SIGTERM, manager.shutdown)
    signal.signal(signal.SIGINT, manager.shutdown)

    print(f"Starting GCP Sync Manager. Config path: {CONFIG_PATH}")
    
    # 1. Initial Load
    await manager.reconcile_topics()

    # 2. Setup Watchdog for Dynamic File Watcher approach
    config_dir = os.path.dirname(CONFIG_PATH)
    if os.path.exists(config_dir):
        observer = Observer()
        handler = TopicConfigHandler(callback=manager.trigger_reconcile)
        observer.schedule(handler, path=config_dir, recursive=False)
        observer.start()
        print(f"Started file watcher on directory: {config_dir}")
    else:
        print(f"[Warning] Config directory {config_dir} does not exist. Running static.")

    # Keep alive
    while manager.running:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
