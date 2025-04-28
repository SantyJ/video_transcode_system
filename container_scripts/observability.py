# observability.py

import time
import os

class Observability:
    def __init__(self, log_file_path="metrics.log"):
        self.cache_hits = 0
        self.cache_misses = 0
        self.offloads = 0
        self.local_processings = 0
        self.start_time = time.time()
        self.log_file_path = log_file_path

        # Clear old log on startup
        with open(self.log_file_path, "w") as f:
            f.write("timestamp, event, details\n")

    def log(self, event, details=""):
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_msg = f"{timestamp}, {event}, {details}"
        print(f"[Observability] {log_msg}")

        with open(self.log_file_path, "a") as f:
            f.write(log_msg + "\n")

    def cache_hit(self):
        self.cache_hits += 1
        self.log("cache_hit")

    def cache_miss(self):
        self.cache_misses += 1
        self.log("cache_miss")

    def offload_success(self, peer):
        self.offloads += 1
        self.log("offload", f"peer={peer}")

    def local_processing(self):
        self.local_processings += 1
        self.log("local_processing")

    def summary(self):
        elapsed = time.time() - self.start_time
        self.log("summary", f"hits={self.cache_hits}, misses={self.cache_misses}, offloads={self.offloads}, local={self.local_processings}, uptime={elapsed:.1f}s")
