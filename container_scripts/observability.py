# observability.py

import time
import os

class Observability:
    def __init__(self, log_file_path="metrics.log"):
        self.cache_hits = 0
        self.cache_misses = 0
        self.offloads = 0
        self.offloads_local = 0
        self.offloads_remote = 0
        self.local_responses = 0
        self.remote_responses = 0
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
        self.log("offload_total", f"peer={peer}")
    
    def offload_success_local(self, peer):
        self.offloads_local += 1
        self.log("offload_local", f"peer={peer}")
    
    def offload_success_remote(self, peer):
        self.offloads_remote += 1
        self.log("offload_remote", f"peer={peer}")
    
    def local_response(self, peer):
        self.local_responses += 1
        self.log("local_response", f"peer={peer}")
    
    def remote_response(self, peer):
        self.remote_responses += 1
        self.log("remote_response", f"peer={peer}")

    def local_processing(self):
        self.local_processings += 1
        self.log("local_processing")

    def summary(self):
        elapsed = time.time() - self.start_time
        self.log("summary", f"hits={self.cache_hits}, misses={self.cache_misses}, offloads_total={self.offloads}, offloads_same_region={self.offloads_local}, offloads_cross_region={self.offloads_remote}, responses_same_region={self.local_responses}, responses_cross_region={self.remote_responses}, local_processings={self.local_processings}, uptime={elapsed:.1f}s")
