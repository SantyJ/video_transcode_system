import requests, threading, os, random, time, uuid

NODES = os.getenv("TARGET_NODES", "").split(",")
FILE = os.getenv("UPLOAD_FILE", "test.mp4")  # Must be a video file
CONCURRENCY = int(os.getenv("CONCURRENCY", 20))
PUSH_INTERVAL = 5  # seconds

# Metrics (shared across threads)
success_count = 0
total_count = 0
latency_list = []
metrics_lock = threading.Lock()

def wait_until_available(node, retries=10, delay=5):
    url = f"http://{node}:5000"
    for _ in range(retries):
        try:
            r = requests.get(f"{url}/health", timeout=2)
            if r.status_code == 200:
                print(f"{url} is healthy.")
                return True
        except requests.RequestException:
            pass
        time.sleep(delay)
    print(f"{url} did not respond after {retries} retries.")
    return False

print("Checking node availability...")
NODES = [node for node in NODES if wait_until_available(node)]

if not NODES:
    print("No available nodes to upload to.")
    exit(1)

def upload_task(i):
    global success_count, total_count, latency_list

    attempt = 0
    success = False
    used_nodes = set()

    while attempt < 5 and not success:
        attempt += 1
        # with metrics_lock:
        #     total_count += 1

        available_nodes = list(set(NODES) - used_nodes)
        if not available_nodes:
            available_nodes = NODES
            used_nodes.clear()
        node = random.choice(available_nodes)
        used_nodes.add(node)
        url = f"http://{node}:5000/upload"

        try:
            with open(FILE, 'rb') as f:
                files = {"file": (os.path.basename(FILE), f, "video/mp4")}
                start = time.time()
                r = requests.post(url, files=files, timeout=15)
                rtt = (time.time() - start) * 1000  # ms

                if r.status_code == 200:
                    output_file = f"output_{i}.mp4"
                    with open(output_file, "wb") as out:
                        out.write(r.content)

                    server_latency = float(r.headers.get("X-Processing-Latency", "0"))
                    total_latency = server_latency + rtt
                    # network_rtt = rtt - server_latency
                    # total_latency = server_latency + network_rtt

                    with metrics_lock:
                        success_count += 1
                        total_count += 1
                        latency_list.append(total_latency)
                    print(f"[{i}] Uploaded to {url} | total count: {total_count} | success: {success_count} | server latency: {round(server_latency,2)} ms | Latency: {round(total_latency,2)} ms")
                    success = True
                else:
                    with metrics_lock:
                        total_count += 1
                    print(f"[{i}] Attempt {attempt}: Status {r.status_code}")
        except Exception as e:
            with metrics_lock:
                total_count += 1
            print(f"[{i}] Attempt {attempt}: Exception: {e}")

def push_metrics_periodically():
    global success_count, total_count, latency_list
    while True:
        time.sleep(PUSH_INTERVAL)
        print("In push func")
        with metrics_lock:
            if total_count == 0:
                continue
            throughput = round((success_count / total_count) * 100, 1)
            avg_latency = round(sum(latency_list) / len(latency_list), 2) if latency_list else None

            payload = {
                "node_id": "client",
                "throughput": throughput,
                "avg_latency": avg_latency,
                "success": success_count,
                "total": total_count,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            
            print(f"Push payload: {payload}")
            try:
                requests.post(f"http://central_server:8005/update_metrics", json=payload, timeout=5)
                print(f"[Metrics] Sent: {payload}")
            except Exception as e:
                print(f"[Metrics] Failed to push: {e}")

# Start metric pusher thread
metrics_thread = threading.Thread(target=push_metrics_periodically)
metrics_thread.start()

# Start concurrent uploads
threads = [threading.Thread(target=upload_task, args=(i,)) for i in range(CONCURRENCY)]
[t.start() for t in threads]
[t.join() for t in threads]

print("All uploads complete. Keeping client alive to push final metrics...")

try:
    metrics_thread.join()  # block here forever unless interrupted
except KeyboardInterrupt:
    print("Interrupted. Exiting...")

# Keep the script running for metric pushes (e.g., CLI dashboards)
# try:
#     while True:
#         time.sleep(60)
# except KeyboardInterrupt:
#     print("Exiting...")
