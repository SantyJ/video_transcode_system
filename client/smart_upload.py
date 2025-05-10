import requests, threading, os, random, time, subprocess

# Environment variables
all_nodes = [node.strip() for node in os.getenv("TARGET_NODES", "").split(",") if node.strip()]
# FILE = os.getenv("UPLOAD_FILE", "test.mp4")
CONCURRENCY = int(os.getenv("CONCURRENCY", 20))
PUSH_INTERVAL = 5  # seconds
VIDEO_DIRS = os.getenv("VIDEO_DIRS", "./videos").split(",")

# Shared state
healthy_nodes = set()
success_count = 0
total_count = 0
latency_list = []
metrics_lock = threading.Lock()

# def get_video_files():
#     return [os.path.join(VIDEO_DIR, f) for f in os.listdir(VIDEO_DIR)
#             if f.lower().endswith((".mp4", ".mov", ".avi", ".mkv"))]

def get_video_files():
    video_files = []
    for dir_path in VIDEO_DIRS:
        dir_path = dir_path.strip()
        if not os.path.isdir(dir_path):
            continue
        for f in os.listdir(dir_path):
            if f.lower().endswith((".mp4", ".mov", ".avi", ".mkv")):
                video_files.append(os.path.join(dir_path, f))
    return video_files

def check_node_health(node):
    try:
        r = requests.get(f"http://{node}:5000/health", timeout=2)
        return r.status_code == 200
    except requests.RequestException:
        return False

def wait_until_available(node, retries=5, delay=5):
    for _ in range(retries):
        if check_node_health(node):
            print(f"http://{node}:5000 is healthy.")
            return True
        time.sleep(delay)
    print(f"http://{node}:5000 did not respond after {retries} retries.")
    return False

def refresh_node_health():
    while True:
        for node in all_nodes:
            if node not in healthy_nodes and check_node_health(node):
                print(f"[HealthCheck] {node} is back online.")
                with metrics_lock:
                    healthy_nodes.add(node)
        time.sleep(5)

def upload_task(i):
    global success_count, total_count, latency_list
    attempt = 0
    success = False
    used_nodes = set()

    while attempt < 5 and not success:
        attempt += 1
        with metrics_lock:
            available_nodes = list(healthy_nodes - used_nodes)

        if not available_nodes:
            with metrics_lock:
                available_nodes = list(healthy_nodes)
                used_nodes.clear()

        if not available_nodes:
            print(f"[{i}] No healthy nodes available at attempt {attempt}")
            time.sleep(2)
            continue

        # node = random.choice(available_nodes)
        node = random.choice(["node1","node4"]) # Send only to nodes in FRA region
        used_nodes.add(node)
        url = f"http://{node}:5000/upload"

        try:
            video_file = random.choice(get_video_files())
            with open(video_file, 'rb') as f:
                headers = {
                    "Content-Type": "video/mp4",
                    "X-Filename": os.path.basename(video_file)
                }
                
                start = time.time()
                r = requests.post(url, data=f, headers=headers, timeout=40)
                
                rtt = (time.time() - start) * 1000

                if r.status_code == 200:
                    output_file = f"output_{i}.mp4"
                    with open(output_file, "wb") as out:
                        out.write(r.content)

                    server_latency = float(r.headers.get("X-Processing-Latency", "0"))
                    total_latency = server_latency + rtt

                    with metrics_lock:
                        success_count += 1
                        total_count += 1
                        latency_list.append(total_latency)
                    print(f"[{i}] Uploaded to {url} | success: {success_count} | latency: {round(total_latency,2)} ms")
                    success = True
                else:
                    with metrics_lock:
                        total_count += 1
                        healthy_nodes.discard(node)
                    print(f"[{i}] Attempt {attempt}: Status {r.status_code} - Marking {node} unhealthy")

        except Exception as e:
            with metrics_lock:
                total_count += 1
                healthy_nodes.discard(node)
            print(f"[{i}] Attempt {attempt}: Exception: {e} - Marking {node} unhealthy")

def push_metrics_periodically():
    global success_count, total_count, latency_list
    while True:
        time.sleep(PUSH_INTERVAL)
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

        try:
            requests.post("http://central_server:8005/update_metrics", json=payload, timeout=5)
            print(f"[Metrics] Sent: {payload}")
        except Exception as e:
            print(f"[Metrics] Failed to push: {e}")

def clear_output_files():
    for fname in os.listdir("."):
        if fname.startswith("output_") and fname.endswith(".mp4"):
            try:
                os.remove(fname)
            except Exception as e:
                print(f"Error deleting {fname}: {e}")

def rotate_output_files():
    for fname in os.listdir("."):
        if fname.startswith("output_") and fname.endswith(".mp4"):
            new_name = fname.replace("output_", "output_prev_")
            try:
                os.replace(fname, new_name)
            except Exception as e:
                print(f"Error rotating {fname} → {new_name}: {e}")

# Initial node health check
print("Checking node availability...")
for node in all_nodes:
    if wait_until_available(node):
        healthy_nodes.add(node)

if not healthy_nodes:
    print("No healthy nodes available. Exiting.")
    exit(1)

# Start background threads
threading.Thread(target=push_metrics_periodically, daemon=True).start()
threading.Thread(target=refresh_node_health, daemon=True).start()

# Continuous upload batches
try:
    while True:
        print("\n=== Starting new upload batch ===")
        threads = [threading.Thread(target=upload_task, args=(i,)) for i in range(CONCURRENCY)]
        [t.start() for t in threads]
        [t.join() for t in threads]

        print("Upload batch complete. Rotating output files...")
        rotate_output_files()

        print(f"Sleeping {PUSH_INTERVAL} seconds before next batch...\n")
        time.sleep(PUSH_INTERVAL)

except KeyboardInterrupt:
    print("Interrupted. Exiting...")