import requests, threading, os, random, time

NODES = os.getenv("TARGET_NODES", "").split(",")
FILE = os.getenv("UPLOAD_FILE", "test.mp4")  # Should be a video file
CONCURRENCY = int(os.getenv("CONCURRENCY", 20))

def wait_until_available(node, retries=10, delay=5):
    url = f"http://{node}:5000"
    for i in range(retries):
        try:
            res = requests.get(f"{url}/health", timeout=2)
            if res.status_code == 200:
                print(f"{url} is healthy.")
                return True
        except requests.RequestException:
            pass
        time.sleep(delay)
    print(f"{url} did not respond after {retries} retries.")
    return False

# Check node's availability
print("Checking node availability...")
NODES = [node for node in NODES if wait_until_available(node)]

if not NODES:
    print("No available nodes to upload to.")
    exit(1)

def upload_task(i):
    node = random.choice(NODES)
    url = f"http://{node}:5000"
    try:
        with open(FILE, 'rb') as f:
            files = {"file": (os.path.basename(FILE), f, "video/mp4")}
            r = requests.post(f"{url}/upload", files=files)

            if r.status_code == 200:
                output_file = f"output_{i}.mp4"
                with open(output_file, "wb") as out:
                    out.write(r.content)
                print(f"[{i}] Uploaded to {url} and saved result as {output_file}")
            else:
                print(f"[{i}] Failed to upload to {url}: Status {r.status_code}")
    except Exception as e:
        print(f"[{i}] Exception uploading to {url}: {e}")

# Start concurrent uploads
threads = [threading.Thread(target=upload_task, args=(i,)) for i in range(CONCURRENCY)]
[t.start() for t in threads]
[t.join() for t in threads]
