from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
from cachetools import LRUCache
from contextlib import asynccontextmanager
import psutil, hashlib, time, asyncio, httpx, random, os, io
from typing import List, Dict
import subprocess
import tempfile

# Logging module
from observability import Observability
observability = Observability(log_file_path="metrics.log")

app = FastAPI()

job_queue = []
# cache: Dict[str, Dict] = {}
# Create an LRU Cache for transcoded videos (example: 100 entries)
cache = LRUCache(maxsize=100)
peer_status = {}
peer_list = []

@app.get("/")
def root():
    return {"message": "FastAPI node is running"}

@app.get("/health")
def health():
    return {
        "cpu": psutil.cpu_percent(),
        "queue_length": len(job_queue)
    }

@app.get("/peers")
def get_peers():
    # Always include self so other nodes can learn about us
    self_info = {"host": os.getenv("NODE_NAME"), "port": 5000}
    return {"peers": [self_info] + peer_list}

# @app.post("/upload")
# async def upload(file: UploadFile):
#     content = await file.read()
#     content_hash = hashlib.sha256(content).hexdigest()

#     # Check cache
#     if content_hash in cache and time.time() - cache[content_hash]["timestamp"] < 300:
#         return {"result": "cache-hit", "data": cache[content_hash]["result"]}

#     # Try offloading
#     best_peer = get_best_peer()
#     if best_peer and best_peer != "self":
#         try:
#             async with httpx.AsyncClient(timeout=2.0) as client:
#                 r = await client.post(f"http://{best_peer}/upload", files={"file": content})
#                 print(f"[Offload] Successfully offloaded to {best_peer}")
#                 return r.json()
#         except Exception as e:
#             print(f"[Offload] Failed to offload to {best_peer}: {e}")

#     # Local processing
#     job_queue.append(content_hash)
#     result = transcode(content)
#     cache[content_hash] = {"result": result, "timestamp": time.time()}
#     job_queue.remove(content_hash)
#     print(f"[Local] Processed job locally: {content_hash}")
#     return {"result": "processed-locally", "data": result}

@app.get("/cache_stats")
def cache_stats():
    total_items = len(cache)
    total_size = sum(len(video) for video in cache.values())
    cached_hashes = list(cache.keys())

    return {
        "cached_videos": total_items,
        "approx_total_size_bytes": total_size,
        "approx_total_size_MB": round(total_size / (1024 * 1024), 2),
        "cached_hashes": cached_hashes
    }

@app.post("/upload")
async def upload(file: UploadFile = File(...)):
    content = await file.read()

    if not file.content_type.startswith("video/"):
        return {"error": "Only video files are supported."}

    content_hash = hashlib.sha256(content).hexdigest()

    # Check cache first
    if content_hash in cache:
        print(f"[Cache] Cache hit for {content_hash}")
        observability.cache_hit()
        transcoded_data = cache[content_hash]
        return StreamingResponse(io.BytesIO(transcoded_data), media_type="video/mp4")

    observability.cache_miss()

    # Try offloading
    best_peer = get_best_peer()
    if best_peer and best_peer != "self":
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                files = {"file": (file.filename, content, file.content_type)}
                r = await client.post(f"http://{best_peer}/upload", files=files)
                print(f"[Offload] Successfully offloaded to {best_peer}")
                observability.offload_success(best_peer)

                transcoded_data = r.content
                cache[content_hash] = transcoded_data  # Cache offloaded result too

                return StreamingResponse(io.BytesIO(transcoded_data), media_type="video/mp4")
        except Exception as e:
            print(f"[Offload] Failed to offload to {best_peer}: {e}")

    # Local processing fallback
    job_queue.append(content_hash)
    transcoded_data = await transcode(content)
    cache[content_hash] = transcoded_data
    job_queue.remove(content_hash)
    print(f"[Local] Processed video locally: {content_hash}")
    observability.local_processing()

    return StreamingResponse(io.BytesIO(transcoded_data), media_type="video/mp4")

# def simulate_job(data):
#     time.sleep(2)
#     return f"Processed {len(data)} bytes"

async def transcode(data: bytes) -> bytes:
    with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as input_tmp:
        input_tmp.write(data)
        input_path = input_tmp.name

    output_path = input_path.replace(".mp4", "_resized.mp4")

    try:
        subprocess.run([
            "ffmpeg",
            "-y",
            "-i", input_path,
            "-vf", "scale=640:360",  # Resize to 640x360
            "-c:v", "libx264",
            "-preset", "fast",
            "-crf", "28",
            output_path
        ], check=True)

        with open(output_path, "rb") as f:
            transcoded_data = f.read()

        print(f"[Transcode] Transcoded video, {len(transcoded_data)} bytes")
        return transcoded_data

    except subprocess.CalledProcessError as e:
        print(f"[Transcode] FFmpeg error: {e}")
        return b""  # return empty bytes on failure

    finally:
        try:
            os.remove(input_path)
            os.remove(output_path)
        except Exception as e:
            print(f"[Cleanup] Error cleaning up files: {e}")

def get_best_peer():
    now = time.time()
    best = None
    best_score = float("inf")
    for peer, stats in peer_status.items():
        if now - stats["last_updated"] > 10:
            continue
        score = stats["cpu"] + stats["queue"]
        if score < best_score:
            best_score = score
            best = peer
    return best  # can be 'self' or 'nodeX:port'

# async def monitor_self():
#     global peer_status
#     try:
#         while True:
#             cpu = psutil.cpu_percent()
#             queue = len(job_queue)
#             peer_status["self"] = {
#                 "cpu": cpu,
#                 "queue": queue,
#                 "last_updated": time.time()
#             }
#             await asyncio.sleep(5)
#     except Exception as e:
#         print(f"[Task Error] monitor_self crashed: {e}")

async def monitor_self():
    global peer_status
    try:
        while True:
            cpu = psutil.cpu_percent()
            queue = len(job_queue)
            peer_status["self"] = {
                "cpu": cpu,
                "queue": queue,
                "last_updated": time.time()
            }
            await asyncio.sleep(5)
    except Exception as e:
        print(f"[Task Error] monitor_self crashed: {e}")

# async def poll_peers():
#     global peer_status
#     try:
#         while True:
#             async with httpx.AsyncClient(timeout=1.5) as client:
#                 for peer in peer_list:
#                     try:
#                         url = f"http://{peer['host']}:{peer['port']}/health"
#                         key = f"{peer['host']}:{peer['port']}"
#                         r = await client.get(url)
#                         data = r.json()
#                         peer_status[key] = {
#                             "cpu": data["cpu"],
#                             "queue": data["queue_length"],
#                             "last_updated": time.time()
#                         }
#                         print(f"[Poll] Reached Successfully: {peer}")
#                     except Exception as e:
#                         print(f"[Poll] Failed to reach {peer}: {e}")
#                         peer_status[f"{peer['host']}:{peer['port']}"] = {
#                             "cpu": 100,
#                             "queue": 100,
#                             "last_updated": 0
#                         }
#             await asyncio.sleep(5)
#     except Exception as e:
#         print(f"[Task Error] poll_peers crashed: {e}")

async def poll_peers():
    global peer_status
    async with httpx.AsyncClient(timeout=1.5) as client:
        for peer in peer_list:
            try:
                url = f"http://{peer['host']}:{peer['port']}/health"
                key = f"{peer['host']}:{peer['port']}"
                r = await client.get(url)
                data = r.json()
                peer_status[key] = {
                    "cpu": data["cpu"],
                    "queue": data["queue_length"],
                    "last_updated": time.time()
                }
                print(f"[Poll] Reached Successfully: {peer}")
            except Exception as e:
                print(f"[Poll] Failed to reach {peer}: {e}")
                peer_status[f"{peer['host']}:{peer['port']}"] = {
                    "cpu": 100,
                    "queue": 100,
                    "last_updated": 0
                }
    await asyncio.sleep(5)

# async def gossip_peers(seed_nodes=None):
#     global peer_list
#     try:
#         while True:
#             if not peer_list and seed_nodes:
#                 print("[Gossip] Peer list empty. Falling back to seed nodes.")
#                 targets = [{"host": seed, "port": 5000} for seed in seed_nodes]
#             else:
#                 targets = random.sample(peer_list, min(2, len(peer_list)))

#             async with httpx.AsyncClient(timeout=3.0) as client:
#                 for peer in targets:
#                     try:
#                         url = f"http://{peer['host']}:{peer['port']}/peers"  # auto construct
#                         r = await client.get(url)
#                         their_peers = r.json().get("peers", [])
#                         for new_peer in their_peers:
#                             if new_peer not in peer_list and new_peer["host"] != os.getenv("NODE_NAME"):
#                                 peer_list.append(new_peer)
#                                 print(f"[Gossip] Discovered new peer: {new_peer}")
#                     except Exception as e:
#                         print(f"[Gossip] Failed gossiping with {peer}: {e}")

#             print(f"[Gossip] Current peer list: {peer_list}")

#             await asyncio.sleep(5)
#     except Exception as e:
#         print(f"[Task Error] gossip_peers crashed: {e}")

async def gossip_peers(seed_nodes=None):
    global peer_list
    if not peer_list and seed_nodes:
        print("[Gossip] Peer list empty. Falling back to seed nodes.")
        targets = [{"host": seed, "port": 5000} for seed in seed_nodes]
    else:
        targets = random.sample(peer_list, min(2, len(peer_list)))

    async with httpx.AsyncClient(timeout=3.0) as client:
        for peer in targets:
            try:
                url = f"http://{peer['host']}:{peer['port']}/peers"  # auto construct
                r = await client.get(url)
                their_peers = r.json().get("peers", [])
                for new_peer in their_peers:
                    if new_peer not in peer_list and new_peer["host"] != os.getenv("NODE_NAME"):
                        peer_list.append(new_peer)
                        print(f"[Gossip] Discovered new peer: {new_peer}")
            except Exception as e:
                print(f"[Gossip] Failed gossiping with {peer}: {e}")

    print(f"[Gossip] Current peer list: {peer_list}")

    await asyncio.sleep(5)

async def fetch_from_seeds(seed_nodes, retries=5):
    global peer_list
    discovered_peers = []

    # await asyncio.sleep(3)  # staggered

    async with httpx.AsyncClient(timeout=3.0) as client:
        for attempt in range(retries):
            for seed in seed_nodes:
                url = f"http://{seed}:5000/peers"   # auto construct URL
                try:
                    r = await client.get(url)
                    peers = r.json().get("peers", [])
                    print(f"[Startup] Fetched {len(peers)} peers from seed {seed}")
                    discovered_peers.extend(peers)
                except Exception as e:
                    print(f"[Startup] Failed to contact seed {seed}, attempt {attempt + 1}: {e}")
            if discovered_peers:
                break
            await asyncio.sleep(2)  # backoff before retrying

    seen = set()
    peer_list = []
    for peer in discovered_peers:
        key = f"{peer['host']}:{peer['port']}"
        if peer["host"] != os.getenv("NODE_NAME") and key not in seen:
            peer_list.append(peer)
            seen.add(key)

    print(f"[Startup] Initial peer list: {peer_list}")

async def wait_until_own_server_ready(host, port, retries=10, delay=2):
    # url = f"http://{host}:{port}/health"
    url = f"http://localhost:{port}/health"
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                r = await client.get(url)
                if r.status_code == 200:
                    print(f"[Startup] Own server reachable at {url}")
                    return
        except Exception:
            pass
        print(f"[Startup] Waiting for own server to be ready ({attempt+1}/{retries})...")
        await asyncio.sleep(delay)
    raise Exception(f"[Startup] Own server not reachable at {url} after {retries} retries!")

async def periodic_summary():
    while True:
        observability.summary()
        await asyncio.sleep(30)

async def safe_background_task(task_name, coro_func):
    while True:
        try:
            await coro_func()
        except Exception as e:
            print(f"[Background Task {task_name}] crashed with exception: {e}")
            await asyncio.sleep(2)  # prevent tight infinite crash loops

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        current_node = os.getenv("NODE_NAME")
        seed_nodes = os.getenv("SEED_NODES", "").split(",")
        print(f"[Lifespan] Node: {current_node}, Seeds: {seed_nodes}")

        await asyncio.sleep(10)

        # host = current_node   # because inside Docker network, they talk via service name
        # port = 5000
        # await wait_until_own_server_ready(host, port)

        await asyncio.sleep(random.uniform(1, 10))  # Avoid thundering herd
        
        await fetch_from_seeds(seed_nodes)
        print("[Lifespan] Seed discovery complete")

        # asyncio.create_task(gossip_peers(seed_nodes))
        # asyncio.create_task(monitor_self())
        # asyncio.create_task(poll_peers())
        # asyncio.create_task(periodic_summary())

        asyncio.create_task(safe_background_task("gossip_peers", lambda: gossip_peers(seed_nodes)))
        asyncio.create_task(safe_background_task("monitor_self", monitor_self))
        asyncio.create_task(safe_background_task("poll_peers", poll_peers))
        asyncio.create_task(safe_background_task("periodic_summary", periodic_summary))
        
        print("[Lifespan] Background tasks launched")

        yield  # Hand control back to FastAPI
    except Exception as e:
        print(f"[Lifespan] Startup error: {e}")
        yield  # Still yield to let FastAPI continue running

app.router.lifespan_context = lifespan

# @app.on_event("startup")
# async def startup():
#     try:
#         global peer_list
#         current_node = os.getenv("NODE_NAME")
#         seed_nodes = os.getenv("SEED_NODES", "").split(",")

#         # Now, seed_nodes = ['node2', 'node3', 'node4'] for example
#         # We will later use it like: f"http://{seed}:5000"

#         if not seed_nodes:
#             print("[Startup] No seed nodes provided.")
#             return
        
#         # Add a random sleep between 1 and 10 seconds BEFORE fetching seeds
#         delay = random.randint(1, 10)
#         print(f"[Startup] Sleeping for {delay} seconds before contacting seeds...")
#         await asyncio.sleep(delay)

#         await fetch_from_seeds(seed_nodes)

#         loop = asyncio.get_event_loop()
#         loop.create_task(gossip_peers(seed_nodes))
#         loop.create_task(monitor_self())
#         loop.create_task(poll_peers())
#     except Exception as e:
#         print(f"[Startup] CRITICAL ERROR: {e}")
    