from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import StreamingResponse
from cachetools import LRUCache
from contextlib import asynccontextmanager
import psutil, hashlib, time, asyncio, httpx, random, os, io
from typing import List, Dict
import subprocess
import tempfile
import requests

# All custom modules
# Logging module
from observability import Observability
observability = Observability(log_file_path="metrics.log")
# Zones config
from zone_config import ZONE_RTT

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
        "memory": psutil.virtual_memory().percent,   # NEW
        "queue_length": len(job_queue)
    }

# @app.get("/peers")
# def get_peers():
#     # Always include self so other nodes can learn about us
#     self_info = {"host": os.getenv("NODE_NAME"), "port": 5000}
#     return {"peers": [self_info] + peer_list}

@app.get("/peers")
def get_peers():
    self_info = {
        "host": os.getenv("NODE_NAME"),
        "port": 5000,
        "zone": os.getenv("NODE_ZONE")  # NEW: add own zone
    }
    return {"peers": [self_info] + peer_list}

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

# Push Metrics to o11y server
def push_metrics_to_central(metrics):
    try:
        requests.post("http://central_server:8005/update_metrics", json=metrics, timeout=2)
    except Exception as e:
        print(f"[Metrics Push] Failed to send metrics to central server: {e}")

@app.post("/upload")
async def upload(file: UploadFile = File(...), request: Request = None):
    content = await file.read()

    if not file.content_type.startswith("video/"):
        return {"error": "Only video files are supported."}

    content_hash = hashlib.sha256(content).hexdigest()
    origin_zone = request.headers.get("x-origin-zone") or os.getenv("NODE_ZONE")

    # Check cache first
    if content_hash in cache:
        print(f"[Cache] Cache hit for {content_hash}")
        if origin_zone == os.getenv("NODE_ZONE"):
            observability.local_response(os.getenv("NODE_NAME"))
        else:
            observability.remote_response(os.getenv("NODE_NAME"))
        observability.cache_hit()
        observability.local_processing()
        transcoded_data = cache[content_hash]
        return StreamingResponse(io.BytesIO(transcoded_data), media_type="video/mp4")

    # Try offloading
    best_peer, best_peer_zone = get_best_peer()
    curr_zone = os.getenv("NODE_ZONE")
    if best_peer and best_peer != "self":
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                files = {"file": (file.filename, content, file.content_type)}
                headers = {"x-origin-zone": origin_zone}
                r = await client.post(f"http://{best_peer}/upload", files=files, headers=headers)
                if curr_zone == best_peer_zone:
                    print(f"[Same-Region Offload] Successfully offloaded to {best_peer}")
                    observability.offload_success_local(best_peer)
                else:
                    print(f"[Cross-Region Offload] Successfully offloaded to {best_peer} | {curr_zone} -> {best_peer_zone}")
                    observability.offload_success_remote(best_peer)
                observability.offload_success(best_peer)

                transcoded_data = r.content
                cache[content_hash] = transcoded_data  # Cache offloaded result too

                return StreamingResponse(io.BytesIO(transcoded_data), media_type="video/mp4")
        except Exception as e:
            print(f"[Offload] Failed to offload to {best_peer}: {e}")
            
    # Local processing fallback
    try:
        job_queue.append(content_hash)
        transcoded_data = await transcode(content)
        cache[content_hash] = transcoded_data
        job_queue.remove(content_hash)
        print(f"[Local] Processed video locally: {content_hash}")
        if origin_zone == os.getenv("NODE_ZONE"):
            observability.local_response(os.getenv("NODE_NAME"))
        else:
            observability.remote_response(os.getenv("NODE_NAME"))
        observability.local_processing()
        observability.cache_miss()

        return StreamingResponse(io.BytesIO(transcoded_data), media_type="video/mp4")

    except Exception as e:
        print(f"[Local] Failed to process locally: {e}")
        observability.failed_request()
        return {"error": "Failed to process video."}
    
    # # Local processing fallback
    # job_queue.append(content_hash)
    # transcoded_data = await transcode(content)
    # cache[content_hash] = transcoded_data
    # job_queue.remove(content_hash)
    # print(f"[Local] Processed video locally: {content_hash}")
    # if origin_zone == os.getenv("NODE_ZONE"):
    #     observability.local_response(os.getenv("NODE_NAME"))
    # else:
    #     observability.remote_response(os.getenv("NODE_NAME"))
    # observability.local_processing()
    # observability.cache_miss()

    # return StreamingResponse(io.BytesIO(transcoded_data), media_type="video/mp4")

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

# def get_best_peer():
#     now = time.time()
#     best = None
#     best_score = float("inf")
#     for peer, stats in peer_status.items():
#         if now - stats["last_updated"] > 10:
#             continue
#         score = stats["cpu"] + stats["queue"]
#         if score < best_score:
#             best_score = score
#             best = peer
#     return best  # can be 'self' or 'nodeX:port'

def get_best_peer():
    now = time.time()
    best = None
    best_score = float("inf")

    for peer, stats in peer_status.items():
        if now - stats["last_updated"] > 10:
            continue
        
        if peer == "self":
            peer_host = os.getenv("NODE_NAME")
            peer_port = 5000
            peer_zone = os.getenv("NODE_ZONE")
        else:
            peer_host, peer_port = peer.split(":")
            # Find zone dynamically (peer_status and peer_list should have same list of peers since poll_peers() gets status of all peers in peer_list)
            peer_zone = None
            for p in peer_list:
                if p["host"] == peer_host and str(p["port"]) == peer_port:
                    peer_zone = p.get("zone")
                    break

        rtt = ZONE_RTT.get((os.getenv("NODE_ZONE"), peer_zone), 100) # If unable to fetch RTT info default to 100

        # For further analysis (tunable weights for each metric)
        # CPU_WEIGHT = 0.3
        # MEM_WEIGHT = 0.3
        # QUEUE_WEIGHT = 0.4

        # load_score = (
        #     CPU_WEIGHT * stats["cpu"] +
        #     MEM_WEIGHT * stats.get("memory", 100) +
        #     QUEUE_WEIGHT * stats["queue"]
        # )

        load_score = stats["cpu"] + stats["queue"] + stats["memory"]
        score = (0.5 * rtt) + (0.5 * load_score)

        if score < best_score:
            best_score = score
            best = peer
            best_zone = peer_zone

    return best, best_zone

async def monitor_self():
    global peer_status
    cpu = psutil.cpu_percent()
    queue = len(job_queue)
    memory = psutil.virtual_memory().percent
    peer_status["self"] = {
        "cpu": cpu,
        "queue": queue,
        "memory": memory,
        "last_updated": time.time()
    }
    await asyncio.sleep(5)

async def poll_peers():
    global peer_status
    async with httpx.AsyncClient(timeout=1.5) as client:
        tasks = []
        peer_keys = []

        for peer in peer_list:
            url = f"http://{peer['host']}:{peer['port']}/health"
            tasks.append(client.get(url))
            peer_keys.append(f"{peer['host']}:{peer['port']}")

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        for peer_key, response in zip(peer_keys, responses):
            if isinstance(response, Exception):
                print(f"[Poll] Failed to reach {peer_key}: {response}")
                peer_status[peer_key] = {
                    "cpu": 100,
                    "memory": 100,
                    "queue": 100,
                    "last_updated": 0
                }
            else:
                try:
                    data = response.json()
                    peer_status[peer_key] = {
                        "cpu": data["cpu"],
                        "memory": data["memory"],
                        "queue": data["queue_length"],
                        "last_updated": time.time()
                    }
                    print(f"[Poll] Reached Successfully: {peer_key}")
                except Exception as e:
                    print(f"[Poll] Error parsing response from {peer_key}: {e}")
                    peer_status[peer_key] = {
                        "cpu": 100,
                        "memory": 100,
                        "queue": 100,
                        "last_updated": 0
                    }

    await asyncio.sleep(5)

async def gossip_peers(seed_nodes=None):
    global peer_list

    known_peers = peer_list.copy()
    if seed_nodes:
        known_peers.extend({"host": seed, "port": 5000} for seed in seed_nodes)

    # Remove duplicates
    seen = set()
    unique_peers = []
    for peer in known_peers:
        key = (peer['host'], peer['port'])
        if key not in seen:
            seen.add(key)
            unique_peers.append(peer)

    targets = random.sample(unique_peers, min(2, len(unique_peers)))

    async with httpx.AsyncClient(timeout=3.0) as client:
        tasks = []
        target_keys = []

        for peer in targets:
            url = f"http://{peer['host']}:{peer['port']}/peers"
            tasks.append(client.get(url))
            target_keys.append(f"{peer['host']}:{peer['port']}")

        responses = await asyncio.gather(*tasks, return_exceptions=True)

        for peer_key, response in zip(target_keys, responses):
            if isinstance(response, Exception):
                print(f"[Gossip] Failed gossiping with {peer_key}: {response}")
            else:
                try:
                    their_peers = response.json().get("peers", [])
                    # for new_peer in their_peers:
                    #     if new_peer not in peer_list and new_peer["host"] != os.getenv("NODE_NAME"):
                    #         peer_list.append(new_peer)
                    #         print(f"[Gossip] Discovered new peer: {new_peer}")
                    for new_peer in their_peers:
                        exists = any(p["host"] == new_peer["host"] and p["port"] == new_peer["port"] for p in peer_list)
                        if not exists and new_peer["host"] != os.getenv("NODE_NAME"):
                            peer_list.append(new_peer)
                            print(f"[Gossip] Discovered new peer: {new_peer}")
                except Exception as e:
                    print(f"[Gossip] Error parsing response from {peer_key}: {e}")

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

async def periodic_metrics_push():
    while True:
        try:
            process = psutil.Process()
            mem_info = process.memory_info()
            metrics = {
                "node_id": os.getenv("NODE_NAME", "unknown_node"),
                "node_region": os.getenv("NODE_ZONE", "unknown_region"),
                "cpu_load": psutil.cpu_percent(),
                "memory_usage": round((mem_info.rss / psutil.virtual_memory().total) * 100, 2),
                "queue_length": len(job_queue),
                "cache_hits": observability.cache_hits,
                "cache_misses": observability.cache_misses,
                "offloads_total": observability.offloads,
                "offloads_same_region": observability.offloads_local,
                "offloads_cross_region": observability.offloads_remote,
                "same_region_responses": observability.local_responses,
                "cross_region_responses": observability.remote_responses,
                "local_processings": observability.local_processings,
                "failures": observability.failures,
                "uptime_seconds": int(time.time() - observability.start_time)
            }
            push_metrics_to_central(metrics)
            print(f"[Metrics Push] Sent metrics: {metrics}")
        except Exception as e:
            print(f"[Metrics Task Error]: {e}")

        await asyncio.sleep(5)  # Push every 5 seconds

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
        asyncio.create_task(safe_background_task("periodic_metrics_push", periodic_metrics_push))
        
        print("[Lifespan] Background tasks launched")

        yield  # Hand control back to FastAPI
    except Exception as e:
        print(f"[Lifespan] Startup error: {e}")
        yield  # Still yield to let FastAPI continue running

app.router.lifespan_context = lifespan
