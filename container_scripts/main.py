from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import StreamingResponse
from cachetools import LRUCache
from contextlib import asynccontextmanager
import psutil, hashlib, time, asyncio, httpx, random, os, io
from typing import List, Dict
import subprocess
import tempfile
import requests
import itertools
import aiofiles

# All custom modules
# Logging module
from observability import Observability
observability = Observability(log_file_path="metrics.log")
# Zones config
from zone_config import ZONE_RTT

app = FastAPI()

# job_queue = []

# Shared queue and number of workers
MAX_QUEUE_SIZE = 20
job_queue = asyncio.Queue(MAX_QUEUE_SIZE)
NUM_WORKERS = 5

# cache: Dict[str, Dict] = {}
# Create an LRU Cache for transcoded videos (example: 100 entries)
cache = LRUCache(maxsize=100)
peer_status = {}
peer_list = []

_rr_cycle = None  # Global or module-level

# For queue metrics
enqueue_count = 0
dequeue_count = 0
last_enqueue = 0
last_dequeue = 0
last_metrics_push_time = time.time()
enqueue_timestamps = []
dequeue_timestamps = []

# Gossip test metrics
gossip_cycles = 0
gossip_start_time = None
discovery_complete = False

# Worker with dequeue tracking
async def worker(worker_id):
    global dequeue_count
    print(f"worker: {worker_id}")
    while True:
        (job, future) = await job_queue.get()
        try:
            content_hash, content = job
            transcoded_data = await transcode(content)
            future.set_result(transcoded_data)
        except Exception as e:
            future.set_exception(e)
        finally:
            dequeue_count += 1
            dequeue_timestamps.append(time.time())
            job_queue.task_done()

# Start workers at startup
def start_worker_pool():
    print("In Start worker pool func")
    for i in range(NUM_WORKERS):
        asyncio.create_task(worker(i))

# @app.on_event("startup")
# async def on_startup():
#     print("Calling Start worker pool func")
#     start_worker_pool()

@app.get("/")
def root():
    return {"message": "FastAPI node is running"}

def get_container_cpu_usage(total_cpus=20):
    path = "/sys/fs/cgroup/cpuacct/cpuacct.usage"
    
    with open(path, "r") as f:
        usage1 = int(f.read().strip())
    
    time.sleep(1)

    with open(path, "r") as f:
        usage2 = int(f.read().strip())

    # Calculate delta usage in seconds
    delta_seconds = (usage2 - usage1) / 1e9

    # Raw usage as a percentage of a single CPU
    raw_percent = delta_seconds * 100

    # Scale to total CPUs (e.g., 20 CPUs = 100%)
    scaled_percent = (raw_percent / (total_cpus * 100)) * 100

    print(f"Container CPU usage: {scaled_percent}% (of 20 CPUs)")
    
    return scaled_percent

@app.get("/health")
def health():
    return {
        "cpu": psutil.cpu_percent(),
        "memory": psutil.virtual_memory().percent,   # NEW
        "queue_length": job_queue.qsize()
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

# @app.get("/peers")
# async def get_peers(request: Request):
#     caller = request.query_params.get("caller")
#     if caller:
#         peer_host = caller
#         peer_port = 5000
#         exists = any(p["host"] == peer_host and p["port"] == peer_port for p in peer_list)
#         if not exists and peer_host != os.getenv("NODE_NAME"):
#             peer_list.append({"host": peer_host, "port": peer_port})
#             print(f"[Gossip] Added caller {peer_host} to peer list")

#     return {"peers": peer_list}

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
async def upload(request: Request):
    global enqueue_count
    filename = request.headers.get("X-Filename", f"upload_{int(time.time())}.mp4")
    content_type = request.headers.get("Content-Type", "video/mp4")
    if not content_type.startswith("video/"):
        return {"error": "Only video files are supported."}

    tmp_path = None
    try:
        # Save stream to temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            async for chunk in request.stream():
                tmp.write(chunk)
            tmp_path = tmp.name

        async with aiofiles.open(tmp_path, "rb") as f:
            content = await f.read()

        content_hash = hashlib.sha256(content).hexdigest()
        origin_zone = request.headers.get("x-origin-zone") or os.getenv("NODE_ZONE")
        request_start = float(request.headers.get("x-start-time", time.time()))
        simulated_rtt_accumulated = float(request.headers.get("x-simulated-rtt", "0"))
        curr_zone = os.getenv("NODE_ZONE")

        # Cache check
        if content_hash in cache:
            observability.cache_hit()
            observability.local_processing()
            if origin_zone == curr_zone:
                observability.local_response(os.getenv("NODE_NAME"))
            else:
                observability.remote_response(os.getenv("NODE_NAME"))
            latency_ms = (time.time() - request_start) * 1000 + simulated_rtt_accumulated
            response = StreamingResponse(io.BytesIO(cache[content_hash]), media_type="video/mp4")
            response.headers['X-Processing-Latency'] = str(round(latency_ms, 2))
            return response

        # Try offloading
        best_peer, best_peer_zone = get_best_peer()
        print(f"Peer info: {best_peer} , {best_peer_zone}")
        if best_peer and best_peer != "self":
            try:
                simulated_rtt = ZONE_RTT.get((curr_zone, best_peer_zone), 0)
                await asyncio.sleep(simulated_rtt / 1000.0)
                simulated_rtt_accumulated += simulated_rtt

                async with aiofiles.open(tmp_path, "rb") as f:
                    file_bytes = await f.read()

                async with httpx.AsyncClient(timeout=40.0) as client:
                    headers = {
                        "x-origin-zone": origin_zone,
                        "x-start-time": str(request_start),
                        "x-simulated-rtt": str(simulated_rtt_accumulated),
                        "X-Filename": filename,
                        "Content-Type": "video/mp4"
                    }

                    r = await client.post(
                        f"http://{best_peer}/upload",
                        content=file_bytes,
                        headers=headers
                    )

                transcoded_data = r.content
                cache[content_hash] = transcoded_data

                if curr_zone == best_peer_zone:
                    print(f"[Same-Region Offload] Successfully offloaded to {best_peer}")
                    observability.offload_success_local(best_peer)
                else:
                    print(f"[Cross-Region Offload] Successfully offloaded to {best_peer} | {curr_zone} -> {best_peer_zone}")
                    observability.offload_success_remote(best_peer)
                observability.offload_success(best_peer)

                peer_latency = float(r.headers.get("X-Processing-Latency", "0"))
                total_latency = peer_latency + simulated_rtt
                response = StreamingResponse(io.BytesIO(transcoded_data), media_type="video/mp4")
                response.headers['X-Processing-Latency'] = str(round(total_latency, 2))
                return response

            except Exception as e:
                print(f"[Offload] Failed to offload to {best_peer}: {e}")

        # Local processing via worker pool
        try:
            loop = asyncio.get_event_loop()
            response_future = loop.create_future()
            await job_queue.put(((content_hash, content), response_future))
            enqueue_count += 1
            enqueue_timestamps.append(time.time())

            transcoded_data = await response_future
            cache[content_hash] = transcoded_data
            observability.local_processing()
            observability.cache_miss()
            if origin_zone == curr_zone:
                observability.local_response(os.getenv("NODE_NAME"))
            else:
                observability.remote_response(os.getenv("NODE_NAME"))

            latency_ms = (time.time() - request_start) * 1000 + simulated_rtt_accumulated
            response = StreamingResponse(io.BytesIO(transcoded_data), media_type="video/mp4")
            response.headers['X-Processing-Latency'] = str(round(latency_ms, 2))
            return response

        except asyncio.QueueFull:
            observability.failed_request()
            return {"error": "Server is too busy. Please try again later."}
        except Exception as e:
            observability.failed_request()
            print(f"[Local] Failed to process locally: {e}")
            return {"error": "Failed to process video."}

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
                print(f"[Cleanup] Deleted tmp file: {tmp_path}")
            except Exception as e:
                print(f"[Cleanup] Error deleting tmp file: {e}")

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

# Random offloading
# def get_best_peer():
#     now = time.time()
#     candidates = [
#         peer for peer in peer_status
#         if peer != "self" and time.time() - peer_status[peer]["last_updated"] <= 10
#     ]

#     if not candidates:
#         return None, None

#     selected = random.choice(candidates)
#     peer_host, peer_port = selected.split(":")
#     peer_zone = next((p["zone"] for p in peer_list if p["host"] == peer_host and str(p["port"]) == peer_port), "unknown")
#     return selected, peer_zone

# Round-Robin offloading
# def get_best_peer():
#     global _rr_cycle
#     now = time.time()
#     candidates = [
#         peer for peer in peer_status
#         if peer != "self" and time.time() - peer_status[peer]["last_updated"] <= 10
#     ]

#     if not candidates:
#         return None, None

#     if _rr_cycle is None or not hasattr(_rr_cycle, '__next__'):
#         _rr_cycle = itertools.cycle(candidates)

#     selected = next(_rr_cycle)
#     peer_host, peer_port = selected.split(":")
#     peer_zone = next((p["zone"] for p in peer_list if p["host"] == peer_host and str(p["port"]) == peer_port), "unknown")
#     return selected, peer_zone

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
        # score = (0.5 * rtt) + (0.5 * load_score)

        min_rtt = 10
        max_rtt = 250

        normalized_rtt = (rtt - min_rtt) / (max_rtt - min_rtt)  # Scale to 0-1
        max_load = 200 + MAX_QUEUE_SIZE
        normalized_load = load_score / max_load  # If max load is 300
        
        score = 0.3 * normalized_load + 0.7 * normalized_rtt # Lower RTT biased (Same region offloads)
        # score = 0.8 * normalized_load + 0.2 * normalized_rtt # Lower Load biased (Cross region offloads happen more often)

        if score < best_score:
            best_score = score
            best = peer
            best_zone = peer_zone

    return best, best_zone

async def monitor_self():
    global peer_status
    cpu = psutil.cpu_percent()
    queue = job_queue.qsize()
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
    global peer_list, gossip_cycles, gossip_start_time, discovery_complete

    if not discovery_complete:
        if gossip_start_time is None:
            gossip_start_time = time.time()
        gossip_cycles += 1

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
                    for new_peer in their_peers:
                        exists = any(p["host"] == new_peer["host"] and p["port"] == new_peer["port"] for p in peer_list)
                        if not exists and new_peer["host"] != os.getenv("NODE_NAME"):
                            peer_list.append(new_peer)
                            print(f"[Gossip] Discovered new peer: {new_peer}")
                except Exception as e:
                    print(f"[Gossip] Error parsing response from {peer_key}: {e}")

    print(f"[Gossip] Current peer list: {peer_list}")

    # Check for full discovery
    if not discovery_complete and len(peer_list) >= 4:
        elapsed = time.time() - gossip_start_time
        print(f"[Gossip] All peers discovered in {elapsed:.2f} seconds over {gossip_cycles} gossip cycles.")
        discovery_complete = True

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

# Metrics pusher (run in background)
# async def push_metrics_periodically():
#     global last_enqueue, last_dequeue, last_metrics_push_time

#     while True:
#         await asyncio.sleep(5)
#         now = time.time()
#         interval = now - last_metrics_push_time
#         last_metrics_push_time = now

#         new_enqueue = enqueue_count - last_enqueue
#         new_dequeue = dequeue_count - last_dequeue

#         enqueue_rate = new_enqueue / interval
#         dequeue_rate = new_dequeue / interval
#         queue_growth = new_enqueue - new_dequeue

#         last_enqueue = enqueue_count
#         last_dequeue = dequeue_count

#         payload = {
#             "node_id": os.getenv("NODE_NAME"),
#             "queue_len": job_queue.qsize(),
#             "enqueue_rate": round(enqueue_rate, 2),
#             "dequeue_rate": round(dequeue_rate, 2),
#             "queue_growth": queue_growth,
#             "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
#         }

#         try:
#             requests.post("http://central_server:8005/update_metrics", json=payload, timeout=5)
#             print(f"[Metrics] Sent: {payload}")
#         except Exception as e:
#             print(f"[Metrics] Failed to push: {e}")

async def periodic_metrics_push():
    global last_metrics_push_time, last_enqueue, last_dequeue, enqueue_timestamps, dequeue_timestamps
    global enqueue_count, dequeue_count
    while True:
        try:
            process = psutil.Process()
            mem_info = process.memory_info()

            now = time.time()
            # interval = now - last_metrics_push_time
            # last_metrics_push_time = now

            # new_enqueue = enqueue_count - last_enqueue
            # new_dequeue = dequeue_count - last_dequeue

            # enqueue_rate = new_enqueue / interval
            # dequeue_rate = new_dequeue / interval
            # queue_growth = new_enqueue - new_dequeue

            # last_enqueue = enqueue_count
            # last_dequeue = dequeue_count
            
            window = 60  # seconds
            cutoff = time.time() - window
            enqueue_timestamps[:] = [t for t in enqueue_timestamps if t >= cutoff]
            dequeue_timestamps[:] = [t for t in dequeue_timestamps if t >= cutoff]

            enqueue_rate = round(len(enqueue_timestamps) / window, 2)
            dequeue_rate = round(len(dequeue_timestamps) / window, 2)

            # Calculate per-minute totals
            enqueue_count_per_min = len(enqueue_timestamps)
            dequeue_count_per_min = len(dequeue_timestamps)

            # Calculate queue growth (net jobs added in the last minute)
            queue_growth = enqueue_count_per_min - dequeue_count_per_min

            # queue_growth = len(enqueue_timestamps) - len(dequeue_timestamps)

            metrics = {
                "node_id": os.getenv("NODE_NAME", "unknown_node"),
                "node_region": os.getenv("NODE_ZONE", "unknown_region"),
                "cpu_load": psutil.cpu_percent(),
                "memory_usage": round((mem_info.rss / psutil.virtual_memory().total) * 100, 2),
                "queue_length": job_queue.qsize(),
                "enqueue_rate": round(enqueue_rate, 2),
                "dequeue_rate": round(dequeue_rate, 2),
                "enqueue_count_per_min": enqueue_count_per_min,
                "dequeue_count_per_min": dequeue_count_per_min,
                "queue_growth": queue_growth,
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
            print(f"Enqueue count: {enqueue_count} Dequeue count: {dequeue_count}")
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

        await asyncio.sleep(3)

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

        print("Calling Start worker pool func")
        start_worker_pool()
        
        print("[Lifespan] Background tasks launched")

        yield  # Hand control back to FastAPI
    except Exception as e:
        print(f"[Lifespan] Startup error: {e}")
        yield  # Still yield to let FastAPI continue running

app.router.lifespan_context = lifespan
