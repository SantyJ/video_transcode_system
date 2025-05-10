import docker
import time
import requests
import re

client = docker.DockerClient(base_url='unix://var/run/docker.sock')

CENTRAL_SERVER_URL = "http://central_server:8005/update_node_metrics"
PUSH_INTERVAL = 5  # seconds
known_nodes = set()  # Persist known nodes across ticks

def calculate_cpu_percent(stats):
    cpu_delta = stats['cpu_stats']['cpu_usage']['total_usage'] - stats['precpu_stats']['cpu_usage']['total_usage']
    system_delta = stats['cpu_stats']['system_cpu_usage'] - stats['precpu_stats']['system_cpu_usage']
    if system_delta > 0.0 and cpu_delta > 0.0:
        return round((cpu_delta / system_delta) * len(stats['cpu_stats']['cpu_usage']['percpu_usage']) * 100.0, 2)
    return 0.0

# def collect_and_push():
#     containers = client.containers.list()
#     for container in containers:
#         if re.match(r".*node.*", container.name):
#             try:
#                 stats = container.stats(stream=False)

#                 cpu_percent = calculate_cpu_percent(stats)
#                 mem_usage = stats['memory_stats']['usage']
#                 mem_limit = stats['memory_stats']['limit']
#                 mem_percent = round((mem_usage / mem_limit) * 100, 2) if mem_limit > 0 else 0.0

#                 # match = re.search(r"(node\\d+)", container.name)
#                 match = re.search(r"node\d+", container.name)
#                 node_id = match.group(0) if match else container.name

#                 payload = {
#                     "node_id": node_id,
#                     "cpu_percent": cpu_percent,
#                     "memory_percent": mem_percent,
#                     "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
#                 }

#                 print (f"Post payload: {payload}")
#                 response = requests.post(CENTRAL_SERVER_URL, json=payload, timeout=5)
#                 print(f"[Sent] {payload} | Status: {response.status_code}")

#             except Exception as e:
#                 print(f"[Error] Failed to collect/send stats for {container.name}: {e}")

def collect_and_push():
    global known_nodes

    containers = client.containers.list()
    active_nodes = {}

    # First, collect active stats
    for container in containers:
        if re.match(r".*node.*", container.name):
            try:
                stats = container.stats(stream=False)

                cpu_percent = calculate_cpu_percent(stats)
                mem_usage = stats['memory_stats']['usage']
                mem_limit = stats['memory_stats']['limit']
                mem_percent = round((mem_usage / mem_limit) * 100, 2) if mem_limit > 0 else 0.0

                # match = re.search(r"node\\d+", container.name)
                match = re.search(r"node\d+", container.name)
                node_id = match.group(0) if match else container.name
                known_nodes.add(node_id)
                # print(f"Node id: {node_id} Mem usage: {mem_usage} Mem limit: {mem_limit} Mem percent: {mem_percent}")
                active_nodes[node_id] = {
                    "cpu_percent": cpu_percent,
                    "memory_percent": mem_percent
                }

            except Exception as e:
                print(f"[Error] Failed to collect stats from {container.name}: {e}")

    # Now send for all known nodes
    for node_id in known_nodes:
        stats = active_nodes.get(node_id, {"cpu_percent": 0.0, "memory_percent": 0.0})
        payload = {
            "node_id": node_id,
            **stats,
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }

        try:
            response = requests.post(CENTRAL_SERVER_URL, json=payload, timeout=5)
            print(f"[Monitor] Sent for {node_id}: {payload} | Status: {response.status_code}")
        except Exception as e:
            print(f"[Monitor] Failed to push for {node_id}: {e}")

if __name__ == "__main__":
    while True:
        collect_and_push()
        time.sleep(PUSH_INTERVAL)