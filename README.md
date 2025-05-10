# Decentralized Video Transcoding System with Gossip-Based Peer Discovery
This project builds a decentralized, load-aware video transcoding system using FastAPI, Docker Compose, and lightweight observability. Nodes dynamically offload tasks based on CPU and queue metrics, cache transcoded videos, use gossip protocols to discover peers.
The goal of the project is to build a self-organizing, fault-tolerant system where nodes intelligently balance transcoding workloads without relying on any centralized scheduler service.

## Features

- Proximity- and load-aware offloading between nodes  
- Lightweight peer discovery with gossip protocol  
- Fully decentralized architecture (no central controller)  
- Real-time system metrics and observability dashboard  
- Simulated client load testing

## Prerequisites

- **Docker Desktop** → [Download Docker Desktop](https://www.docker.com/products/docker-desktop/)  
- WSL 2 (Windows Subsystem for Linux) backend enabled for Docker on Windows (if using WSL)  

> **Note:** This software was developed and tested in a Linux environment using **WSL 2 on Windows**. We strongly recommend running in **Linux or WSL** for consistent behavior and performance.

## Instructions to Run the Software:

- Clone this repo and cd to the repo directory
- Run the System: docker-compose up --build
    - This will start:
    - 5 transcoding nodes (node1 → node5)
    - A client container that sends video uploads every 5 seconds
    - A resource monitor container which ships cpu and memory metrics from node containers to the observability container
    - A central observability server (container) showing live metrics shipped from containers and the resource monitor

- The System Activity can be at: http://localhost:8005/ (This displays all live metrics of the system)
- The Container logs can be monitored in Docker desktop as shown below:

![alt text](image.png)

![alt text](image-1.png)

## Instructions for Empirical Evaluation of the Software:

# Comparing Random Offload Routing vs Our Score-based Routing Algorithm

- Under container_scripts directory, in main.py:
    - in line 322 un-comment the get_best_peer() function under the "Random Offloading" Comment.
    - comment the get_best_peer() function in line 358

- Run the docker compose application.
- In the dashboard (http://localhost:8005/) you can check the Client Metrics section for the throughput, latency, offloads and Response related metrics.

- Now, revert to the original main.py script and run the docker compose application.
- Check the dashboard again for the same metrics and compare.

- Check our project report to see comparison between the 2 offloading, reasoning and results based on these metrics.

# Comparing Round-Robin Offload Routing vs Our Score-based Routing Algorithm

- Under container_scripts directory, in main.py:
    - in line 338 un-comment the get_best_peer() function under the "Round-Robin offloading" Comment.
    - comment the get_best_peer() function in line 358

- Rest of the steps remain the same as the previous test.

# Fault Tolerance test (1 node down scenario)

- Run the docker compose app
- Check the dashboard for CPU metrics
- Stop one of the nodes which have significant CPU load by doing -> docker stop <container_id for that node>
- Check container id of containers in docker desktop:

![alt text](image.png)

- Check the latency and throughput metrics (graphs and tables) in dashboard

# Fault Tolerance test (2 node down scenario)

- Similar to previous step but need to bring down 2 node containers.

# Evaluating Offload Behavior Under Regional Saturation: RTT vs Load Bias

- In client/smart_upload.py comment line 78 and uncomment line 81
- In container_scripts/main.py 