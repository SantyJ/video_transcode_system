# Decentralized Video Transcoding System
This project builds a decentralized, load-aware video transcoding system using FastAPI, Docker Compose, and lightweight observability. Nodes dynamically offload tasks based on CPU and queue metrics, cache transcoded videos, use gossip protocols to discover peers.
The goal of the project is to build a self-organizing, fault-tolerant system where nodes intelligently balance transcoding workloads without relying on any centralized scheduler service.
