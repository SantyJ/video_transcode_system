# central_server.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import time

app = FastAPI()

# In-memory metrics storage
metrics_store = {}          # { node_id: {metrics...} }
system_metrics_store = {}   # { node_id: {cpu, mem, timestamp...} }

@app.post("/update_metrics")
async def update_metrics(request: Request):
    data = await request.json()
    node_id = data.get("node_id", "unknown_node")
    metrics_store[node_id] = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        **data
    }
    return {"status": "received"}

@app.post("/update_node_metrics")
async def update_node_metrics(request: Request):
    data = await request.json()
    node_id = data.get("node_id", "unknown_node")
    system_metrics_store[node_id] = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        **data
    }
    return {"status": "system_metrics_received"}

@app.get("/metrics")
async def get_metrics():
    return JSONResponse(content=metrics_store)

@app.get("/system_metrics")
async def get_system_metrics():
    return JSONResponse(content=system_metrics_store)

# Mount the static directory to serve dashboard.html
app.mount("/", StaticFiles(directory="static", html=True), name="static")