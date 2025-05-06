# central_server.py
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import time

app = FastAPI()

# In-memory metrics storage
metrics_store = {}  # { node_id: {metrics...} }

@app.post("/update_metrics")
async def update_metrics(request: Request):
    data = await request.json()
    node_id = data.get("node_id", "unknown_node")
    metrics_store[node_id] = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        **data
    }
    return {"status": "received"}

@app.get("/metrics")
async def get_metrics():
    return JSONResponse(content=metrics_store)

# @app.get("/", response_class=HTMLResponse)
# def dashboard():
#     html = "<html><head><meta http-equiv='refresh' content='5'></head><body>"
#     html += "<h1>🚀 Central Metrics Dashboard</h1><hr>"
#     for node_id, metrics in metrics_store.items():
#         html += f"<h2>Node: {node_id}</h2><ul>"
#         for key, value in metrics.items():
#             html += f"<li><b>{key}</b>: {value}</li>"
#         html += "</ul><hr>"
#     html += "</body></html>"
#     return html

# @app.get("/", response_class=HTMLResponse)
# async def serve_dashboard():
#     with open("dashboard.html", "r", encoding="utf-8") as f:
#         html = f.read()
#     return HTMLResponse(content=html)

# Mount the static directory to serve dashboard.html
app.mount("/", StaticFiles(directory="static", html=True), name="static")
