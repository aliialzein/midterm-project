"""Throwaway stand-in for the real metadata service (built in a later task).

Run:  uvicorn tools.mock_metadata:app --port 8000
Then: http://localhost:8000/nodes shows the last heartbeat of every node.
"""
import time

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Mock metadata service")
nodes: dict[str, dict] = {}


class Heartbeat(BaseModel):
    node_id: str
    url: str = ""
    timestamp: float
    chunk_count: int
    used_bytes: int
    free_bytes: int
    total_bytes: int


@app.post("/heartbeat")
def heartbeat(hb: Heartbeat):
    nodes[hb.node_id] = {**hb.model_dump(), "received_at": time.time()}
    print(f"[heartbeat] {hb.node_id}: chunks={hb.chunk_count} free={hb.free_bytes // 2**20} MiB")
    return {"ok": True}


@app.get("/nodes")
def list_nodes():
    now = time.time()
    return [{**n, "seconds_since_heartbeat": round(now - n["received_at"], 1)} for n in nodes.values()]
