"""Storage node: a small FastAPI service that stores chunks addressed by SHA-256."""
import asyncio
import logging
import os
import socket
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from starlette.concurrency import run_in_threadpool

from .store import (
    ChunkCorrupted,
    ChunkNotFound,
    ChunkStore,
    HashMismatch,
    InvalidHash,
)

log = logging.getLogger("storage_node")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


@dataclass
class Settings:
    node_id: str
    node_url: str
    data_dir: str
    metadata_url: str  # empty string disables heartbeats
    heartbeat_interval: float
    max_chunk_size: int

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            node_id=os.getenv("NODE_ID", socket.gethostname()),
            node_url=os.getenv("NODE_URL", ""),
            data_dir=os.getenv("DATA_DIR", "./data"),
            metadata_url=os.getenv("METADATA_URL", ""),
            heartbeat_interval=float(os.getenv("HEARTBEAT_INTERVAL", "5")),
            max_chunk_size=int(os.getenv("MAX_CHUNK_SIZE", str(16 * 1024 * 1024))),
        )


async def heartbeat_loop(store: ChunkStore, cfg: Settings) -> None:
    """Tell the metadata service we're alive, every few seconds, forever.

    A failed heartbeat is never fatal: the node keeps serving chunks. The metadata
    service is the one that decides a node is dead (no heartbeat for N seconds).
    """
    url = f"{cfg.metadata_url.rstrip('/')}/heartbeat"
    async with httpx.AsyncClient(timeout=3.0) as client:
        while True:
            payload = {
                "node_id": cfg.node_id,
                "url": cfg.node_url,
                "timestamp": time.time(),
                **store.stats(),
            }
            try:
                r = await client.post(url, json=payload)
                r.raise_for_status()
            except Exception as exc:
                log.warning("heartbeat to %s failed: %s", url, exc)
            await asyncio.sleep(cfg.heartbeat_interval)


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or Settings.from_env()
    store = ChunkStore(cfg.data_dir)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        task = None
        if cfg.metadata_url:
            task = asyncio.create_task(heartbeat_loop(store, cfg))
            log.info("node %s sending heartbeats to %s", cfg.node_id, cfg.metadata_url)
        yield
        if task:
            task.cancel()

    app = FastAPI(title=f"Storage Node {cfg.node_id}", lifespan=lifespan)
    app.state.store = store
    app.state.settings = cfg

    @app.put("/chunks/{chunk_id}")
    async def put_chunk(chunk_id: str, request: Request, response: Response):
        declared = request.headers.get("content-length")
        if declared and int(declared) > cfg.max_chunk_size:
            raise HTTPException(413, "chunk too large")
        data = await request.body()
        if len(data) > cfg.max_chunk_size:
            raise HTTPException(413, "chunk too large")
        try:
            created = await run_in_threadpool(store.put, chunk_id, data)
        except InvalidHash:
            raise HTTPException(400, "chunk id must be 64 lowercase hex chars (SHA-256)")
        except HashMismatch:
            raise HTTPException(400, "content does not match SHA-256 in URL")
        response.status_code = 201 if created else 200
        return {"chunk_id": chunk_id, "size": len(data), "stored": created, "deduplicated": not created}

    @app.get("/chunks/{chunk_id}")
    async def get_chunk(chunk_id: str):
        try:
            data = await run_in_threadpool(store.get, chunk_id)
        except InvalidHash:
            raise HTTPException(400, "invalid chunk id")
        except ChunkNotFound:
            raise HTTPException(404, "chunk not found")
        except ChunkCorrupted:
            log.error("chunk %s failed integrity check; quarantined", chunk_id)
            raise HTTPException(422, "chunk corrupted on disk and has been quarantined")
        return Response(content=data, media_type="application/octet-stream",
                        headers={"X-Chunk-Sha256": chunk_id})

    @app.head("/chunks/{chunk_id}")
    async def head_chunk(chunk_id: str):
        try:
            size = await run_in_threadpool(store.head, chunk_id)
        except InvalidHash:
            raise HTTPException(400, "invalid chunk id")
        except ChunkNotFound:
            raise HTTPException(404, "chunk not found")
        return Response(status_code=200, headers={"Content-Length": str(size), "X-Chunk-Sha256": chunk_id})

    @app.delete("/chunks/{chunk_id}")
    async def delete_chunk(chunk_id: str):
        try:
            deleted = await run_in_threadpool(store.delete, chunk_id)
        except InvalidHash:
            raise HTTPException(400, "invalid chunk id")
        if not deleted:
            raise HTTPException(404, "chunk not found")
        return {"chunk_id": chunk_id, "deleted": True}

    @app.get("/health")
    async def health():
        return {"status": "ok", "node_id": cfg.node_id, **store.stats()}

    return app
