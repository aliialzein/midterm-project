"""FastAPI application factory for the metadata service."""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query, status
from pydantic import BaseModel, Field

from .config import Settings
from .db import MetadataDB
from .gc import GarbageCollector
from .ring import HashRing

log = logging.getLogger("metadata_service")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


class HeartbeatPayload(BaseModel):
    node_id: str
    url: str = ""
    timestamp: float
    chunk_count: int
    used_bytes: int
    free_bytes: int
    total_bytes: int


class ChunkRef(BaseModel):
    hash: str
    size: int
    uploaded: bool = False
    nodes: list[str] = Field(default_factory=list)


class CommitPayload(BaseModel):
    bucket: str
    key: str
    size: int
    chunker: str = "fixed"
    chunks: list[ChunkRef]


class ChunkCheckPayload(BaseModel):
    hashes: list[str]


class MetadataNode:
    def __init__(self, db: MetadataDB, ring: HashRing, settings: Settings):
        self.db = db
        self.ring = ring
        self.settings = settings


def _load_ring(db: MetadataDB, ring: HashRing) -> None:
    for node_id, url in db.ring_nodes():
        ring.add_node(node_id)


def create_app(settings: Settings | None = None) -> FastAPI:
    cfg = settings or Settings.from_env()
    db = MetadataDB(cfg.database_url, schema_path=Path(__file__).with_name("schema.sql"))
    ring = HashRing(cfg.vnodes)
    _load_ring(db, ring)

    gc = None
    if not cfg.disable_gc:
        gc = GarbageCollector(db, ring, cfg)
        gc.start()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            _load_ring(db, ring)
            yield
        finally:
            if gc is not None:
                gc.stop()
                gc.join(timeout=5)
            db.close()

    app = FastAPI(title="Metadata Service", lifespan=lifespan)

    @app.on_event("startup")
    def startup() -> None:
        db.apply_schema()
        _load_ring(db, ring)

    @app.post("/heartbeat")
    def heartbeat(payload: HeartbeatPayload):
        db.upsert_node(payload.model_dump())
        node_id = payload.node_id
        if node_id not in ring.nodes:
            ring.add_node(node_id)
        db.set_ring_node(node_id, True)
        return {"ok": True, "node_id": node_id}

    @app.get("/nodes")
    def list_nodes():
        rows = db.list_nodes(cfg.heartbeat_timeout)
        return rows

    @app.delete("/ring/nodes/{node_id}")
    def remove_ring_node(node_id: str):
        db.set_ring_node(node_id, False)
        ring.remove_node(node_id)
        return {"node_id": node_id, "in_ring": False}

    @app.post("/ring/nodes/{node_id}")
    def add_ring_node(node_id: str):
        db.set_ring_node(node_id, True)
        ring.add_node(node_id)
        return {"node_id": node_id, "in_ring": True}

    @app.get("/placement/{chunk_hash}")
    def placement(chunk_hash: str, replicas: int = Query(default=3, ge=1)):
        replica_limit = min(replicas, cfg.replication_factor, max(1, len(ring.nodes)))
        positions = db.get_placement(chunk_hash, replica_limit, ring)
        return {"chunk_hash": chunk_hash, "nodes": positions}

    @app.post("/chunks/check")
    def chunks_check(payload: ChunkCheckPayload):
        return db.check_chunks(payload.hashes)

    @app.post("/objects/commit")
    def commit_object(payload: CommitPayload):
        if not payload.chunks:
            raise HTTPException(status.HTTP_400_BAD_REQUEST, "object must contain chunks")
        try:
            version = db.commit_object(payload.bucket, payload.key, payload.size, payload.chunker, [chunk.model_dump() for chunk in payload.chunks])
        except ValueError as exc:
            missing = list(str(exc).split(":", 1)[1].strip("[]' ")) if ":" in str(exc) else []
            raise HTTPException(status.HTTP_409_CONFLICT, {"missing_chunks": missing})
        return {"bucket": payload.bucket, "key": payload.key, "version": version}

    @app.get("/objects/{bucket}/{key:path}")
    def get_manifest(bucket: str, key: str, version: int | None = None):
        manifest = db.manifest(bucket, key, version)
        if manifest is None:
            raise HTTPException(status.HTTP_404_NOT_FOUND, "object not found")
        for chunk in manifest["chunks"]:
            chunk["ring_targets"] = [
                {"node_id": node_id, "url": db.get_node_url(node_id) or ""}
                for node_id in ring.get_nodes(chunk["hash"], replicas=cfg.replication_factor)
            ]
        return manifest

    @app.delete("/objects/{bucket}/{key:path}")
    def delete_object(bucket: str, key: str, version: int | None = None):
        result = db.delete_object(bucket, key, version)
        return result

    @app.get("/buckets/{bucket}/objects")
    def list_bucket_objects(bucket: str, prefix: str = "", limit: int = Query(default=100, ge=1)):
        return db.list_bucket_objects(bucket, prefix=prefix, limit=limit)

    @app.get("/stat")
    def stat():
        values = db.stat()
        values["ring_nodes"] = len(ring.nodes)
        return values

    @app.get("/health")
    def health():
        return {"status": "ok", "database_url": cfg.database_url}

    return app
