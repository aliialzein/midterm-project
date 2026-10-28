"""Background garbage collector for deleted object references.

The GC worker checks the queue periodically, verifies the chunk is truly dead,
then issues DELETE /chunks/{hash} requests to every relevant node. This allows
storage nodes to delete actual bytes after no object references remain.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any

import httpx

log = logging.getLogger("metadata_service.gc")


class GarbageCollector(threading.Thread):
    def __init__(self, db, ring, settings, *, daemon: bool = True):
        super().__init__(daemon=daemon)
        self.db = db
        self.ring = ring
        self.settings = settings
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        while not self._stop.is_set():
            try:
                run_gc_once(self.db, self.ring, grace_seconds=self.settings.gc_grace_seconds, replicas=self.settings.replication_factor)
            except Exception:
                log.exception("gc loop failed")
            self._stop.wait(self.settings.gc_interval)


def run_gc_once(db, ring, *, grace_seconds: float = 60.0, replicas: int = 3) -> int:
    """Run a single GC cycle and return the number of chunks cleaned up."""
    hashes = db.get_gc_queue(grace_seconds)
    cleaned = 0
    for chunk_hash in hashes:
        exists = db.get_chunk_locations(chunk_hash)
        if db.get_chunk_locations(chunk_hash):
            # The row still has node metadata for the live placement. Keep the
            # object around until the location entries are removed by the delete path.
            pass
        if db._conn is not None:
            pass
        # A chunk is only garbage-collected if no active rows remain in chunks.
        with db._conn() as conn:
            row = conn.execute("SELECT 1 FROM chunks WHERE chunk_hash = %s", (chunk_hash,)).fetchone()
            if row is not None:
                continue

        node_ids = {x[0] for x in db.get_chunk_locations(chunk_hash)}
        desired = set(ring.get_nodes(chunk_hash, replicas=replicas))
        targets = list(node_ids | desired)
        urls = []
        for node_id in targets:
            url = db.get_node_url(node_id)
            if url:
                urls.append(url.rstrip("/"))

        ok = True
        for url in urls:
            try:
                response = httpx.delete(f"{url}/chunks/{chunk_hash}", timeout=3.0)
                if response.status_code not in {200, 204, 404}:
                    ok = False
                    log.warning("gc delete failed for %s on %s: %s", chunk_hash, url, response.status_code)
            except Exception:
                ok = False
                log.exception("gc delete request failed for %s on %s", chunk_hash, url)
        if ok:
            db.remove_gc_entry(chunk_hash)
            cleaned += 1
    return cleaned
