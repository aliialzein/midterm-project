"""PostgreSQL access layer for the metadata service.

The implementation intentionally uses raw SQL and psycopg's sync connection pool
rather than an ORM so the service remains fast and transparent for object-store
operations, GC, and transactional ref counting.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable

import psycopg
from psycopg_pool import ConnectionPool


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


class MetadataDB:
    def __init__(self, database_url: str, *, schema_path: str | Path | None = None):
        self.database_url = database_url
        self.schema_path = Path(schema_path) if schema_path else None
        self.pool = ConnectionPool(conninfo=database_url, min_size=1, max_size=5)
        self.pool.open()

    def close(self) -> None:
        self.pool.close()

    def _conn(self):
        return self.pool.connection()

    def apply_schema(self, *, search_path: str | None = None) -> None:
        if self.schema_path is None:
            raise ValueError("schema_path is required")
        sql = self.schema_path.read_text(encoding="utf-8")
        with self._conn() as conn:
            if search_path:
                conn.execute(f"SET search_path TO {search_path}, public")
            conn.execute(sql)
            conn.commit()

    def set_search_path(self, conn, schema_name: str) -> None:
        if not schema_name:
            return
        q = _quote_identifier(schema_name)
        conn.execute(f"SET search_path TO {q}, public")

    def get_node(self, node_id: str):
        with self._conn() as conn:
            return conn.execute(
                "SELECT node_id, url, free_bytes, used_bytes, total_bytes, chunk_count, last_heartbeat, in_ring FROM nodes WHERE node_id = %s",
                (node_id,),
            ).fetchone()

    def upsert_node(self, payload: dict[str, Any]) -> None:
        node_id = payload["node_id"]
        url = payload.get("url") or ""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT in_ring FROM nodes WHERE node_id = %s",
                (node_id,),
            ).fetchone()
            insert_in_ring = True if row is None else bool(row[0])
            conn.execute(
                """
                INSERT INTO nodes (node_id, url, free_bytes, used_bytes, total_bytes, chunk_count, last_heartbeat, in_ring)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
                ON CONFLICT (node_id)
                DO UPDATE SET
                    url = EXCLUDED.url,
                    free_bytes = EXCLUDED.free_bytes,
                    used_bytes = EXCLUDED.used_bytes,
                    total_bytes = EXCLUDED.total_bytes,
                    chunk_count = EXCLUDED.chunk_count,
                    last_heartbeat = NOW(),
                    in_ring = COALESCE(nodes.in_ring, TRUE)
                """,
                (
                    node_id,
                    url,
                    payload.get("free_bytes"),
                    payload.get("used_bytes"),
                    payload.get("total_bytes"),
                    payload.get("chunk_count"),
                    insert_in_ring,
                ),
            )
            conn.commit()

    def list_nodes(self, heartbeat_timeout: float | None = None):
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT node_id, url, free_bytes, used_bytes, total_bytes, chunk_count, last_heartbeat, in_ring FROM nodes ORDER BY node_id"
            ).fetchall()
            result = []
            now = __import__("time").time()
            for row in rows:
                node_id, url, free_bytes, used_bytes, total_bytes, chunk_count, last_heartbeat, in_ring = row
                seconds = (now - last_heartbeat.timestamp()) if last_heartbeat else None
                alive = bool(last_heartbeat is not None and (heartbeat_timeout is None or seconds is not None and seconds <= heartbeat_timeout))
                result.append({
                    "node_id": node_id,
                    "url": url,
                    "free_bytes": free_bytes,
                    "used_bytes": used_bytes,
                    "total_bytes": total_bytes,
                    "chunk_count": chunk_count,
                    "last_heartbeat": last_heartbeat.isoformat() if last_heartbeat else None,
                    "seconds_since_heartbeat": round(seconds, 3) if seconds is not None else None,
                    "alive": alive,
                    "in_ring": bool(in_ring),
                })
            return result

    def set_ring_node(self, node_id: str, in_ring: bool) -> None:
        with self._conn() as conn:
            conn.execute(
                "UPDATE nodes SET in_ring = %s WHERE node_id = %s",
                (in_ring, node_id),
            )
            conn.commit()

    def ring_nodes(self):
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT node_id, url FROM nodes WHERE in_ring = TRUE ORDER BY node_id"
            ).fetchall()
            return rows

    def check_chunks(self, hashes: list[str]) -> dict[str, list[str]]:
        if not hashes:
            return {"known": [], "unknown": []}
        with self._conn() as conn:
            res = conn.execute(
                "SELECT chunk_hash FROM chunks WHERE chunk_hash = ANY (%s)",
                (hashes,),
            ).fetchall()
            known = {row[0] for row in res}
            return {
                "known": sorted(known),
                "unknown": [h for h in hashes if h not in known],
            }

    def next_version(self, bucket: str, key: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(version), 0) FROM objects WHERE bucket = %s AND key = %s",
                (bucket, key),
            ).fetchone()
            return int(row[0]) + 1

    def commit_object(self, bucket: str, key: str, size: int, chunker: str, chunks: list[dict[str, Any]]) -> int:
        """Commit a new object version with transactional dedup semantics."""
        if not chunks:
            raise ValueError("object cannot be empty")

        with self._conn() as conn:
            conn.execute("BEGIN")
            try:
                version = conn.execute(
                    "SELECT COALESCE(MAX(version), 0) + 1 FROM objects WHERE bucket = %s AND key = %s FOR UPDATE",
                    (bucket, key),
                ).fetchone()[0]
                occurrences: dict[str, int] = {}
                missing: list[str] = []
                for item in chunks:
                    chunk_hash = item["hash"]
                    occurrences[chunk_hash] = occurrences.get(chunk_hash, 0) + 1
                    if item.get("uploaded") is False:
                        exists = conn.execute(
                            "SELECT 1 FROM chunks WHERE chunk_hash = %s FOR UPDATE",
                            (chunk_hash,),
                        ).fetchone()
                        if exists is None:
                            missing.append(chunk_hash)
                if missing:
                    conn.execute("ROLLBACK")
                    raise ValueError(f"missing chunks: {missing}")

                for hash_value, count in occurrences.items():
                    size_for_hash = next((c["size"] for c in chunks if c["hash"] == hash_value), 0)
                    conn.execute(
                        """
                        INSERT INTO chunks (chunk_hash, size, ref_count)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (chunk_hash)
                        DO UPDATE SET
                            ref_count = chunks.ref_count + EXCLUDED.ref_count,
                            size = EXCLUDED.size
                        """,
                        (hash_value, size_for_hash, count),
                    )

                object_id = conn.execute(
                    "INSERT INTO objects (bucket, key, version, size, chunker) VALUES (%s, %s, %s, %s, %s) RETURNING id",
                    (bucket, key, version, size, chunker),
                ).fetchone()[0]

                for seq, item in enumerate(chunks):
                    conn.execute(
                        "INSERT INTO object_chunks (object_id, seq, chunk_hash, size) VALUES (%s, %s, %s, %s)",
                        (object_id, seq, item["hash"], item["size"]),
                    )
                    for node_id in item.get("nodes", []):
                        conn.execute(
                            "INSERT INTO chunk_locations (chunk_hash, node_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            (item["hash"], node_id),
                        )
                conn.execute(
                    "DELETE FROM gc_queue WHERE chunk_hash = ANY (%s)",
                    (list(occurrences),),
                )
                conn.commit()
                return version
            except Exception:
                conn.rollback()
                raise

    def delete_object(self, bucket: str, key: str, version: int | None = None) -> dict[str, Any]:
        with self._conn() as conn:
            conn.execute("BEGIN")
            try:
                if version is None:
                    rows = conn.execute(
                        "SELECT id, version FROM objects WHERE bucket = %s AND key = %s ORDER BY version",
                        (bucket, key),
                    ).fetchall()
                else:
                    rows = conn.execute(
                        "SELECT id, version FROM objects WHERE bucket = %s AND key = %s AND version = %s",
                        (bucket, key, version),
                    ).fetchall()
                deleted_versions = [row[1] for row in rows]
                orphaned_hashes: set[str] = set()
                for object_id, _ in rows:
                    chunks = conn.execute(
                        "SELECT chunk_hash, size FROM object_chunks WHERE object_id = %s ORDER BY seq",
                        (object_id,),
                    ).fetchall()
                    if not chunks:
                        continue
                    counts: dict[str, int] = {}
                    for chunk_hash, _ in chunks:
                        counts[chunk_hash] = counts.get(chunk_hash, 0) + 1
                    for chunk_hash, count in counts.items():
                        conn.execute(
                            "UPDATE chunks SET ref_count = ref_count - %s WHERE chunk_hash = %s",
                            (count, chunk_hash),
                        )
                        remaining = conn.execute(
                            "SELECT ref_count FROM chunks WHERE chunk_hash = %s",
                            (chunk_hash,),
                        ).fetchone()
                        if remaining is not None and remaining[0] <= 0:
                            orphaned_hashes.add(chunk_hash)
                            conn.execute("DELETE FROM chunks WHERE chunk_hash = %s", (chunk_hash,))
                            conn.execute(
                                "INSERT INTO gc_queue (chunk_hash) VALUES (%s) ON CONFLICT (chunk_hash) DO NOTHING",
                                (chunk_hash,),
                            )
                    conn.execute("DELETE FROM object_chunks WHERE object_id = %s", (object_id,))
                conn.execute("DELETE FROM objects WHERE bucket = %s AND key = %s AND version = ANY (%s)", (bucket, key, deleted_versions))
                for chunk_hash in sorted(orphaned_hashes):
                    conn.execute(
                        "DELETE FROM chunk_locations WHERE chunk_hash = %s",
                        (chunk_hash,),
                    )
                conn.commit()
                return {"deleted_versions": deleted_versions, "orphaned_chunks": len(orphaned_hashes)}
            except Exception:
                conn.rollback()
                raise

    def manifest(self, bucket: str, key: str, version: int | None = None):
        with self._conn() as conn:
            if version is not None:
                row = conn.execute(
                    "SELECT id, bucket, key, version, size, chunker, created_at FROM objects WHERE bucket = %s AND key = %s AND version = %s",
                    (bucket, key, version),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT id, bucket, key, version, size, chunker, created_at FROM objects WHERE bucket = %s AND key = %s ORDER BY version DESC LIMIT 1",
                    (bucket, key),
                ).fetchone()
            if row is None:
                return None
            object_id = row[0]
            chunks = conn.execute(
                "SELECT chunk_hash, size FROM object_chunks WHERE object_id = %s ORDER BY seq",
                (object_id,),
            ).fetchall()
            manifest_chunks = []
            for chunk_hash, chunk_size in chunks:
                locations = conn.execute(
                    "SELECT n.node_id, n.url FROM chunk_locations cl JOIN nodes n ON cl.node_id = n.node_id WHERE cl.chunk_hash = %s ORDER BY n.node_id",
                    (chunk_hash,),
                ).fetchall()
                ring_targets = []
                manifest_chunks.append({
                    "hash": chunk_hash,
                    "size": chunk_size,
                    "locations": [{"node_id": x[0], "url": x[1]} for x in locations],
                    "ring_targets": ring_targets,
                })
            return {
                "bucket": row[1],
                "key": row[2],
                "version": row[3],
                "size": row[4],
                "chunker": row[5],
                "created_at": row[6].isoformat() if row[6] else None,
                "chunks": manifest_chunks,
            }

    def list_bucket_objects(self, bucket: str, prefix: str = "", limit: int = 100):
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT key, size, version, created_at
                FROM (
                    SELECT o.key, o.size, o.version, o.created_at,
                           ROW_NUMBER() OVER (PARTITION BY o.key ORDER BY o.version DESC) AS rn
                    FROM objects o
                    WHERE o.bucket = %s AND o.key LIKE %s
                ) t
                WHERE t.rn = 1
                ORDER BY key
                LIMIT %s
                """,
                (bucket, f"{prefix}%", limit),
            ).fetchall()
            return [{"key": row[0], "size": row[1], "version": row[2], "created_at": row[3].isoformat() if row[3] else None} for row in rows]

    def stat(self):
        with self._conn() as conn:
            counts = conn.execute(
                "SELECT COUNT(*) FROM nodes UNION ALL SELECT COUNT(*) FROM objects UNION ALL SELECT COUNT(*) FROM chunks"
            ).fetchall()
            return {
                "node_count": counts[0][0],
                "object_count": counts[1][0],
                "chunk_count": counts[2][0],
            }

    def get_placement(self, chunk_hash: str, replicas: int, ring) -> list[dict[str, str]]:
        picked = ring.get_nodes(chunk_hash, replicas=replicas)
        rows = []
        if not picked:
            return rows
        with self._conn() as conn:
            for node_id in picked:
                row = conn.execute(
                    "SELECT node_id, url FROM nodes WHERE node_id = %s AND in_ring = TRUE",
                    (node_id,),
                ).fetchone()
                if row:
                    rows.append({"node_id": row[0], "url": row[1]})
        return rows

    def delete_chunk_locations(self, chunk_hash: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM chunk_locations WHERE chunk_hash = %s", (chunk_hash,))
            conn.commit()

    def get_chunk_locations(self, chunk_hash: str):
        with self._conn() as conn:
            return conn.execute(
                "SELECT node_id FROM chunk_locations WHERE chunk_hash = %s ORDER BY node_id",
                (chunk_hash,),
            ).fetchall()

    def get_node_url(self, node_id: str):
        with self._conn() as conn:
            row = conn.execute("SELECT url FROM nodes WHERE node_id = %s", (node_id,)).fetchone()
            return row[0] if row else None

    def mark_missing_chunk(self, chunk_hash: str) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO gc_queue (chunk_hash) VALUES (%s) ON CONFLICT (chunk_hash) DO NOTHING",
                (chunk_hash,),
            )
            conn.commit()

    def get_gc_queue(self, older_than_seconds: float):
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT chunk_hash FROM gc_queue WHERE enqueued_at < NOW() - (%s * INTERVAL '1 second')",
                (older_than_seconds,),
            ).fetchall()
            return [r[0] for r in rows]

    def remove_gc_entry(self, chunk_hash: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM gc_queue WHERE chunk_hash = %s", (chunk_hash,))
            conn.execute("DELETE FROM chunk_locations WHERE chunk_hash = %s", (chunk_hash,))
            conn.execute("DELETE FROM chunks WHERE chunk_hash = %s", (chunk_hash,))
            conn.commit()

    def delete_missing_chunk(self, chunk_hash: str) -> None:
        with self._conn() as conn:
            conn.execute("DELETE FROM chunks WHERE chunk_hash = %s", (chunk_hash,))
            conn.commit()
