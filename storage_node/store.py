"""Local-disk, content-addressed chunk store.

Layout on disk:
    <data_dir>/chunks/ab/cd/abcd...(64 hex chars)    <- the chunk bytes
    <data_dir>/tmp/                                   <- in-flight writes
    <data_dir>/quarantine/                            <- chunks that failed verification

The file name IS the SHA-256 of the content, so integrity can always be
re-checked without any external metadata.
"""
import hashlib
import os
import re
import shutil
import threading
import uuid
from pathlib import Path

HASH_RE = re.compile(r"^[0-9a-f]{64}$")


class InvalidHash(ValueError):
    """Chunk id is not a 64-char lowercase hex string."""


class HashMismatch(ValueError):
    """Uploaded bytes do not hash to the id in the URL."""


class ChunkNotFound(KeyError):
    pass


class ChunkCorrupted(Exception):
    """Stored bytes no longer match their hash (bit rot, disk error, tampering)."""


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ChunkStore:
    def __init__(self, data_dir: str | os.PathLike):
        self.root = Path(data_dir)
        self.chunks_dir = self.root / "chunks"
        self.tmp_dir = self.root / "tmp"
        self.quarantine_dir = self.root / "quarantine"
        for d in (self.chunks_dir, self.tmp_dir, self.quarantine_dir):
            d.mkdir(parents=True, exist_ok=True)
        # leftovers from a crash mid-write are never valid chunks
        for f in self.tmp_dir.iterdir():
            f.unlink(missing_ok=True)

        self._lock = threading.Lock()
        self._count = 0
        self._bytes = 0
        self._scan()

    # ---------- helpers ----------
    @staticmethod
    def _check(chunk_id: str) -> None:
        if not HASH_RE.match(chunk_id):
            raise InvalidHash(chunk_id)

    def _path(self, chunk_id: str) -> Path:
        # two-level fan-out keeps directories small (65k buckets)
        return self.chunks_dir / chunk_id[:2] / chunk_id[2:4] / chunk_id

    def _scan(self) -> None:
        count = size = 0
        for p in self.chunks_dir.rglob("*"):
            if p.is_file():
                count += 1
                size += p.stat().st_size
        self._count, self._bytes = count, size

    # ---------- operations ----------
    def put(self, chunk_id: str, data: bytes) -> bool:
        """Store a chunk. Returns True if newly written, False if it already existed (dedup)."""
        self._check(chunk_id)
        if sha256_hex(data) != chunk_id:
            raise HashMismatch(chunk_id)

        path = self._path(chunk_id)
        if path.exists():
            return False

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.tmp_dir / f"{chunk_id}.{uuid.uuid4().hex}"
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())  # durable before it becomes visible
        with self._lock:
            if path.exists():  # lost a race with an identical concurrent PUT
                tmp.unlink(missing_ok=True)
                return False
            os.replace(tmp, path)  # atomic: readers see all of the chunk or none of it
            self._count += 1
            self._bytes += len(data)
        return True

    def get(self, chunk_id: str) -> bytes:
        """Read a chunk and verify its hash. Corrupted chunks are quarantined and rejected."""
        self._check(chunk_id)
        path = self._path(chunk_id)
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            raise ChunkNotFound(chunk_id)

        if sha256_hex(data) != chunk_id:
            self._quarantine(chunk_id, path, len(data))
            raise ChunkCorrupted(chunk_id)
        return data

    def head(self, chunk_id: str) -> int:
        """Return chunk size in bytes. Cheap existence check (does not re-hash)."""
        self._check(chunk_id)
        try:
            return self._path(chunk_id).stat().st_size
        except FileNotFoundError:
            raise ChunkNotFound(chunk_id)

    def delete(self, chunk_id: str) -> bool:
        self._check(chunk_id)
        path = self._path(chunk_id)
        with self._lock:
            try:
                size = path.stat().st_size
                path.unlink()
            except FileNotFoundError:
                return False
            self._count -= 1
            self._bytes -= size
        return True

    def _quarantine(self, chunk_id: str, path: Path, size: int) -> None:
        with self._lock:
            if path.exists():
                shutil.move(str(path), self.quarantine_dir / f"{chunk_id}.{uuid.uuid4().hex[:8]}")
                self._count -= 1
                self._bytes -= size

    # ---------- stats for heartbeat ----------
    def stats(self) -> dict:
        usage = shutil.disk_usage(self.root)
        return {
            "chunk_count": self._count,
            "used_bytes": self._bytes,
            "free_bytes": usage.free,
            "total_bytes": usage.total,
        }
