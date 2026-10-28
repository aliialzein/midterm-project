"""Deterministic consistent-hash ring for node placement.

The ring is purely in-memory and intentionally read-mostly: membership changes are
coalesced into a sorted tuple and then swapped atomically so lookup calls never
have to rebuild state while reading.
"""

from __future__ import annotations

import hashlib
import threading
from bisect import bisect_left


def _sha256_bytes(value: str) -> bytes:
    return hashlib.sha256(value.encode("utf-8")).digest()


def _ring_position(value: str) -> int:
    return int.from_bytes(_sha256_bytes(value)[:8], byteorder="big", signed=False)


class HashRing:
    """A consistent hash ring with a fixed number of virtual nodes per physical node."""

    def __init__(self, vnodes: int = 128):
        if vnodes <= 0:
            raise ValueError("vnodes must be > 0")
        self.vnodes = int(vnodes)
        self._lock = threading.RLock()
        self._nodes: set[str] = set()
        self._ring: list[tuple[int, str]] = []

    @property
    def nodes(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._nodes))

    def add_node(self, node_id: str) -> None:
        with self._lock:
            self._nodes.add(node_id)
            self._rebuild()

    def remove_node(self, node_id: str) -> None:
        with self._lock:
            self._nodes.discard(node_id)
            self._rebuild()

    def _rebuild(self) -> None:
        ring: list[tuple[int, str]] = []
        for node_id in sorted(self._nodes):
            for i in range(self.vnodes):
                ring.append((_ring_position(f"{node_id}#{i}"), node_id))
        ring.sort(key=lambda item: item[0])
        self._ring = ring

    def get_nodes(self, chunk_hash: str, replicas: int = 1) -> list[str]:
        """Return distinct physical nodes for a chosen chunk hash.

        The ring is walked clockwise from the chunk hash position. This produces a
        stable result independent of insertion order and naturally de-dups by node.
        """
        if not chunk_hash:
            return []
        with self._lock:
            if not self._ring:
                return []
            replica_count = max(0, min(int(replicas), len(self._nodes)))
            if replica_count == 0:
                return []

            chunk_bytes = hashlib.sha256(chunk_hash.encode("utf-8")).digest()
            target_position = int.from_bytes(chunk_bytes[:8], byteorder="big", signed=False)

            positions = [pos for pos, _ in self._ring]
            start = bisect_left(positions, target_position)
            if start == len(positions):
                start = 0

            picked: list[str] = []
            seen: set[str] = set()
            for offset in range(len(self._ring)):
                _, node_id = self._ring[(start + offset) % len(self._ring)]
                if node_id in seen:
                    continue
                picked.append(node_id)
                seen.add(node_id)
                if len(picked) >= replica_count:
                    return picked
            return picked

    def primary(self, chunk_hash: str) -> str | None:
        nodes = self.get_nodes(chunk_hash, replicas=1)
        return nodes[0] if nodes else None
