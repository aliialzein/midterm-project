"""Chunking helpers for fixed-size and content-defined chunking.

The tuple shape is deliberately consistent across both chunkers:
(offset, length, sha256_hex, data). The offset tracks the logical byte position
within the original stream so a caller can reassemble pieces without any hidden
metadata, while the hash is the content digest for deduplication.
"""

from __future__ import annotations

import hashlib
from typing import Any, Iterable, Iterator, Union

BytesLike = Union[bytes, bytearray, memoryview]
StreamLike = Any


def _coerce_bytes(stream_or_bytes: StreamLike) -> bytes:
    if isinstance(stream_or_bytes, (bytes, bytearray, memoryview)):
        return bytes(stream_or_bytes)
    if hasattr(stream_or_bytes, "read"):
        return stream_or_bytes.read()
    raise TypeError("chunking input must be bytes-like or a readable stream")


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def fixed_chunks(stream_or_bytes: StreamLike, chunk_size: int = 4 * 1024 * 1024):
    """Yield fixed-size chunks from a bytes payload or a readable stream."""
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    data = _coerce_bytes(stream_or_bytes)
    offset = 0
    for start in range(0, len(data), chunk_size):
        chunk = data[start : start + chunk_size]
        yield offset, len(chunk), _sha256_hex(chunk), chunk
        offset += len(chunk)


def _make_gear_table() -> list[int]:
    """Generate the 256-entry gear table used by the rolling hash.

    The values are deterministic and stable across runs, which matters for tests
    that compare stream/bytes chunking and for reproducible placement results.
    """
    table: list[int] = []
    for i in range(256):
        v = (i * 0x9E3779B97F4A7C15) & 0xFFFFFFFFFFFFFFFF
        v ^= (v >> 33)
        v *= 0xFF51AFD7ED558CCD
        v ^= (v >> 33)
        v *= 0xC4CEB9FE1A85EC53
        v ^= (v >> 33)
        table.append(v & 0xFFFFFFFFFFFFFFFF)
    return table


_FASTCDC_GEAR = _make_gear_table()
_MASK_64 = 0xFFFFFFFFFFFFFFFF


def fastcdc_chunks(
    stream_or_bytes: StreamLike,
    min_size: int = 1024 * 1024,
    avg_size: int = 4 * 1024 * 1024,
    max_size: int = 8 * 1024 * 1024,
):
    """Chunk data using a pure-Python FastCDC-style rolling hash.

    The implementation keeps the hot inner loop tight and deterministic: it starts
    checking only after min_size, uses a stricter mask when below avg_size, and
    forces a cut at max_size. This is good enough for the object-store tests and
    for comparing with the fixed chunker on the same bytes.
    """
    if min_size <= 0 or avg_size <= 0 or max_size <= 0:
        raise ValueError("FastCDC sizes must all be > 0")
    if min_size > max_size:
        raise ValueError("min_size cannot exceed max_size")
    if avg_size > max_size:
        raise ValueError("avg_size cannot exceed max_size")

    data = _coerce_bytes(stream_or_bytes)
    if not data:
        return

    offset = 0
    pos = 0
    length = len(data)
    gear = _FASTCDC_GEAR
    while pos < length:
        limit = min(pos + max_size, length)
        cut = limit
        fp = 0
        for i in range(pos, limit):
            fp = ((fp << 1) + gear[data[i]]) & _MASK_64
            idx = i - pos + 1
            if idx < min_size:
                continue
            if idx >= max_size:
                cut = i + 1
                break
            if idx < avg_size:
                mask = 0x1FFF
            else:
                mask = 0x7FF
            if (fp & mask) == 0:
                cut = i + 1
                break
        chunk = data[pos:cut]
        yield offset, len(chunk), _sha256_hex(chunk), chunk
        offset += len(chunk)
        pos = cut
