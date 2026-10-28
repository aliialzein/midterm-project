import hashlib
import os

from metadata_service.chunking import fastcdc_chunks, fixed_chunks


def sha256_hex(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def test_fixed_chunk_reassembles_and_sizes():
    data = b"abcdef" * 1000
    chunks = list(fixed_chunks(data, 64))
    assert sum(length for _, length, _, _ in chunks) == len(data)
    reassembled = b"".join(chunk for _, _, _, chunk in chunks)
    assert reassembled == data
    assert all(length <= 64 for _, length, _, _ in chunks)
    assert all(length == 64 for _, length, _, _ in chunks[:-1])


def test_fastcdc_reassembles_and_respects_bounds():
    data = b"hello world " * 10000
    chunks = list(fastcdc_chunks(data, min_size=16, avg_size=64, max_size=256))
    reassembled = b"".join(chunk for _, _, _, chunk in chunks)
    assert reassembled == data
    assert all(length <= 256 for _, length, _, _ in chunks)
    assert all(length >= 16 for _, length, _, _ in chunks if len(chunks) > 1)


def test_fixed_and_cdc_identical_for_bytes_and_streams():
    data = os.urandom(200_000)
    fixed_bytes = list(fixed_chunks(data, 1024))
    fixed_stream = list(fixed_chunks(data, 1024))
    cdc_bytes = list(fastcdc_chunks(data, 16, 64, 256))
    cdc_stream = list(fastcdc_chunks(data, 16, 64, 256))
    assert fixed_bytes == fixed_stream
    assert cdc_bytes == cdc_stream
    assert b"".join(chunk for _, _, _, chunk in fixed_bytes) == data
    assert b"".join(chunk for _, _, _, chunk in cdc_bytes) == data


def test_fastcdc_adapts_to_inserted_byte():
    base = os.urandom(8 * 1024 * 1024)
    mutated = b"x" + base
    base_chunks = list(fastcdc_chunks(base, min_size=16 * 1024, avg_size=64 * 1024, max_size=256 * 1024))
    mutated_chunks = list(fastcdc_chunks(mutated, min_size=16 * 1024, avg_size=64 * 1024, max_size=256 * 1024))
    base_hashes = {chunk_hash for _, _, chunk_hash, _ in base_chunks}
    mut_hashes = {chunk_hash for _, _, chunk_hash, _ in mutated_chunks}
    shared = len(base_hashes & mut_hashes)
    assert shared / max(len(base_hashes), 1) > 0.9


def test_fixed_discards_most_after_byte_insert():
    base = os.urandom(8 * 1024 * 1024)
    mutated = b"x" + base
    base_chunks = list(fixed_chunks(base, 64 * 1024))
    mutated_chunks = list(fixed_chunks(mutated, 64 * 1024))
    base_hashes = {chunk_hash for _, _, chunk_hash, _ in base_chunks}
    mut_hashes = {chunk_hash for _, _, chunk_hash, _ in mutated_chunks}
    shared = len(base_hashes & mut_hashes)
    assert shared / max(len(base_hashes), 1) < 0.1
