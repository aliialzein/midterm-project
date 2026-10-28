"""Benchmark fixed-size vs FastCDC chunking and save a small report."""

from __future__ import annotations

import csv
import hashlib
import random
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from metadata_service.chunking import fastcdc_chunks, fixed_chunks

RESULTS = Path(__file__).resolve().parent / "results"
RESULTS.mkdir(parents=True, exist_ok=True)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def make_file(size: int, seed: int = 7) -> bytes:
    rnd = random.Random(seed)
    return bytes(rnd.getrandbits(8) for _ in range(size))


def dedup_ratio(base_chunks, variant_chunks) -> float:
    base_total = sum(length for _, length, _, _ in base_chunks)
    variant_total = sum(length for _, length, _, _ in variant_chunks)
    total = base_total + variant_total
    seen = {}
    for _, _, chunk_hash, chunk in base_chunks + variant_chunks:
        seen[chunk_hash] = max(seen.get(chunk_hash, 0), len(chunk))
    unique_total = sum(seen.values())
    return 1.0 - (unique_total / total) if total else 0.0


def benchmark(data: bytes, chunker):
    start = time.perf_counter()
    chunks = list(chunker(data))
    elapsed = time.perf_counter() - start
    total_bytes = sum(length for _, length, _, _ in chunks)
    avg_size = total_bytes / len(chunks) if chunks else 0.0
    throughput = (len(data) / (1024 * 1024)) / elapsed if elapsed else 0.0
    return chunks, {"chunk_count": len(chunks), "avg_chunk_size": avg_size, "throughput_mib_s": throughput}


def variants_for(base: bytes):
    return {
        "identical_copy": base,
        "insert_1_byte_start": b"X" + base,
        "overwrite_100_middle": base[:len(base) // 2] + (b"Y" * 100) + base[len(base) // 2 + 100 :],
        "append_1mib": base + (b"Z" * (1024 * 1024)),
    }


def main() -> None:
    base = make_file(32 * 1024 * 1024, seed=123)
    base_fixed, _ = benchmark(base, lambda b: fixed_chunks(b, 256 * 1024))
    base_cdc, _ = benchmark(base, lambda b: fastcdc_chunks(b, 64 * 1024, 256 * 1024, 1024 * 1024))

    rows = []
    for variant_name, variant_data in variants_for(base).items():
        for algorithm_name, chunker in [
            ("fixed", lambda b: fixed_chunks(b, 256 * 1024)),
            ("fastcdc", lambda b: fastcdc_chunks(b, 64 * 1024, 256 * 1024, 1024 * 1024)),
        ]:
            chunks, metrics = benchmark(variant_data, chunker)
            if algorithm_name == "fixed":
                base_reference = base_fixed
            else:
                base_reference = base_cdc
            ratio = dedup_ratio(base_reference, chunks)
            rows.append({
                "variant": variant_name,
                "algorithm": algorithm_name,
                "chunk_count": metrics["chunk_count"],
                "avg_chunk_size": metrics["avg_chunk_size"],
                "dedup_ratio": ratio,
                "throughput_mib_s": metrics["throughput_mib_s"],
            })

    csv_path = RESULTS / "chunking_benchmark.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["variant", "algorithm", "chunk_count", "avg_chunk_size", "dedup_ratio", "throughput_mib_s"],
        )
        writer.writeheader()
        writer.writerows(rows)

    labels = [f"{row['algorithm']}:{row['variant']}" for row in rows]
    values = [row["chunk_count"] for row in rows]
    plt.figure(figsize=(12, 6))
    plt.bar(labels, values)
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Chunk count")
    plt.title("Chunking benchmark")
    plt.tight_layout()
    plt.savefig(RESULTS / "chunking_benchmark.png", dpi=150)
    plt.close()

    print("algorithm\tvariant\tchunk_count\tavg_chunk_size\tdedup_ratio\tMiB/s")
    for row in rows:
        print(
            f"{row['algorithm']}\t{row['variant']}\t{row['chunk_count']}\t{row['avg_chunk_size']:.2f}\t{row['dedup_ratio']:.4f}\t{row['throughput_mib_s']:.2f}"
        )
    print(f"CSV: {csv_path}")
    print(f"PNG: {RESULTS / 'chunking_benchmark.png'}")


if __name__ == "__main__":
    main()
