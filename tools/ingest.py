"""End-to-end CLI for the metadata and storage service.

The CLI intentionally keeps deployment assumptions shallow: it operates against a
metadata service and a set of storage nodes, and it rewrites node docker-internal
URLs to host-exposed ports when the caller starts it from the machine that runs
Docker Desktop.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
from pathlib import Path

import httpx

from metadata_service.chunking import fastcdc_chunks, fixed_chunks


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def rewrite_node_url(url: str, rewrite_nodes: bool = False) -> str:
    if not rewrite_nodes:
        return url
    match = re.match(r"^http://node(\d+):8000$", url)
    if not match:
        return url
    return f"http://localhost:900{match.group(1)}"


def _read_file(path: str) -> bytes:
    return Path(path).read_bytes()


def _candidate_nodes(metadata_url: str, chunk_hash: str, replicas: int = 3, rewrite_nodes: bool = False):
    result = httpx.get(f"{metadata_url.rstrip('/')}/placement/{chunk_hash}", params={"replicas": replicas}, timeout=10.0)
    result.raise_for_status()
    nodes = result.json()["nodes"]
    if rewrite_nodes:
        return [{"node_id": n["node_id"], "url": rewrite_node_url(n["url"], True)} for n in nodes]
    return nodes


def put_file(file_path: str, bucket: str, key: str, *, chunker: str, metadata_url: str, rewrite_nodes: bool):
    data = _read_file(file_path)
    if chunker == "fixed":
        chunks = list(fixed_chunks(data, 4 * 1024 * 1024))
    elif chunker == "cdc":
        chunks = list(fastcdc_chunks(data, 1024 * 1024, 4 * 1024 * 1024, 8 * 1024 * 1024))
    else:
        raise ValueError("chunker must be 'fixed' or 'cdc'")

    check = httpx.post(f"{metadata_url.rstrip('/')}/chunks/check", json={"hashes": [h for _, _, h, _ in chunks]}, timeout=10.0)
    check.raise_for_status()
    known = set(check.json()["known"]) if check.json() else set()

    uploaded = 0
    deduplicated = 0
    bytes_uploaded = 0
    objects = []
    for offset, length, chunk_hash, chunk_data in chunks:
        is_known = chunk_hash in known
        if is_known:
            deduplicated += 1
            nodes = _candidate_nodes(metadata_url, chunk_hash, replicas=3, rewrite_nodes=rewrite_nodes)
            objects.append({"hash": chunk_hash, "size": length, "uploaded": False, "nodes": [n["node_id"] for n in nodes]})
            continue
        nodes = _candidate_nodes(metadata_url, chunk_hash, replicas=3, rewrite_nodes=rewrite_nodes)
        for node in nodes:
            resp = httpx.put(f"{node['url'].rstrip('/')}/chunks/{chunk_hash}", content=chunk_data, timeout=10.0)
            if 200 <= resp.status_code < 300:
                uploaded += 1
                bytes_uploaded += len(chunk_data)
        objects.append({"hash": chunk_hash, "size": length, "uploaded": True, "nodes": [n["node_id"] for n in nodes]})

    commit = httpx.post(
        f"{metadata_url.rstrip('/')}/objects/commit",
        json={
            "bucket": bucket,
            "key": key,
            "size": len(data),
            "chunker": chunker,
            "chunks": objects,
        },
        timeout=20.0,
    )
    commit.raise_for_status()
    print(f"chunk_count={len(chunks)} unique={len(chunks)-deduplicated} deduplicated={deduplicated} bytes_uploaded={bytes_uploaded} bytes_skipped={sum(len(c[3]) for c in chunks if c[2] in known)}")
    return commit.json()


def get_file(bucket: str, key: str, outfile: str, *, metadata_url: str, rewrite_nodes: bool):
    manifest = httpx.get(f"{metadata_url.rstrip('/')}/objects/{bucket}/{key}", timeout=10.0)
    manifest.raise_for_status()
    manifest_json = manifest.json()
    out_path = Path(outfile)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pieces: list[bytes] = []
    for chunk in manifest_json["chunks"]:
        chunk_hash = chunk["hash"]
        urls = [loc["url"] for loc in chunk["locations"]]
        if rewrite_nodes:
            urls = [rewrite_node_url(url, True) for url in urls]
        if not urls:
            urls = [rewrite_node_url(item["url"], rewrite_nodes) for item in chunk.get("ring_targets", [])]
        data = None
        for url in urls:
            try:
                response = httpx.get(f"{url.rstrip('/')}/chunks/{chunk_hash}", timeout=10.0)
                if response.status_code == 200:
                    data = response.content
                    break
            except Exception:
                continue
        if data is None:
            raise RuntimeError(f"chunk {chunk_hash} unavailable from locations")
        if sha256_hex(data) != chunk_hash:
            raise RuntimeError(f"sha256 mismatch for chunk {chunk_hash}")
        pieces.append(data)
    blob = b"".join(pieces)
    if len(blob) != manifest_json["size"]:
        raise RuntimeError(f"size mismatch: expected {manifest_json['size']} got {len(blob)}")
    out_path.write_bytes(blob)
    print(f"downloaded {len(blob)} bytes to {outfile}")


def rm_file(bucket: str, key: str, *, metadata_url: str):
    result = httpx.delete(f"{metadata_url.rstrip('/')}/objects/{bucket}/{key}", timeout=10.0)
    result.raise_for_status()
    print(result.json())


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    put = sub.add_parser("put")
    put.add_argument("file")
    put.add_argument("bucket")
    put.add_argument("key")
    put.add_argument("--chunker", choices=["fixed", "cdc"], default="fixed")
    put.add_argument("--metadata", default="http://localhost:8000")
    put.add_argument("--rewrite-nodes", action="store_true")

    get = sub.add_parser("get")
    get.add_argument("bucket")
    get.add_argument("key")
    get.add_argument("outfile")
    get.add_argument("--metadata", default="http://localhost:8000")
    get.add_argument("--rewrite-nodes", action="store_true")

    rm = sub.add_parser("rm")
    rm.add_argument("bucket")
    rm.add_argument("key")
    rm.add_argument("--metadata", default="http://localhost:8000")

    args = parser.parse_args()
    if args.command == "put":
        put_file(args.file, args.bucket, args.key, chunker=args.chunker, metadata_url=args.metadata, rewrite_nodes=args.rewrite_nodes)
    elif args.command == "get":
        get_file(args.bucket, args.key, args.outfile, metadata_url=args.metadata, rewrite_nodes=args.rewrite_nodes)
    elif args.command == "rm":
        rm_file(args.bucket, args.key, metadata_url=args.metadata)


if __name__ == "__main__":
    main()
