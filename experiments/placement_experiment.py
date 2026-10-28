"""Pure in-memory placement experiment for a consistent hash ring."""

from __future__ import annotations

import csv
import hashlib
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from metadata_service.ring import HashRing

RESULTS = Path(__file__).resolve().parent / "results"
RESULTS.mkdir(parents=True, exist_ok=True)


def chunk_hashes(count: int):
    for i in range(count):
        yield hashlib.sha256(str(i).encode("utf-8")).hexdigest()


def summarize_counts(values):
    total = sum(values.values())
    shares = {node: count / total for node, count in values.items()}
    mean = total / len(values) if values else 0.0
    stddev = statistics.pstdev(list(values.values())) if len(values) > 1 else 0.0
    cv = stddev / mean if mean else 0.0
    return shares, cv


def naive_assign(chunk_hash: str, nodes: list[str]) -> str:
    idx = int.from_bytes(bytes.fromhex(chunk_hash[:8]), byteorder="big", signed=False) % len(nodes)
    return nodes[idx]


def main() -> None:
    chunks = list(chunk_hashes(100_000))
    rows = []
    balance_rows = []
    move_rows = []
    for vnodes in [1, 16, 64, 128, 256]:
        ring = HashRing(vnodes=vnodes)
        for node in ["n1", "n2", "n3", "n4"]:
            ring.add_node(node)
        counts = {node: 0 for node in ["n1", "n2", "n3", "n4"]}
        for ch in chunks:
            node = ring.primary(ch)
            counts[node] += 1
        shares, cv = summarize_counts(counts)
        row = {"vnodes": vnodes, "n1": shares["n1"], "n2": shares["n2"], "n3": shares["n3"], "n4": shares["n4"], "cv": cv}
        rows.append(row)
        balance_rows.append((vnodes, cv))

    csv_path = RESULTS / "placement_experiment.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["vnodes", "n1", "n2", "n3", "n4", "cv"])
        writer.writeheader()
        writer.writerows(rows)

    print("vnodes\tn1_share\tn2_share\tn3_share\tn4_share\tcv")
    for row in rows:
        print(f"{row['vnodes']}\t{row['n1']:.4f}\t{row['n2']:.4f}\t{row['n3']:.4f}\t{row['n4']:.4f}\t{row['cv']:.4f}")

    ring = HashRing(vnodes=128)
    for node in ["n1", "n2", "n3", "n4"]:
        ring.add_node(node)
    before = {ch: ring.primary(ch) for ch in chunks}
    ring.add_node("n5")
    after_add = {ch: ring.primary(ch) for ch in chunks}
    moved_primary_add = sum(1 for ch in chunks if before[ch] != after_add[ch]) / len(chunks)
    replicas_before = {ch: ring.get_nodes(ch, replicas=3) for ch in chunks}
    ring5 = HashRing(vnodes=128)
    for node in ["n1", "n2", "n3", "n4", "n5"]:
        ring5.add_node(node)
    replica_set_after_add = {ch: ring5.get_nodes(ch, replicas=3) for ch in chunks}
    moved_replica_add = sum(1 for ch in chunks if set(replicas_before.get(ch, [])) != set(replica_set_after_add.get(ch, []))) / len(chunks)

    ring6 = HashRing(vnodes=128)
    for node in ["n1", "n2", "n3", "n4", "n5"]:
        ring6.add_node(node)
    before_remove = {ch: ring6.primary(ch) for ch in chunks}
    ring6.remove_node("n5")
    after_remove = {ch: ring6.primary(ch) for ch in chunks}
    moved_primary_remove = sum(1 for ch in chunks if before_remove[ch] != after_remove[ch]) / len(chunks)

    move_rows.extend([
        {"scenario": "4->5 primary", "value": moved_primary_add},
        {"scenario": "4->5 replica", "value": moved_replica_add},
        {"scenario": "5->4 primary", "value": moved_primary_remove},
    ])

    naive_nodes = ["n1", "n2", "n3", "n4"]
    naive_counts = {node: 0 for node in naive_nodes}
    for ch in chunks:
        node = naive_assign(ch, naive_nodes)
        naive_counts[node] += 1
    actual_counts = {node: 0 for node in naive_nodes}
    ring4 = HashRing(vnodes=128)
    for node in naive_nodes:
        ring4.add_node(node)
    for ch in chunks:
        actual_counts[ring4.primary(ch)] += 1
    baseline = sum(abs(naive_counts[node] - actual_counts[node]) for node in naive_nodes) / len(chunks)

    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1)
    vnodes_list = [row["vnodes"] for row in rows]
    cv_list = [row["cv"] for row in rows]
    plt.plot(vnodes_list, cv_list, marker="o")
    plt.xlabel("vnodes")
    plt.ylabel("coefficient of variation")
    plt.title("Load balance by vnode count")
    plt.grid(True, alpha=0.3)

    plt.subplot(1, 2, 2)
    labels = ["4->5 primary", "4->5 replica", "5->4 primary"]
    values = [moved_primary_add, moved_replica_add, moved_primary_remove]
    plt.bar(labels, values, color=["steelblue", "darkorange", "forestgreen"])
    plt.axhline(0.2, color="red", linestyle="--", label="ideal ~20%")
    plt.legend()
    plt.title("Node-change movement")
    plt.ylabel("fraction moved")
    plt.tight_layout()
    plt.savefig(RESULTS / "placement_experiment.png", dpi=150)
    plt.close()

    print(f"primary_move_after_add={moved_primary_add:.4f} replica_set_move_after_add={moved_replica_add:.4f}")
    print(f"primary_move_after_remove={moved_primary_remove:.4f}")
    print(f"baseline_naive_4->5_shift={baseline:.4f}")
    print(f"CSV: {csv_path}")
    print(f"PNG: {RESULTS / 'placement_experiment.png'}")


if __name__ == "__main__":
    main()
