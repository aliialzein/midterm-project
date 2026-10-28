from metadata_service.ring import HashRing


def test_ring_deterministic_and_distinct():
    ring = HashRing(vnodes=128)
    for node in ["n1", "n2", "n3", "n4"]:
        ring.add_node(node)
    nodes = ring.get_nodes("abcd" * 16, replicas=3)
    assert len(nodes) == 3
    assert len(set(nodes)) == len(nodes)
    assert ring.get_nodes("abcd" * 16, replicas=3) == nodes


def test_ring_replicas_capped_by_node_count():
    ring = HashRing(vnodes=64)
    for node in ["n1", "n2", "n3", "n4"]:
        ring.add_node(node)
    for i in range(1000):
        nodes = ring.get_nodes(f"hash-{i}", replicas=10)
        assert len(nodes) <= 4


def test_ring_balance_roughly_even():
    ring = HashRing(vnodes=128)
    for node in ["n1", "n2", "n3", "n4"]:
        ring.add_node(node)
    counts = {node: 0 for node in ["n1", "n2", "n3", "n4"]}
    for i in range(20_000):
        node = ring.primary(f"chunk-{i}")
        counts[node] += 1
    total = sum(counts.values())
    mean = total / 4
    low = min(counts.values())
    high = max(counts.values())
    assert low >= mean * 0.8
    assert high <= mean * 1.2


def test_ring_add_remove_changes_expected_subset():
    ring = HashRing(vnodes=128)
    for node in ["n1", "n2", "n3", "n4"]:
        ring.add_node(node)
    before = {f"hash-{i}": ring.primary(f"hash-{i}") for i in range(2000)}
    ring.add_node("n5")
    after = {f"hash-{i}": ring.primary(f"hash-{i}") for i in range(2000)}
    moved = sum(1 for key in before if before[key] != after[key]) / len(before)
    assert 0.15 <= moved <= 0.25
    for key in before:
        if before[key] != after[key]:
            assert after[key] == "n5"

    ring.remove_node("n5")
    final = {f"hash-{i}": ring.primary(f"hash-{i}") for i in range(2000)}
    for key in before:
        if before[key] != final[key]:
            assert final[key] == "n5" or final[key] in {"n1", "n2", "n3", "n4"}
