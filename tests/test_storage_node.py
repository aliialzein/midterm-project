import hashlib

import pytest
from fastapi.testclient import TestClient

from storage_node.app import Settings, create_app


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


@pytest.fixture()
def client(tmp_path):
    cfg = Settings(node_id="test", node_url="", data_dir=str(tmp_path), metadata_url="",
                   heartbeat_interval=5, max_chunk_size=1024 * 1024)
    with TestClient(create_app(cfg)) as c:
        yield c


def test_put_get_head_delete_roundtrip(client):
    data = b'{"hello": "world"}'
    h = sha(data)

    r = client.put(f"/chunks/{h}", content=data)
    assert r.status_code == 201 and r.json()["stored"] is True

    r = client.get(f"/chunks/{h}")
    assert r.status_code == 200 and r.content == data

    r = client.head(f"/chunks/{h}")
    assert r.status_code == 200 and r.headers["content-length"] == str(len(data))

    assert client.delete(f"/chunks/{h}").status_code == 200
    assert client.get(f"/chunks/{h}").status_code == 404
    assert client.head(f"/chunks/{h}").status_code == 404
    assert client.delete(f"/chunks/{h}").status_code == 404


def test_put_is_idempotent_dedup(client):
    data = b"same bytes"
    h = sha(data)
    assert client.put(f"/chunks/{h}", content=data).status_code == 201
    r = client.put(f"/chunks/{h}", content=data)
    assert r.status_code == 200 and r.json()["deduplicated"] is True
    assert client.get("/health").json()["chunk_count"] == 1


def test_put_rejects_wrong_hash(client):
    r = client.put(f"/chunks/{sha(b'a')}", content=b"b")
    assert r.status_code == 400


@pytest.mark.parametrize("bad", ["abc", "../../etc/passwd", "Z" * 64, "A" * 64])
def test_invalid_ids_rejected(client, bad):
    assert client.get(f"/chunks/{bad}").status_code in (400, 404)
    assert client.put(f"/chunks/{bad}", content=b"x").status_code in (400, 404)


def test_corrupted_chunk_detected_and_quarantined(client, tmp_path):
    data = b"precious data" * 100
    h = sha(data)
    client.put(f"/chunks/{h}", content=data)

    # simulate bit rot directly on disk
    path = tmp_path / "chunks" / h[:2] / h[2:4] / h
    raw = bytearray(path.read_bytes())
    raw[0] ^= 0xFF
    path.write_bytes(bytes(raw))

    r = client.get(f"/chunks/{h}")
    assert r.status_code == 422
    assert list((tmp_path / "quarantine").iterdir())        # moved aside
    assert client.get(f"/chunks/{h}").status_code == 404    # no longer served
    assert client.get("/health").json()["chunk_count"] == 0


def test_oversized_chunk_rejected(client):
    data = b"x" * (1024 * 1024 + 1)
    assert client.put(f"/chunks/{sha(data)}", content=data).status_code == 413


def test_chunk_count_survives_restart(tmp_path):
    cfg = Settings("t", "", str(tmp_path), "", 5, 1024 * 1024)
    with TestClient(create_app(cfg)) as c:
        for i in range(3):
            d = f"chunk-{i}".encode()
            c.put(f"/chunks/{sha(d)}", content=d)
    with TestClient(create_app(cfg)) as c:
        assert c.get("/health").json()["chunk_count"] == 3
