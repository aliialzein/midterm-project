import os
from contextlib import contextmanager

import pytest
from fastapi.testclient import TestClient

from metadata_service.app import create_app

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(TEST_DATABASE_URL is None or not TEST_DATABASE_URL.strip(), reason="TEST_DATABASE_URL not set")


@pytest.fixture()
def app_client(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", TEST_DATABASE_URL)
    with TestClient(create_app()) as client:
        yield client


def test_heartbeat_and_placement(app_client):
    response = app_client.post(
        "/heartbeat",
        json={"node_id": "n1", "url": "http://n1:8000", "timestamp": 1.0, "chunk_count": 3, "used_bytes": 10, "free_bytes": 20, "total_bytes": 30},
    )
    assert response.status_code == 200
    nodes = app_client.get("/nodes")
    assert any(n["node_id"] == "n1" for n in nodes.json())
    placement = app_client.get("/placement/abcd" + "cd" * 16, params={"replicas": 2})
    assert placement.status_code == 200
    assert len(placement.json()["nodes"]) >= 1


def test_commit_and_manifest(app_client):
    payload = {
        "bucket": "b",
        "key": "k",
        "size": 10,
        "chunker": "fixed",
        "chunks": [{"hash": "a" * 64, "size": 10, "uploaded": True, "nodes": ["n1"]}],
    }
    commit = app_client.post("/objects/commit", json=payload)
    assert commit.status_code in {200, 201, 409}

    out = app_client.get("/objects/b/k")
    assert out.status_code in {200, 404}


def test_delete_and_bucket_list(app_client):
    app_client.post("/objects/commit", json={"bucket": "b", "key": "k", "size": 10, "chunker": "fixed", "chunks": [{"hash": "b" * 64, "size": 10, "uploaded": True, "nodes": ["n1"]}]})
    delete = app_client.delete("/objects/b/k")
    assert delete.status_code == 200
    out = app_client.get("/buckets/b/objects")
    assert out.status_code == 200
