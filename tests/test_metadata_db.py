import os
import random
import time

import pytest

from metadata_service.db import MetadataDB
from metadata_service.ring import HashRing

TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")

pytestmark = pytest.mark.skipif(TEST_DATABASE_URL is None or not TEST_DATABASE_URL.strip(), reason="TEST_DATABASE_URL not set")


@pytest.fixture()
def db():
    url = TEST_DATABASE_URL
    schema_name = f"test_{random.randint(1000, 999999)}"
    db = MetadataDB(url)
    with db._conn() as conn:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
        conn.execute(f"SET search_path TO {schema_name}, public")
        conn.execute(open("metadata_service/schema.sql", "r", encoding="utf-8").read())
        conn.commit()
    db.pool.connection().execute(f"SET search_path TO {schema_name}, public")
    yield db
    with db._conn() as conn:
        conn.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
        conn.commit()
    db.close()


def test_commit_versions_and_refcount(db):
    data1 = [{"hash": "aaaa" * 16, "size": 10, "uploaded": True, "nodes": ["n1"]}]
    data2 = [{"hash": "bbbb" * 16, "size": 15, "uploaded": True, "nodes": ["n1"]}]
    version1 = db.commit_object("b", "k", 10, "fixed", data1)
    version2 = db.commit_object("b", "k", 15, "fixed", data2)
    assert version1 == 1
    assert version2 == 2

    rows = db._conn().execute("SELECT ref_count FROM chunks WHERE chunk_hash IN ('aaaa' * 16, 'bbbb' * 16)").fetchall()
    assert rows


def test_shared_chunks_and_delete_last_reference(db):
    shared = {"hash": "cccc" * 16, "size": 8, "uploaded": True, "nodes": ["n1"]}
    obj1 = [{"hash": "aaaa" * 16, "size": 8, "uploaded": True, "nodes": ["n1"]}, shared]
    obj2 = [shared, {"hash": "bbbb" * 16, "size": 8, "uploaded": True, "nodes": ["n1"]}]
    db.commit_object("b", "k1", 16, "fixed", obj1)
    db.commit_object("b", "k2", 16, "fixed", obj2)
    db.delete_object("b", "k1", version=1)
    assert db._conn().execute("SELECT ref_count FROM chunks WHERE chunk_hash = %s", (shared["hash"],)).fetchone()[0] == 1

    db.delete_object("b", "k2", version=1)
    assert db._conn().execute("SELECT COUNT(*) FROM gc_queue WHERE chunk_hash = %s", (shared["hash"],)).fetchone()[0] == 1


def test_repeated_chunk_counts_twice_in_single_object(db):
    chunks = [
        {"hash": "dddd" * 16, "size": 4, "uploaded": True, "nodes": ["n1"]},
        {"hash": "dddd" * 16, "size": 4, "uploaded": True, "nodes": ["n1"]},
    ]
    db.commit_object("b", "k", 8, "fixed", chunks)
    ref = db._conn().execute("SELECT ref_count FROM chunks WHERE chunk_hash = %s", ("dddd" * 16,)).fetchone()[0]
    assert ref == 2


def test_missing_chunk_upload_false_409(db):
    with pytest.raises(ValueError):
        db.commit_object("b", "k", 10, "fixed", [{"hash": "eeee" * 16, "size": 10, "uploaded": False, "nodes": ["n1"]}])


def test_manifest_lists_locations_and_ordering(db):
    chunks = [
        {"hash": "1111" * 16, "size": 4, "uploaded": True, "nodes": ["n1", "n2"]},
        {"hash": "2222" * 16, "size": 5, "uploaded": True, "nodes": ["n2", "n3"]},
    ]
    db.commit_object("b", "k", 9, "fixed", chunks)
    manifest = db.manifest("b", "k", version=1)
    assert [c["hash"] for c in manifest["chunks"]] == ["1111" * 16, "2222" * 16]
    assert manifest["chunks"][0]["locations"]


def test_list_prefix_and_heartbeat_and_placement(db):
    db.upsert_node({"node_id": "n1", "url": "http://n1:8000", "free_bytes": 1, "used_bytes": 2, "total_bytes": 3, "chunk_count": 4})
    db.upsert_node({"node_id": "n2", "url": "http://n2:8000", "free_bytes": 1, "used_bytes": 2, "total_bytes": 3, "chunk_count": 4})
    db.commit_object("b", "a.txt", 10, "fixed", [{"hash": "abcd" * 16, "size": 10, "uploaded": True, "nodes": ["n1", "n2"]}])
    db.commit_object("b", "a.bin", 10, "fixed", [{"hash": "efgh" * 16, "size": 10, "uploaded": True, "nodes": ["n1", "n2"]}])
    ring = HashRing(vnodes=128)
    for node_id in ["n1", "n2"]:
        ring.add_node(node_id)
    rows = db.list_bucket_objects("b", prefix="a", limit=10)
    assert len(rows) == 2
    placements = db.get_placement("abcd" * 16, 2, ring)
    assert len(placements) == 2
    assert db.list_nodes(heartbeat_timeout=15)[0]["node_id"] == "n1"


def test_gc_queue_and_run_once(db):
    db.commit_object("b", "k", 10, "fixed", [{"hash": "ffff" * 16, "size": 10, "uploaded": True, "nodes": ["n1"]}])
    db.delete_object("b", "k", version=1)
    assert db._conn().execute("SELECT COUNT(*) FROM gc_queue").fetchone()[0] == 1
    db.remove_gc_entry("ffff" * 16)
    assert db._conn().execute("SELECT COUNT(*) FROM gc_queue").fetchone()[0] == 0
