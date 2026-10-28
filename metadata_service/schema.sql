CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    url TEXT,
    free_bytes BIGINT,
    used_bytes BIGINT,
    total_bytes BIGINT,
    chunk_count BIGINT,
    last_heartbeat TIMESTAMPTZ,
    in_ring BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS objects (
    id BIGSERIAL PRIMARY KEY,
    bucket TEXT,
    key TEXT,
    version INT,
    size BIGINT,
    chunker TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(bucket, key, version)
);

CREATE INDEX IF NOT EXISTS idx_objects_bucket_key_version
    ON objects(bucket, key, version DESC);

CREATE TABLE IF NOT EXISTS object_chunks (
    object_id BIGINT REFERENCES objects ON DELETE CASCADE,
    seq INT,
    chunk_hash TEXT,
    size INT,
    PRIMARY KEY(object_id, seq)
);

CREATE INDEX IF NOT EXISTS idx_object_chunks_chunk_hash
    ON object_chunks(chunk_hash);

CREATE TABLE IF NOT EXISTS chunks (
    chunk_hash TEXT PRIMARY KEY,
    size INT,
    ref_count INT NOT NULL CHECK (ref_count >= 0),
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS chunk_locations (
    chunk_hash TEXT,
    node_id TEXT,
    PRIMARY KEY(chunk_hash, node_id)
);

CREATE TABLE IF NOT EXISTS gc_queue (
    chunk_hash TEXT PRIMARY KEY,
    enqueued_at TIMESTAMPTZ DEFAULT now()
);
