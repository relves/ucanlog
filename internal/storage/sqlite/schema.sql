-- Schema for per-log SQLite database
-- Each database contains: main log + {logDID}-revocations log pair

PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

-- Log records (both main and revocations log DIDs)
CREATE TABLE IF NOT EXISTS logs (
    log_did TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

-- Tree state: Merkle tree metadata
CREATE TABLE IF NOT EXISTS tree_state (
    log_did TEXT PRIMARY KEY,
    size INTEGER NOT NULL DEFAULT 0,
    root BLOB,
    FOREIGN KEY (log_did) REFERENCES logs(log_did) ON DELETE CASCADE
);

-- Revocations: shared across the log pair
CREATE TABLE IF NOT EXISTS revocations (
    delegation_cid TEXT PRIMARY KEY,
    revoked_at TEXT NOT NULL
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_revocations_revoked_at ON revocations(revoked_at);

-- Latest head CAR per log: always holds the most recent sequenced CAR bytes.
-- Updated synchronously on every append (same tx as upload_queue insert).
-- Used for cold-start recovery without a gateway round-trip.
CREATE TABLE IF NOT EXISTS latest_head_car (
    log_did     TEXT PRIMARY KEY,
    tree_size   INTEGER NOT NULL,
    head_cid    TEXT NOT NULL,
    car_data    BLOB NOT NULL,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Upload queue: pending CARs awaiting Storacha upload.
CREATE TABLE IF NOT EXISTS upload_queue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    log_did     TEXT NOT NULL,
    tree_size   INTEGER NOT NULL,
    head_cid    TEXT NOT NULL,
    car_data    BLOB,                    -- nulled after successful upload
    status      TEXT NOT NULL DEFAULT 'pending',  -- pending | uploading | uploaded | failed
    created_at  TEXT NOT NULL DEFAULT (datetime('now')),
    uploaded_at TEXT,
    error       TEXT,
    retry_count INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_upload_queue_status ON upload_queue(status, created_at);
CREATE INDEX IF NOT EXISTS idx_upload_queue_log ON upload_queue(log_did, tree_size);

-- Finalized blobs that need separate IPNI-indexed upload.
CREATE TABLE IF NOT EXISTS upload_queue_blobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    queue_id    INTEGER NOT NULL REFERENCES upload_queue(id) ON DELETE CASCADE,
    path        TEXT NOT NULL,
    blob_cid    TEXT NOT NULL,
    blob_data   BLOB,                    -- nulled after successful upload
    status      TEXT NOT NULL DEFAULT 'pending',
    uploaded_at TEXT,
    error       TEXT
);

CREATE INDEX IF NOT EXISTS idx_upload_queue_blobs_status ON upload_queue_blobs(status);
