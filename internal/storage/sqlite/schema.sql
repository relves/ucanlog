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

-- CID index: maps Tessera paths to Storacha CIDs
CREATE TABLE IF NOT EXISTS cid_index (
    log_did TEXT NOT NULL,
    path TEXT NOT NULL,
    cid TEXT NOT NULL,
    PRIMARY KEY (log_did, path),
    FOREIGN KEY (log_did) REFERENCES logs(log_did) ON DELETE CASCADE
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

-- Index persistence metadata: tracks when index was last persisted to Storacha
CREATE TABLE IF NOT EXISTS index_persistence (
    log_did TEXT PRIMARY KEY,
    last_upload_time TEXT NOT NULL,
    last_uploaded_size INTEGER NOT NULL DEFAULT 0,
    last_uploaded_cid TEXT,
    FOREIGN KEY (log_did) REFERENCES logs(log_did) ON DELETE CASCADE
);

-- GC progress: tracks garbage collection progress
CREATE TABLE IF NOT EXISTS gc_progress (
    log_did TEXT PRIMARY KEY,
    from_size INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (log_did) REFERENCES logs(log_did) ON DELETE CASCADE
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_cid_index_log_did ON cid_index(log_did);
CREATE INDEX IF NOT EXISTS idx_revocations_revoked_at ON revocations(revoked_at);
