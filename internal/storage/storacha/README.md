# Storacha Storage Personality for Tessera

This package provides a native Tessera storage driver backed by Storacha's decentralized storage network.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    storacha.Storage                             │
│  (implements tessera.Driver)                                    │
│                                                                 │
│  ┌──────────────────┐    ┌────────────────────────────────────┐ │
│  │  Appender()      │    │  objStore                          │ │
│  │  - Returns       │    │  - in-memory mutable tlog state    │ │
│  │    Appender +    │    │  - setObject / getObject           │ │
│  │    LogReader     │    │  - tracks finalized bundle CIDs    │ │
│  └──────────────────┘    └────────────────────────────────────┘ │
│                                                                 │
│  ┌──────────────────┐    ┌────────────────────────────────────┐ │
│  │  flushFn         │    │  SQLite (StateStore / QueueStore)  │ │
│  │  - Checkpoint    │    │  - latest_head_car (cold start)    │ │
│  │    signing       │    │  - upload_queue (async upload)     │ │
│  │  - Build hybrid  │    │  - coordination (sequencing)       │ │
│  │    CAR           │    └────────────────────────────────────┘ │
│  │  - Enqueue CAR   │                                           │
│  │  - Stash receipt │    ┌────────────────────────────────────┐ │
│  └──────────────────┘    │  uploadqueue.Manager (background)  │ │
│                          │  - polls upload_queue              │ │
│  ┌──────────────────┐    │  - uploads to Storacha             │ │
│  │  StorachaClient  │    │  - marks rows uploaded             │ │
│  │  (per-request,   │◄───│                                    │ │
│  │   delegated)     │    └────────────────────────────────────┘ │
│  └──────────────────┘                                           │
└─────────────────────────────────────────────────────────────────┘
```

**Key design:** writes are immediately sequenced and checkpointed, a hybrid CAR is built and enqueued locally, and the append receipt is returned to the caller — all before any Storacha network I/O. A background worker drains the queue asynchronously.

## Append Flow

1. `appender.Add(ctx, entry)` — entry enters Tessera's internal queue (maxSize=1, flushes immediately)
2. Tessera integrates the entry into the Merkle tree and requests a witness checkpoint signature
3. `flushFn` is called with the new tree size, root, and signed checkpoint
4. Finalized bundle CIDs are pre-computed locally (no network I/O)
5. A hybrid CAR is built in memory; its root CID becomes `head_cid`
6. `EnqueueAndUpdateHead` writes to SQLite atomically (upsert `latest_head_car`, insert `upload_queue` row)
7. Receipt `{index, tree_size, head_cid, checkpoint}` is returned to the caller
8. Background worker picks up the queue row, uploads to Storacha, marks it done

## Cold-Start Recovery

On startup, `storacha.New` calls `GetLatestHeadCAR` from SQLite. If a CAR is found locally it is parsed to restore in-memory state — no network fetch required. Only on first boot (or after a migration) is an IPFS gateway fetch needed.

After restoring state, the upload queue worker starts and immediately drains any rows left `pending` or `uploading` from a prior run.

## Usage

```go
import (
    "github.com/relves/ucanlog/internal/storage/sqlite"
    "github.com/relves/ucanlog/internal/storage/storacha"
    "github.com/transparency-dev/tessera"
)

storeManager := sqlite.NewStoreManager("./data")
defer storeManager.CloseAll()

store, err := storeManager.GetStore(logDID)
if err != nil {
    // handle error
}

driver, err := storacha.New(ctx, storacha.Config{
    SpaceDID:   "did:key:z6Mk...",
    LogDID:     logDID,
    StateStore: store,
    // Client is set per-request via SetClient; leave nil here
})

storage := driver.(*storacha.Storage)
defer storage.Close()

appender, reader, err := storage.Appender(ctx,
    tessera.NewAppendOptions().WithCheckpointSigner(signer))
```

## Configuration

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| SpaceDID | Yes | - | Storacha space DID (did:key:...) |
| StateStore | Yes | - | SQLite store for CAR queue + cold-start state |
| LogDID | Yes | - | Log identifier used by StateStore |
| Client | No | nil | Set per-request via `SetClient`; background worker uses this |
| GatewayURL | No | https://ipfs.w3s.link | IPFS gateway for cold-start fetch fallback |
| ServiceURL | No | https://up.storacha.network | Storacha service URL |
| HTTPClient | No | http.DefaultClient | HTTP client for gateway/service |

## Customer-Delegated Storage

Each append request carries a UCAN delegation (`Space→Agent→UCANlog`). A fresh `GuppyClient` is created from this delegation and installed via `SetClient` before `appender.Add` is called. The background upload worker then uses this client for the actual Storacha upload.

```go
client, err := pool.GetClient(spaceDID, delegation)
storage.SetClient(client)

index, err := appender.Add(ctx, tessera.NewEntry(data))
```

### Upload Protocol (GuppyClient)

For each CAR upload:
1. `space/blob/add` — allocate shard, get presigned PUT URL
2. HTTP PUT blob data to presigned URL
3. `ucan/conclude` with `http/put` receipt to confirm upload
4. `space/index/add` — register IPNI index
5. `upload/add` — register root CID in the space

### ClientPool

`ClientPool` manages per-space delegated clients for multi-tenant use:

```go
pool, _ := storacha.NewClientPool(storacha.ClientPoolConfig{
    ServiceSigner: serviceSigner,
})

client, err := pool.GetClient(spaceDID, delegation)
```

## Deduplication / Idempotency

`DequeuePendingCARs` atomically transitions rows from `pending → uploading` inside a SQLite transaction. Concurrent workers and poll cycles will never pick up the same row twice.

## Testing

```bash
go test ./internal/storage/storacha/... -v
go test ./internal/storage/storacha/... -v -run TestStorage_Appender
```

## Comparison with Other Drivers

| Feature | POSIX | AWS | GCP | Storacha |
|---------|-------|-----|-----|----------|
| Blob Storage | Filesystem | S3 | GCS | IPFS/Filecoin |
| Coordination | File locks | MySQL | Spanner | SQLite (local) |
