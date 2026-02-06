# Storacha Storage Personality for Tessera

This package provides a native Tessera storage driver backed by Storacha's decentralized storage network.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    storacha.Storage                             │
│  (implements tessera.Driver)                                    │
│                                                                 │
│  ┌──────────────────┐    ┌────────────────────────────────────┐ │
│  │  Appender()      │    │  objStore (interface)              │ │
│  │  - Returns       │    │  - setObject(path, data) → CID     │ │
│  │    Appender +    │    │  - getObject(path) → data          │ │
│  │    LogReader     │    │  - setObjectIfNoneMatch()          │ │
│  └──────────────────┘    └────────────────────────────────────┘ │
│                                      │                          │
│  ┌──────────────────┐                ▼                          │
│  │  coordinator     │    ┌────────────────────────────────────┐ │
│  │  - StateStore    │    │  StorachaClient                    │ │
│  │  - Sequencing    │    │  - UploadBlob() → CID              │ │
│  │  - Integration   │    │  - FetchBlob(CID) → data           │ │
│  └──────────────────┘    └────────────────────────────────────┘ │
│                                      │                          │
│  ┌──────────────────┐                ▼                          │
│  │  CIDIndex        │    ┌────────────────────────────────────┐ │
│  │  - path → CID    │    │  IPFS Gateway / Storacha Network   │ │
│  │  - Persisted     │    │  (decentralized storage)           │ │
│  │    in StateStore │    │                                    │ │
│  └──────────────────┘    └────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

**Key insight:** Storacha uses content-addressing (upload → CID), while Tessera uses path-addressing. The `objStore` layer bridges this by maintaining a path→CID index internally.

## Usage

### Basic Usage

```go
import (
    "github.com/relves/ucanlog/internal/storage/sqlite"
    "github.com/relves/ucanlog/internal/storage/storacha"
    "github.com/transparency-dev/tessera"
)

storeManager := sqlite.NewStoreManager("./data")
defer storeManager.CloseAll()

logDID := "did:key:z6Mk..." // Log identifier for state storage
stateStore, err := storeManager.GetStateStore(logDID)
if err != nil {
    // handle error
}

driver, err := storacha.New(ctx, storacha.Config{
    SpaceDID:   "did:key:z6Mk...", // Storacha space
    LogDID:     logDID,
    StateStore: stateStore,
    Client:     myStorachaClient, // StorachaClient implementation
})

// Use with Tessera
storage := driver.(*storacha.Storage)
appender, reader, err := storage.Appender(ctx, tessera.NewAppendOptions().
    WithCheckpointSigner(signer))
```

### With ucanlog Manager (single-tenant storage)

```go
import (
    "github.com/relves/ucanlog/internal/storage/sqlite"
    "github.com/relves/ucanlog/internal/storage/storacha"
    "github.com/relves/ucanlog/pkg/tlog"
)

storeManager := sqlite.NewStoreManager("./data")
defer storeManager.CloseAll()

cidStore := tlog.NewStateStoreCIDStore(storeManager.GetStateStore)

manager, _ := tlog.NewStorachaManager(
    "./data",
    signer,
    checkpointPrivateKey,
    "ucanlog",
    storachaClient,
    "did:key:z6Mk...", // SpaceDID
    cidStore,
)

// Use normally
manager.AddEntry(ctx, "my-log", []byte("data"))
```

## Configuration

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| SpaceDID | Yes | - | Storacha space DID (did:key:...) |
| StateStore | Yes | - | State storage (SQLite, etc.) for CID index + coordination |
| LogDID | Yes | - | Log identifier used by StateStore |
| Client | No | placeholder | StorachaClient for uploads/fetches |
| GatewayURL | No | https://ipfs.w3s.link | IPFS gateway for retrievals |
| ServiceURL | No | https://up.storacha.network | Storacha service URL |
| HTTPClient | No | http.DefaultClient | HTTP client for gateway/service requests |
| IndexPersistence | No | nil | Enable index CAR persistence (indexpersist.Config) |
| GC | No | nil | Enable bundle garbage collection (gc.Config) |

## How It Works

1. **Path→CID Mapping**: Tessera uses paths (e.g., `tile/entries/000/001`). Storacha uses CIDs. The driver maintains an internal index mapping paths to CIDs.

2. **Writes**: `setObject(path, data)` uploads to Storacha, gets CID, stores mapping.

3. **Reads**: `getObject(path)` looks up CID in index, fetches from Storacha/IPFS.

4. **Coordination**: Uses StateStore-backed coordination (SQLite). Multi-instance support depends on the StateStore implementation.

## Testing

```bash
# Run all tests
go test ./internal/storage/storacha/... -v

# Run specific test
go test ./internal/storage/storacha/... -v -run TestStorage_Appender
```

## Customer-Delegated Storage

Production usage relies on the `DelegatedClient` which uses customer-provided UCAN delegations. Each customer delegates access to their own Storacha space, and the service writes to that space on their behalf.

### How It Works

1. **Customer creates a Storacha space** and delegates capabilities to the ucanlog service
2. **Customer includes delegation** in their `tlog/append` requests
3. **Service uses delegation** to write to the customer's space via `DelegatedClient`
4. **Data is stored** in the customer's own Storacha space

### DelegatedClient

```go
import "github.com/relves/ucanlog/internal/storage/storacha"

// Create a delegated client for a customer's space
client, err := storacha.NewDelegatedClient(storacha.DelegatedClientConfig{
    ServiceSigner: serviceSigner,           // Service's identity
    Delegation:    customerDelegation,      // Customer's UCAN delegation
    SpaceDID:      "did:key:z6Mk...",       // Customer's space
})

// Upload to customer's space
cid, err := client.UploadBlob(ctx, spaceDID, data)
```

### ClientPool

For multi-tenant scenarios, use `ClientPool` to manage per-customer clients:

```go
pool, _ := storacha.NewClientPool(storacha.ClientPoolConfig{
    ServiceSigner: serviceSigner,
})

// Get or create a client for a specific customer/log
client, err := pool.GetClient(logID, spaceDID, delegation)

// Update delegation for subsequent requests
pool.UpdateClientDelegation(logID, newDelegation)
```

### Upload Workflow

The `DelegatedClient.UploadBlob` method implements the complete w3up upload protocol:
1. Invoke `space/blob/add` with customer's delegation as proof
2. Extract presigned URL from `blob/allocate` receipt in fork effects
3. HTTP PUT blob data to the presigned URL
4. Issue and submit `http/put` receipt via `ucan/conclude` to confirm upload
5. Return content CID

## Comparison with Other Drivers

| Feature | POSIX | AWS | GCP | Storacha |
|---------|-------|-----|-----|----------|
| Blob Storage | Filesystem | S3 | GCS | IPFS/Filecoin |
| Coordination | File locks | MySQL | Spanner | StateStore (SQLite) |
| Content Addressing | No | No | No | Yes (CIDs) |
| Decentralized | No | No | No | Yes |

*Multi-instance depends on StateStore implementation.
