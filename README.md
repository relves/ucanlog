
# ![UCANLOG](/ucanlog-logo.svg) - Transparency Log with Decentralized Auth & Storage

![ucanlog infographic](/ucanlog-infographic.png)

## Overview

UCANLOG is a Go library for building transparent appand-only log services using [Tessera](https://github.com/transparency-dev/tessera) with capability-based access control via [UCANs](https://ucan.xyz/) and user owned storage via [Storacha](https://storacha.network).

## Status

Functional POC/RFC. Work in progress. Use at your own risk.

## Motivation 

The goal of this library is to form the foundation for "Proof-as-a-Service". It enables the creation of user owned append-only logs that can be used as verifiable evidence for use cases such as chains of custody, provenance and authorship proofs, credential issuance, and key agreement protocols. Each record can be a public commitment to either pubic OR private data via hashing (depending on your usecase and privacy model).

## Distinctive Features

UCANLOG uses the same tamper-proof, cryptographic record keeping system used by Certificate Authorities but with the following design goals: 

### User owned/controlled authorization
- Users manage access control to their own logs (UCAN enforcement)
- Users can delegate access to others (UCAN validation of delegations)
- Write access can be immediatly revoked (UCAN revocation enforcement)

### User owned/controlled data storage
- Users can credibly take their transparency logs to another service.

### No blockchain
- No crypto wallets
- No crypto currency


## Quick Start

### Installation

```bash
go get github.com/relves/ucanlog
```

### Basic Usage

```go
import (
    "github.com/relves/ucanlog/pkg/log"
    "github.com/relves/ucanlog/pkg/server"
    "github.com/relves/ucanlog/pkg/tlog"
    "github.com/relves/ucanlog/pkg/ucan"
    "github.com/relves/ucanlog/internal/storage/sqlite"
)

// Create SQLite store manager
storeManager := sqlite.NewStoreManager("./data")
defer storeManager.CloseAll()

// Create log manager
tlogMgr, err := tlog.NewDelegatedManager(tlog.DelegatedManagerConfig{
    BasePath:      "./data",
    ServiceSigner: serviceSigner,
    // ... other config
})
if err != nil {
    log.Fatal(err)
}

// Create log service
logService := log.NewLogServiceWithConfig(log.LogServiceConfig{
    TlogManager:  tlogMgr,
    LogMetaStore: tlog.NewLogMetaStore("./data"),
    ServiceDID:   serviceSigner.DID().String(),
    StoreManager: storeManager,
})

// Create server
srv, err := server.NewServer(
    server.WithSigner(serviceSigner),
    server.WithLogService(logService),
    server.WithStoreManager(storeManager),
)
if err != nil {
    log.Fatal(err)
}
```


#### Example Flow

```go
// 1. Get current head (or use empty string for first append)
indexCID := "" // Or fetch from previous response

// 2. Prepare append with expected head
caveats := AppendCaveats{
    Data:       base64Data,
    IndexCID:   indexCID,      // Expected head (for optimistic concurrency)
    Delegation: delegation,
}

// 3. Send append request
result, err := client.Append(ctx, caveats)
if err != nil {
    // Handle HeadMismatch error - refresh and retry
}

// 4. Update local state with new head
indexCID = result.NewIndexCID
treeSize = result.TreeSize
```

See [SEQUENCE DIAGRAM](docs/SEQUENCE_DIAG.md) for the data flow accross servcies.


## Public API

### Server Options

```go
server := server.NewServer(
    server.WithSigner(serviceSigner),           // Required: Service signer
    server.WithLogService(logService),          // Required: Log operations
    server.WithStoreManager(storeManager),      // Required: SQLite state storage
    server.WithValidator(validator),            // Optional: Custom validator
)
```

### Request Validation

Implement custom validation by implementing `server.RequestValidator`:

```go
type RequestValidator interface {
    ValidateRequest(ctx context.Context, inv invocation.Invocation) error
}
```

## Configuration

### Environment Variables (when used as standalone service)

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `DATA_PATH` | Directory for log storage | `./data` | No |
| `IPFS_GATEWAY_URL` | IPFS gateway used to proxy tlog-tiles data | `https://w3s.link` | No |
| `LOG_LEVEL` | Minimum log level (`debug`, `info`, `warn`, `error`) | `info` | No |
| `PORT` | HTTP server port | `8080` | No |
| `UCANLOG_PRIVATE_KEY` | Base64-encoded Ed25519 private key | Generated | No |

## API Capabilities

### tlog/create
Creates a new transparent log. The space DID from the delegation becomes the log identity.

**Caveats:**
- `delegation`: Base64-encoded UCAN delegation granting write access

**Returns:**
- `logId`: The space DID (e.g., `did:key:z6Mk...`)
- `index_cid`: Initial head CID (empty string for new log)
- `tree_size`: Initial tree size (0 for new log)

### tlog/append
Appends data to an existing log. Requires delegation on every request (stateless auth).

**Caveats:**
- `data`: Base64-encoded data to append
- `index_cid`: Expected current head CID (for optimistic concurrency)
- `delegation`: Base64-encoded UCAN delegation (required)

**Returns:**
- `index`: The index of the appended entry
- `new_index_cid`: New head CID after append
- `tree_size`: New tree size after append

**Errors:**
- `HeadMismatch`: Expected head doesn't match current head (concurrent modification detected)

### tlog/read
Reads entries from a log with optional pagination.

**Caveats:**
- `offset`: Starting index (optional, default: 0)
- `limit`: Maximum entries to return (optional, default: 100)

**Returns:**
- `entries`: Array of log entries
- `total`: Total number of entries in the log

### tlog/revoke
Revokes UCAN delegations. The delegation to revoke must first be stored in the space.

**Caveats:**
- `cid`: CID of the delegation to revoke (must be stored in the space)
- `delegation`: Base64-encoded storage delegation (for fetching and writing)

**Workflow:**
1. Client stores the delegation as a blob in their space
2. Client sends revocation request with the CID
3. Service fetches the delegation, validates authority, and adds CID to revocation log

**Authorization:** Only the delegation issuer or upstream authorities can revoke.

### tlog/gc
Runs manual garbage collection to remove obsolete partial bundles. Requires a direct `space/blob/remove` delegation from the space owner.

**Caveats:**
- `logId`: The space DID (log identifier)
- `delegation`: Base64-encoded UCAN delegation granting `space/blob/remove` for the space DID (must be direct, no proof chain)

**Returns:**
- `bundlesProcessed`: Number of bundles processed
- `blobsRemoved`: Number of blobs successfully removed
- `bytesFreed`: Bytes freed (estimated; may be 0)
- `newGCPosition`: New GC checkpoint position

## HTTP Query Endpoints

### GET /logs/{logID}/head

Retrieves the current state of a log without UCAN authentication. Useful for clients to check the current head before sending append requests.

**Parameters:**
- `logID`: The log DID (space DID)

**Returns (JSON):**
```json
{
  "index_cid": "bafyCurrentHead",
  "tree_size": 42,
  "checkpoint_cid": "bafyCheckpoint"  // Optional
}
```

**Status Codes:**
- `200 OK`: Log found and state returned
- `404 Not Found`: Log does not exist
- `400 Bad Request`: Invalid logID parameter

**Example:**
```bash
curl http://localhost:8080/logs/did:key:z6Mk.../head
```

**Use Cases:**
- Check current head before append (optimistic concurrency)
- Monitor log growth (tree size)
- Verify checkpoint publication

### tlog-tiles API

UCANLOG exposes read-only tile endpoints compatible with the [tlog-tiles specification](https://github.com/C2SP/C2SP/blob/main/tlog-tiles.md). These endpoints proxy tile data from IPFS and do not require UCAN authentication.

#### GET /logs/{logID}/checkpoint

Returns the latest signed checkpoint for the log.

**Returns:** `text/plain` — the checkpoint body

**Cache:** `public, max-age=5` (short-lived for freshness)

**Example:**
```bash
curl http://localhost:8080/logs/did:key:z6Mk.../checkpoint
```

#### GET /logs/{logID}/tile/{level}/{tilePath...}

Returns a Merkle tree tile at the given level and index.

**Parameters:**
- `level`: Tile tree level (0–63)
- `tilePath`: Tile index encoded as path segments, e.g. `x000/x001/234` or `x000/x001/234.p/128` for partial tiles

**Returns:** `application/octet-stream` — raw tile bytes

**Cache:** `public, max-age=31536000, immutable`

**Example:**
```bash
# Full tile at level 0, index 0
curl http://localhost:8080/logs/did:key:z6Mk.../tile/0/x000/000

# Partial tile (128 entries wide)
curl http://localhost:8080/logs/did:key:z6Mk.../tile/0/x000/001.p/128
```

#### GET /logs/{logID}/tile/entries/{entryPath...}

Returns a bundle of log entry data.

**Parameters:**
- `entryPath`: Entry bundle index encoded as path segments, same format as `tilePath`

**Returns:** `application/octet-stream` — raw entry bundle bytes

**Cache:** `public, max-age=31536000, immutable`

**Example:**
```bash
curl http://localhost:8080/logs/did:key:z6Mk.../tile/entries/x000/000
```

#### Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `IPFS_GATEWAY_URL` | IPFS gateway used to proxy tile data | `https://w3s.link` |

## Delegation Model

### Space DID as Log Identity

The customer's Storacha **space DID** serves as the **log identity**:

- One log per Storacha space
- The space DID is extracted from the delegation's capability resources
- All capabilities must target the same space DID

### Required Capabilities

Delegations must include these Storacha capabilities:

1. `space/blob/add` - Upload blob data
2. `space/index/add` - Register index CARs
3. `upload/add` - Register uploads

### Creating a Delegation

```go
dlg, err := delegation.Delegate(
    customerSigner,  // Your space key
    serviceDID,      // Service's DID
    []ucan.Capability[ucan.NoCaveats]{
        ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
        ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
        ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
    },
)

// Encode for API
encoded, _ := delegation.Format(dlg)
```

See [UCAN_DELEGATION_SPEC](docs/UCAN_DELEGATION_SPEC.md) for complete documentation.

## Security Model

### UCAN-Based Authorization

UCANLOG uses UCANs for decentralized authorization:

1. **Service Identity**: Service has Ed25519 key pair and DID
2. **Capability Delegation**: Clients prove authorization through delegation chains
3. **Stateless Authentication**: Every request validated independently
4. **Attenuated Permissions**: Fine-grained access control
5. **Pluggable Validation**: Custom request validators for additional checks

### Revocation Authority

Delegation revocation follows authority hierarchy:

- **Issuers can revoke**: The principal who issued a delegation can revoke it
- **Upstream can revoke downstream**: Space owners can revoke any downstream delegation
- **Recipients cannot revoke**: Receiving a delegation doesn't grant revocation rights

### Invocation Authorization

Every API request must be signed by the principal who created the delegation being used. This prevents "delegation theft" where an attacker finds a public delegation and attempts to use it.

### Key Concepts

- **Space DID as Identity**: Customer's Storacha space DID identifies the log
- **Delegation Chains**: Capabilities can be delegated with restrictions
- **Audience**: Delegations addressed to service DID

## Development

### Prerequisites

- Go 1.21+

### Building

```bash
# Run tests
go test ./...

# Build example service
go build -o bin/service ./cmd/ucanlog
```


## Deployment

### As a Library

Integrate ucanlog into your Go application by importing the public packages and creating custom validators as needed.

### As Standalone Service

Use the example in `cmd/ucanlog` for a basic service, or build your own with custom validators.

### Production Considerations

1. **Persistent Keys**: Use fixed private keys for consistent service DID
2. **Data Storage**: Configure persistent storage paths
3. **TLS**: Run behind reverse proxy with TLS
4. **Rate Limiting**: Implement in custom validators
5. **Monitoring**: Add logging and metrics
6. **Backup**: Regular backups of log data

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass
5. Submit a pull request

## Thanks

UCANLOG stands on the shoulders of giants. We are grateful to the following projects and communities:

- **[Tessera](https://github.com/transparency-dev/tessera)** - For building the foundation of cryptographically verifiable transparent logs
- **[UCAN](https://ucan.xyz/)** - For the User Controlled Authorization Network protocol and the amazing local-first community
- **[Storacha](https://storacha.network)** - For providing reliable decentralized storage with UCAN access control

## License

Apache 2.0 License - see [LICENSE](LICENSE) file for details.
