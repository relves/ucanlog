# ![UCANLOG](/ucanlog-logo.svg) - Transparency Log with Decentralized Auth & Storage

![ucanlog infographic](/ucanlog-infographic.png)

## Overview

UCANLOG is a Go library for building transparent append-only log services using [Tessera](https://github.com/transparency-dev/tessera) with capability-based access control via [UCANs](https://ucan.xyz/) and user-owned storage via [Storacha](https://storacha.network).

**Status:** Experimental. Use at your own risk.

## Motivation

UCANLOG forms the foundation for "Proof-as-a-Service" — user-owned append-only logs that serve as verifiable evidence for chains of custody, provenance, credential issuance, and key agreement protocols. Each record is a public commitment to public or private data via cryptographic hashing.

It uses the same tamper-proof system as the [Sigstore Certificate Authority](https://docs.sigstore.dev/certificate_authority/overview/), but adds:

- **User-controlled decentralized authorization** — UCAN delegation chains; write access revocable instantly
- **User-controlled decentralized storage** — data lives on the IPFS network; fully portable

See [SEQUENCE DIAGRAM](docs/SEQUENCE_DIAG.md) for the full data flow.

---

## Quick Start

```bash
go get github.com/relves/ucanlog
```

```go
storeManager := sqlite.NewStoreManager("./data")
defer storeManager.CloseAll()

tlogMgr, err := tlog.NewDelegatedManager(tlog.DelegatedManagerConfig{
    BasePath:      "./data",
    ServiceSigner: serviceSigner,
    // ...
})

logService := log.NewLogServiceWithConfig(log.LogServiceConfig{
    TlogManager:  tlogMgr,
    StoreManager: storeManager,
    ServiceDID:   serviceSigner.DID().String(),
})

srv, err := server.NewServer(
    server.WithSigner(serviceSigner),
    server.WithLogService(logService),
    server.WithStoreManager(storeManager),
)
```

---

## API Capabilities (UCAN RPC)

All write operations are invoked over UCAN RPC (`POST /`). Every request is independently authenticated — no server-side sessions.

### tlog/create

Creates a new transparent log. The space DID from the delegation becomes the log identity.

**Caveats:** `delegation` (base64 UCAN)

**Returns:** `{ logId, tree_size: 0 }`

---

### tlog/append

Appends data to an existing log.

**Caveats:**

| Field | Required | Description |
|-------|----------|-------------|
| `data` | Yes | Base64-encoded data to append |
| `delegation` | Yes | Base64-encoded UCAN delegation |

**Returns:**

| Field | Description |
|-------|-------------|
| `index` | Zero-based position of this entry in the log |
| `tree_size` | Total entries after this append |
| `head_cid` | Pre-computed CID of the current head CAR |
| `checkpoint` | Base64-encoded signed checkpoint note |

The receipt is returned synchronously after the entry is sequenced and the CAR is written locally. The Storacha upload happens in the background.

---

### tlog/read

Reads entries from a log.

**Caveats:** `offset` (default 0), `limit` (default 100)

**Returns:** `{ entries: [...], total }`

---

### tlog/revoke

Revokes a UCAN delegation. The delegation to revoke must first be stored in the space.

**Caveats:** `cid` (CID of stored delegation), `delegation` (storage delegation)

**Workflow:**
1. Client stores the delegation blob in their space
2. Client sends revocation request with the CID
3. Service fetches, validates authority, and adds to the revocation log

Only the delegation issuer or upstream authorities can revoke.

---

## HTTP Read Endpoints

These endpoints are unauthenticated and read-only.

### GET /logs/{logID}/head

Returns current log state.

```json
{ "tree_size": 42, "head_cid": "bafybeig..." }
```

### tlog-tiles API

Compatible with the [tlog-tiles spec](https://github.com/C2SP/C2SP/blob/main/tlog-tiles.md). Proxies tile data from IPFS.

| Endpoint | Description |
|----------|-------------|
| `GET /logs/{logID}/checkpoint` | Latest signed checkpoint (`text/plain`) |
| `GET /logs/{logID}/tile/{level}/{path...}` | Merkle tree tile (`application/octet-stream`) |
| `GET /logs/{logID}/tile/entries/{path...}` | Entry bundle (`application/octet-stream`) |

---

## Configuration

| Variable | Description | Default |
|----------|-------------|---------|
| `DATA_PATH` | Directory for log storage | `./data` |
| `IPFS_GATEWAY_URL` | IPFS gateway for tile proxying | `https://w3s.link` |
| `LOG_LEVEL` | `debug` / `info` / `warn` / `error` | `info` |
| `PORT` | HTTP server port | `8080` |
| `UCANLOG_PRIVATE_KEY` | Base64 Ed25519 private key | Generated |
| `STORACHA_REPLICA_COUNT` | Blob replicas (0 = skip replication) | `2` |

---

## Delegation Model

The customer's Storacha **space DID** is the **log identity**. Every delegation must target a space DID and include:

- `space/blob/add` — upload blob data
- `space/blob/replicate` — replicate blobs across storage nodes (set `STORACHA_REPLICA_COUNT=0` in dev to skip)
- `space/index/add` — register IPNI index CARs
- `upload/add` — register uploads in the space

See [UCAN_DELEGATION_SPEC](docs/UCAN_DELEGATION_SPEC.md) for full details.

---

## Security Model

- **Stateless authentication** — every request validated independently via UCAN delegation chain
- **Delegation theft prevention** — invocation must be signed by the same principal who created the delegation
- **Revocation authority** — issuers and upstream authorities can revoke; recipients cannot
- **Pluggable validation** — implement `server.RequestValidator` for additional checks

---

## Development

```bash
# Test
go test ./...

# Build
go build -o bin/ucanlog ./cmd/ucanlog
```

### Production Checklist

- Use a fixed `UCANLOG_PRIVATE_KEY` for a consistent service DID across restarts
- Run behind a reverse proxy with TLS
- Back up the SQLite data directory regularly
- Set `STORACHA_REPLICA_COUNT=0` for dev/test accounts that don't support replication

---

## Thanks

- **[Tessera](https://github.com/transparency-dev/tessera)** — cryptographically verifiable transparent logs
- **[UCAN](https://ucan.xyz/)** — User Controlled Authorization Network
- **[Storacha](https://storacha.network)** — decentralized storage with UCAN access control

## License

Apache 2.0 — see [LICENSE](LICENSE)
