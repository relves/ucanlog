# recoverlog

Restores a log's state in the SQLite database from a known IPFS head CID, enabling the ucanlog service to resume the log without forking.

Use this after a partial or total database wipeout when the head CID is known from monitoring, the Storacha console, or operator notes.

## When to use this

- The ucanlog database was wiped or corrupted and one or more logs fail to start
- ucanlog logs `"cannot resume log: no head CID found in state store"` on startup
- You have the last known head CID for the affected log (from monitoring, Storacha console, or prior operator notes)

Do **not** use this to start a log from scratch — use the normal `CreateLog` API for new logs.

## Prerequisites

- The last known head CID for the main log (required)
- The last known head CID for the revocations log, **if any revocations were ever recorded** (see [Revocations](#revocations))
- Network access to the IPFS gateway (default: `https://w3s.link`)

## Usage

```sh
go run ./cmd/recoverlog \
  --data-path ./data \
  --log-id did:key:z6Mk... \
  --head-cid bafybeifoo
```

With revocations:

```sh
go run ./cmd/recoverlog \
  --data-path ./data \
  --log-id did:key:z6Mk... \
  --head-cid bafybeifoo \
  --revocations-head-cid bafybeebar
```

## Flags

| Flag | Required | Description |
|---|---|---|
| `--data-path` | Yes | Path to the ucanlog data directory (same as `DATA_PATH` in the service) |
| `--log-id` | Yes | DID of the log to recover |
| `--head-cid` | Yes | Last known head CID for the main log |
| `--revocations-head-cid` | No | Last known head CID for the revocations log |

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `IPFS_GATEWAY_URL` | `https://w3s.link` | IPFS gateway to fetch the head CAR from |

## Revocations

Every log has a paired revocations log (`<log-id>-revocations`). If credentials were ever revoked against the log, the revocations log will have its own head CID that must be restored alongside the main log.

**Omitting `--revocations-head-cid` when revocations existed leaves the log exposed to formerly revoked credentials.** Only omit it if you are certain no revocations were ever recorded.

If you are unsure whether revocations were recorded, check the Storacha console or your monitoring for uploads to the `<log-id>-revocations` space.

## Running while ucanlog is live

This tool can be run while the ucanlog service is serving other logs. It only writes to the SQLite database and does not affect logs that are already loaded in memory.

After recovery completes, the restored log will be picked up on its next access — no service restart required, unless the log was already loaded in memory with incorrect state (in that case, restart the service).

## What this tool does

1. Opens the SQLite database at `--data-path`
2. Creates the log record row if it does not exist (handles total wipeout)
3. Fetches the head CAR from the IPFS gateway using the supplied head CID
4. Parses the checkpoint embedded in the CAR to extract the tree size and root hash
5. Writes `tree_state` and `latest_head_car` to the database
6. Repeats steps 2–5 for the revocations log if `--revocations-head-cid` was supplied

After this completes, the normal ucanlog startup path (`ModeResume`) will find valid state and the log will resume from where it left off.
