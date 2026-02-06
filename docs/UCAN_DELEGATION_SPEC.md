# UCAN Delegation Specification for Customer Storage

## Overview

This document specifies the UCAN delegation requirements for customers to grant the ucanlog service write access to their Storacha spaces.

## Key Concepts

### Space DID as Log Identity

The customer's **space DID** serves as the **log identity**. When you create a log, the space DID extracted from your delegation becomes the unique identifier for that log. This means:

- One log per Storacha space
- The space DID is returned as the `logId` in create responses
- All subsequent operations (append, read, revoke) reference this space DID

### Stateless Authentication

Every request must include a valid delegation. The service does not maintain session state:

- **Create**: Requires delegation to establish the log
- **Append**: Requires delegation on every append request
- **Revoke**: Requires both the delegation to revoke and a storage delegation

### Invocation Authorization

Every API request must be signed by the principal who created the delegation being used.

- **Rule**: Invocation issuer MUST equal delegation issuer.
- **Why**: This prevents "delegation theft" where an attacker finds a public delegation (e.g., on IPFS) and attempts to use it.
- **Sub-delegation**: To delegate to another user, create a chain: SpaceOwner → FriendB → Service. FriendB then signs the invocation.

## Delegation Structure

```
Customer (space owner)
    DID: did:key:z6Mk...  (customer's space DID - also the log identity)
    │
    └── Delegates to: ucanlog service DID
            │
            ├── Capability: space/blob/add
            │   └── Resource: did:key:z6Mk... (customer's space)
            │
            ├── Capability: space/index/add  
            │   └── Resource: did:key:z6Mk... (customer's space)
            │
            └── Capability: upload/add
                └── Resource: did:key:z6Mk... (customer's space)
```

## Required Capabilities

The customer must delegate the following capabilities to the service:

### 1. `space/blob/add`
Allows the service to upload blob data to the customer's Storacha space.

### 2. `space/index/add`
Allows the service to register index CARs that make content browsable.

### 3. `upload/add`
Allows the service to register uploads (associates root CIDs with shard CIDs).

## Delegation Constraints

### Audience
The delegation's audience MUST be the ucanlog service DID. The service DID can be obtained from the service's `/did` endpoint.

### Resource
The resource (`with` field) MUST be the customer's Storacha space DID. All capabilities in a delegation must target the same space DID.

### Expiry (Optional)
Delegations MAY include an expiry timestamp. The service will reject expired delegations. Customers can refresh delegations by including a new delegation in subsequent requests.

### Proof Chain
The delegation MUST form a valid proof chain from the customer's space key to the service.

## API Usage

### Creating a Log

The create endpoint requires only a `delegation` field. The space DID is automatically extracted from the delegation:

```json
{
  "delegation": "<base64-encoded UCAN delegation>"
}
```

Response:
```json
{
  "logId": "did:key:z6Mk..."  // The space DID is returned as log ID
}
```

### Appending to a Log

Every append request requires a delegation:

```json
{
  "data": "<base64-encoded data>",
  "delegation": "<base64-encoded UCAN delegation>"
}
```

The service extracts the space DID from the delegation and uses it to identify which log to append to.

### Revoking a Delegation

To revoke a delegation:

1. **Store the delegation**: First, the delegation to be revoked must be uploaded to the space as a blob. This makes it discoverable and auditable.

2. **Send revocation request**: Provide the CID of the stored delegation and a storage delegation for access.

```json
{
  "cid": "<CID of the delegation to revoke>",
  "delegation": "<base64-encoded storage delegation>"
}
```

The service will:
1. Fetch the delegation from storage using the CID
2. Parse and validate the delegation
3. Verify the caller has authority to revoke it
4. Add the CID to the revocation log

**Why store delegations?**: This approach has several benefits:
- **Smaller request payloads**: CIDs are much smaller than encoded delegations
- **Auditability**: All delegations are discoverable in the space
- **Self-contained spaces**: Authorization data lives alongside log data
- **No server-side delegation storage**: The service doesn't need to track delegations

**Revocation Authority**: Only the following principals can revoke a delegation:
- The issuer of the delegation
- Any upstream authority in the proof chain (e.g., the space owner can revoke any downstream delegation)

### Garbage Collection

The service implements garbage collection to remove obsolete partial bundle blobs from storage. For security reasons, GC requires explicit authorization through a dedicated endpoint.

#### Why Separate GC Authorization?

Regular log operations (create/append) use delegations with write-only permissions (`space/blob/add`, `space/index/add`, `upload/add`). These delegations should **NOT** include delete permissions because:

- **Accidental data loss**: A compromised delegation could accidentally delete log data
- **Malicious deletion**: Attackers could intentionally destroy the log
- **Least privilege**: Regular operations don't need delete permissions during normal use

#### Running Garbage Collection

To run GC on your log:

**1. Create a GC-specific delegation** with `space/blob/remove` capability:

```bash
# Short-lived delegation for GC (e.g., 1 hour expiry)
storacha delegation create $SERVICE_DID \
  --can 'space/blob/remove' \
  --expiration $(date -u -d '+1 hour' +%s) \
  --base64 > gc-delegation.txt
```

**2. Call the garbage collection endpoint**:

Using the Storacha client (programmatically):

```javascript
import * as Client from '@storacha/client'

// Invoke the tlog/gc capability
const result = await client.capability.invoke({
  issuer: spaceSigner,  // Space owner signs the invocation
  audience: serviceDID,
  capability: {
    can: 'tlog/gc',
    with: serviceDID,
    nb: {
      logId: spaceDID,
      delegation: gcDelegationBase64
    }
  },
  proofs: [] // No proofs needed - direct invocation
})
```

Response:
```json
{
  "bundlesProcessed": 42,
  "blobsRemoved": 126,
  "bytesFreed": 15728640,
  "newGCPosition": 4200
}
```

#### GC Delegation Requirements

The GC delegation MUST satisfy these strict requirements:

1. **Direct delegation**: Must be issued directly by the space owner (space DID) to the service, with no intermediaries
2. **Remove capability**: Must include `space/blob/remove` for the space DID resource
3. **Correct audience**: Must be addressed to the ucanlog service DID
4. **Valid signature**: Must be signed by the space owner
5. **Not expired**: Must not be past its expiration time (if set)
6. **No proof chain**: Must NOT have any proofs attached (prevents delegation chains like space → friend → service)

**Example of valid GC delegation structure:**

```
Space Owner (did:key:z6Mk...)
    │
    └── Direct delegation to: ucanlog service DID
            │
            └── Capability: space/blob/remove
                └── Resource: did:key:z6Mk... (space owner's DID)
```

**Invalid GC delegation examples:**

```
❌ Space Owner → Agent → Service (has proof chain - rejected)
❌ Agent → Service (wrong issuer - rejected)
❌ Space Owner → Service with space/blob/add only (missing remove capability - rejected)
```

**Recommended**: Use short-lived delegations (e.g., 1 hour expiration) for GC operations to minimize security exposure.

#### GC Frequency

Run GC periodically based on your storage needs:
- **High-volume logs**: Daily or weekly
- **Low-volume logs**: Monthly or as-needed
- **Storage-sensitive**: Monitor space usage and run when needed

The service tracks GC progress per log, so each GC run continues from where the previous run left off. Partial bundles are only cleaned up once the corresponding full bundle is written.

#### What Gets Deleted?

Garbage collection removes:
- **Partial entry bundles** (`.p/` directories for completed bundles)
- **Partial tile files** (level 0 and parent tiles for completed bundles)

GC does NOT delete:
- Final/complete bundles
- Active partial bundles (for tree positions not yet finalized)
- Index files or checkpoints

## Creating a Delegation

### Using the Storacha CLI

```bash
# 1. Get the service DID (contact service operator or check /did endpoint)
SERVICE_DID="did:key:z6Mk..."

# 2. Create delegation with required capabilities
storacha delegation create $SERVICE_DID \
  --can 'space/blob/add' \
  --can 'space/index/add' \
  --can 'upload/add' \
  --base64 > delegation.txt
```

### Programmatically (Go)

```go
import (
    "github.com/storacha/go-ucanto/core/delegation"
    "github.com/storacha/go-ucanto/ucan"
)

// Create delegation granting access to your space
dlg, err := delegation.Delegate(
    customerSigner,  // Your space key
    serviceDID,      // Service's DID
    []ucan.Capability[ucan.NoCaveats]{
        ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
        ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
        ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
    },
)

// Encode as base64 for API
encoded, err := delegation.Format(dlg)
```

## Validation

The service validates customer delegations with the following checks:

1. **Audience Check**: Delegation audience must match service DID
2. **Capability Check**: All required capabilities must be present
3. **Resource Check**: All capability resources must match (same space DID)
4. **Expiry Check**: Delegation must not be expired
5. **Signature Check**: Delegation signature must be valid

## Error Codes

| Code | Description |
|------|-------------|
| `DELEGATION_EXPIRED` | The delegation has expired |
| `DELEGATION_WRONG_AUDIENCE` | Delegation audience doesn't match service DID |
| `DELEGATION_MISSING_CAPABILITY` | Required capability not found in delegation |
| `DELEGATION_WRONG_RESOURCE` | Capability resource doesn't match space DID |
| `DELEGATION_INVALID_SIGNATURE` | Delegation signature verification failed |
| `DELEGATION_PARSE_ERROR` | Failed to parse delegation data |
| `DELEGATION_NOT_FOUND` | Delegation not found in storage (for revocation) |
| `DELEGATION_FETCH_ERROR` | Failed to fetch delegation from storage |
| `MISMATCHED_RESOURCES` | Capabilities in delegation target different resources |
| `REVOCATION_NOT_AUTHORIZED` | Principal is not authorized to revoke the delegation |
| `INVOCATION_NOT_AUTHORIZED` | Invocation issuer does not match delegation issuer |
| `DELEGATION_NO_AUTHORITY` | Delegation issuer has no authority over the space (no valid proof chain) |
| `GC_DELEGATION_NOT_DIRECT` | GC delegation must be directly from space owner, no proof chain allowed |
| `GC_FAILED` | Garbage collection operation failed |

## Security Considerations

1. **Scope Limitation**: Delegations grant only the capabilities needed for log operations. The service cannot access other data in the customer's space.

2. **Revocation Authority**: Only the delegation issuer or upstream authorities can revoke. Recipients of a delegation cannot revoke it.

3. **Stateless Design**: Every request is validated independently. There's no server-side session state that could be compromised.

4. **Key Security**: Customers should protect their space keys. The delegation proves that the space owner authorized the service.

5. **Minimal Privilege**: The service stores only log data (tiles, bundles, checkpoints, index CARs) in the customer's space.

6. **Invocation Authorization**: The invocation issuer must match the delegation issuer. This prevents "delegation theft" where an attacker finds a public delegation and attempts to use it.

7. **Proof Chain Validation**: The delegation's proof chain must trace back to the space owner. This prevents attackers from creating fake delegations claiming access to spaces they don't own.

8. **Garbage Collection Isolation**: GC operations require separate, direct delegations with `space/blob/remove` capability. This prevents:
   - **Accidental deletion**: Regular delegations cannot delete data
   - **Privilege escalation**: Attackers with write-only delegations cannot perform destructive operations
   - **Unauthorized cleanup**: Only the space owner can authorize GC through direct delegation
   - **Delegation chain attacks**: GC delegations must be direct (no proof chains), preventing intermediaries from authorizing deletions

## Delegation Chains

For scenarios where authority is delegated through multiple parties:

```
SpaceOwner -> UserB -> UserC -> Service
```

- SpaceOwner can revoke any delegation in the chain
- UserB can revoke UserC's delegation or any downstream
- UserC cannot revoke delegations issued by SpaceOwner or UserB
- Service (the recipient) cannot revoke delegations just because they received them

This ensures that authority flows downstream, and revocation authority flows upstream.
