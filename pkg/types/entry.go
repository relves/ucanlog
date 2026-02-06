// pkg/types/entry.go
package types

import (
	"encoding/json"
	"time"
)

// LogEntry represents an entry in the primary tlog.
type LogEntry struct {
	Index     uint64    `json:"index"`
	Data      []byte    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
	Submitter PublicKey `json:"submitter"`
}

// LogEntryWithProof includes the Merkle inclusion proof.
type LogEntryWithProof struct {
	Entry          LogEntry `json:"entry"`
	InclusionProof []byte   `json:"inclusion_proof"`
	TreeSize       uint64   `json:"tree_size"`
	RootHash       []byte   `json:"root_hash"`
}

// RevocationEntry represents an entry in the revocation tlog.
type RevocationEntry struct {
	Index     uint64         `json:"index"`
	Type      RevocationType `json:"type"`
	Target    []byte         `json:"target"`
	Timestamp time.Time      `json:"timestamp"`
}

// RevocationType defines what is being revoked.
type RevocationType string

const (
	RevokeUCAN       RevocationType = "ucan"       // Revoke specific UCAN by hash
	RevokeAccount    RevocationType = "account"    // Revoke all UCANs for account
	RevokeCapability RevocationType = "capability" // Revoke specific capability
)

// Serialize converts a RevocationEntry to JSON bytes for storage.
func (e *RevocationEntry) Serialize() ([]byte, error) {
	return json.Marshal(e)
}

// Deserialize populates a RevocationEntry from JSON bytes.
func (e *RevocationEntry) Deserialize(data []byte) error {
	return json.Unmarshal(data, e)
}
