// pkg/types/group.go
package types

import (
	"time"
)

// GroupID is a unique identifier for a group.
type GroupID string

// PublicKey represents an ed25519 public key.
type PublicKey []byte

// GroupMetadata contains persistent group configuration.
type GroupMetadata struct {
	ID              GroupID   `json:"id"`
	CreatorAccount  PublicKey `json:"creator_account"` // Deprecated: no longer used in simplified delegation model
	RecoveryKeyHash []byte    `json:"recovery_key_hash"`
	CreatedAt       time.Time `json:"created_at"`
	Frozen          bool      `json:"frozen"`
}

// GroupStatus represents the current state of a group.
type GroupStatus struct {
	Metadata        GroupMetadata `json:"metadata"`
	EntriesLogIndex uint64        `json:"entries_log_index"`
	RevocationIndex uint64        `json:"revocation_index"`
}
