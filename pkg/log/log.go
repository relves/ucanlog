// Package log provides public interfaces for log operations.
package log

import (
	"context"
	"crypto/ed25519"
)

// Service defines the interface for log operations.
type Service interface {
	CreateLog(ctx context.Context, logIdKey, accountIdKey ed25519.PublicKey) (*LogResult, error)
	Append(ctx context.Context, logID string, data []byte) (uint64, error)
	Read(ctx context.Context, logID string, offset, limit int64) (*ReadResult, error)
	GetRevocations(ctx context.Context, logID string) ([]Revocation, error)
	Revoke(ctx context.Context, logID string, cid string) error
}

// LogResult contains the result of creating a log.
type LogResult struct {
	LogID string `json:"logId"`
}

// ReadResult contains read entries and metadata.
type ReadResult struct {
	Entries [][]byte
	Total   int64
}

// Revocation represents a revocation entry.
type Revocation struct {
	Target string
}
