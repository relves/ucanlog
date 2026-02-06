package storage

import (
	"context"
	"time"
)

// StateStore abstracts state storage operations.
// This allows the existing code to work with either JSON files or SQLite.
type StateStore interface {
	// Log metadata - reads from tree_state and index_persistence tables
	GetHead(ctx context.Context, logDID string) (indexCID string, treeSize uint64, err error)

	// CID index
	GetCIDIndex(ctx context.Context, logDID string) (map[string]string, error)
	SetCID(ctx context.Context, logDID, path, cid string) error
	SetCIDs(ctx context.Context, logDID string, mappings map[string]string) error
	DeleteCIDsWithPrefix(ctx context.Context, logDID, prefix string) error

	// Tree state
	GetTreeState(ctx context.Context, logDID string) (size uint64, root []byte, err error)
	SetTreeState(ctx context.Context, logDID string, size uint64, root []byte) error

	// Revocations
	AddRevocation(ctx context.Context, delegationCID string) error
	IsRevoked(ctx context.Context, delegationCID string) (bool, error)
	GetRevocations(ctx context.Context) ([]string, error)

	// Index persistence metadata
	GetIndexPersistence(ctx context.Context, logDID string) (*IndexPersistenceMeta, error)
	SetIndexPersistence(ctx context.Context, logDID string, uploadTime time.Time, uploadedSize uint64, uploadedCID string) error

	// GC progress
	GetGCProgress(ctx context.Context, logDID string) (fromSize uint64, err error)
	SetGCProgress(ctx context.Context, logDID string, fromSize uint64) error
}

// IndexPersistenceMeta holds metadata about index persistence to Storacha.
type IndexPersistenceMeta struct {
	LastUploadTime   time.Time
	LastUploadedSize uint64
	LastUploadedCID  string
}
