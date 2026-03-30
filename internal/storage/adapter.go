package storage

import (
	"context"
)

// StateStore abstracts state storage operations.
// This allows the existing code to work with either JSON files or SQLite.
type StateStore interface {
	// Log metadata — reads from latest_head_car.
	GetHead(ctx context.Context, logDID string) (indexCID string, treeSize uint64, err error)

	// Tree state
	GetTreeState(ctx context.Context, logDID string) (size uint64, root []byte, err error)
	SetTreeState(ctx context.Context, logDID string, size uint64, root []byte) error

	// Revocations
	AddRevocation(ctx context.Context, delegationCID string) error
	IsRevoked(ctx context.Context, delegationCID string) (bool, error)
	GetRevocations(ctx context.Context) ([]string, error)

	// Latest head CAR — updated synchronously on every append.
	// Enables cold-start restore without a gateway round-trip.
	SetLatestHeadCAR(ctx context.Context, logDID string, treeSize uint64, headCID string, carData []byte) error
	GetLatestHeadCAR(ctx context.Context, logDID string) (headCID string, treeSize uint64, carData []byte, err error)
}

// PendingCAR is a CAR awaiting upload in the upload queue.
type PendingCAR struct {
	ID       int64
	LogDID   string
	TreeSize uint64
	HeadCID  string
	CARData  []byte
}

// PendingBlob is a finalized bundle blob awaiting upload.
type PendingBlob struct {
	ID       int64
	QueueID  int64
	Path     string
	BlobCID  string
	BlobData []byte
}

// QueueStore manages the async upload queue.
// LogStore implements this interface.
type QueueStore interface {
	// EnqueueAndUpdateHead atomically writes to latest_head_car and inserts an
	// upload_queue row (plus any blob rows). Returns the new queue entry ID.
	EnqueueAndUpdateHead(ctx context.Context, logDID string, treeSize uint64, headCID string, carData []byte, blobs []PendingBlob) (int64, error)

	DequeuePendingCARs(ctx context.Context, limit int) ([]PendingCAR, error)
	GetPendingBlobsForCAR(ctx context.Context, queueID int64) ([]PendingBlob, error)
	MarkCARUploaded(ctx context.Context, id int64) error
	MarkCARFailed(ctx context.Context, id int64, errMsg string) error
	MarkBlobUploaded(ctx context.Context, id int64) error
	MarkBlobFailed(ctx context.Context, id int64, errMsg string) error
}
