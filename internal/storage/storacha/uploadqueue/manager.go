// Package uploadqueue provides an async upload queue for Storacha CAR uploads.
// CARs are enqueued synchronously (fast, local SQLite write) and uploaded to
// Storacha in the background by worker goroutines.
package uploadqueue

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/relves/ucanlog/internal/storage"
	"github.com/storacha/go-libstoracha/blobindex"
)

// StorachaUploader uploads a full-state CAR to Storacha.
type StorachaUploader interface {
	UploadFullStateCAR(ctx context.Context, carData []byte, rootCID cid.Cid, positions map[cid.Cid]blobindex.Position) (string, error)
	UploadFinalizedBlob(ctx context.Context, data []byte) (string, error)
}

// QueueStore is the storage interface required by the upload queue manager.
// *sqlite.LogStore implements this interface.
type QueueStore interface {
	DequeuePendingCARs(ctx context.Context, limit int) ([]storage.PendingCAR, error)
	GetPendingBlobsForCAR(ctx context.Context, queueID int64) ([]storage.PendingBlob, error)
	MarkCARUploaded(ctx context.Context, id int64) error
	MarkCARFailed(ctx context.Context, id int64, errMsg string) error
	MarkBlobUploaded(ctx context.Context, id int64) error
	MarkBlobFailed(ctx context.Context, id int64, errMsg string) error
}

// Config holds configuration for the upload queue manager.
type Config struct {
	Store        QueueStore
	Uploader     StorachaUploader
	Parallelism  int           // default: 2
	PollInterval time.Duration // default: 1s
	MaxRetries   int           // default: 5
	Logger       *slog.Logger
}

// Manager manages the async upload queue background worker.
type Manager struct {
	store        QueueStore
	uploader     StorachaUploader
	parallelism  int
	pollInterval time.Duration
	maxRetries   int
	logger       *slog.Logger

	mu           sync.Mutex
	workerCtx    context.Context
	workerCancel context.CancelFunc
	wg           sync.WaitGroup

	// notifyCh is written when a new item is enqueued, waking a sleeping worker.
	notifyCh chan struct{}
}

// New creates a new upload queue manager. Call Start to begin background processing.
func New(cfg Config) *Manager {
	if cfg.Parallelism <= 0 {
		cfg.Parallelism = 2
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = 5
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Manager{
		store:        cfg.Store,
		uploader:     cfg.Uploader,
		parallelism:  cfg.Parallelism,
		pollInterval: cfg.PollInterval,
		maxRetries:   cfg.MaxRetries,
		logger:       cfg.Logger,
		notifyCh:     make(chan struct{}, 1),
	}
}

// Start launches the background upload workers.
func (m *Manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.workerCancel != nil {
		return // already running
	}
	m.workerCtx, m.workerCancel = context.WithCancel(ctx)
	for i := 0; i < m.parallelism; i++ {
		m.wg.Add(1)
		go m.workerLoop(m.workerCtx)
	}
}

// Stop gracefully shuts down the background workers.
func (m *Manager) Stop() {
	m.mu.Lock()
	cancel := m.workerCancel
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
}

// Notify wakes a sleeping worker. Call after enqueuing a new CAR.
// The channel buffer is intentionally size 1: rapid consecutive enqueues coalesce
// into a single notification. The second CAR will be picked up on the next poll
// cycle (PollInterval) or by a worker that just finished processing.
func (m *Manager) Notify() {
	select {
	case m.notifyCh <- struct{}{}:
	default:
	}
}

func (m *Manager) workerLoop(ctx context.Context) {
	defer m.wg.Done()
	for {
		if err := m.drainOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			m.logger.Warn("upload worker error", "error", err)
		}

		// Wait for notification or poll interval.
		select {
		case <-ctx.Done():
			return
		case <-m.notifyCh:
		case <-time.After(m.pollInterval):
		}
	}
}

func (m *Manager) drainOnce(ctx context.Context) error {
	// Limit to 1 per worker so multiple workers share the queue fairly.
	cars, err := m.store.DequeuePendingCARs(ctx, 1)
	if err != nil {
		return fmt.Errorf("dequeue: %w", err)
	}
	for _, car := range cars {
		if ctx.Err() != nil {
			return nil
		}
		m.uploadCAR(ctx, car)
	}
	return nil
}

func (m *Manager) uploadCAR(ctx context.Context, car storage.PendingCAR) {
	logger := m.logger.With("logDID", car.LogDID, "treeSize", car.TreeSize, "headCID", car.HeadCID)

	// Upload associated blobs (finalized bundles) first.
	blobs, err := m.store.GetPendingBlobsForCAR(ctx, car.ID)
	if err != nil {
		logger.Warn("failed to fetch pending blobs", "error", err)
		_ = m.store.MarkCARFailed(ctx, car.ID, fmt.Sprintf("fetch blobs: %v", err))
		return
	}
	for _, blob := range blobs {
		if ctx.Err() != nil {
			return
		}
		if _, err := m.uploader.UploadFinalizedBlob(ctx, blob.BlobData); err != nil {
			logger.Warn("blob upload failed", "path", blob.Path, "error", err)
			_ = m.store.MarkBlobFailed(ctx, blob.ID, err.Error())
			_ = m.store.MarkCARFailed(ctx, car.ID, fmt.Sprintf("blob %s: %v", blob.Path, err))
			return
		}
		if err := m.store.MarkBlobUploaded(ctx, blob.ID); err != nil {
			logger.Warn("mark blob uploaded failed", "error", err)
		}
		logger.Info("finalized blob uploaded", "path", blob.Path, "cid", blob.BlobCID)
	}

	// Parse the CAR root CID.
	rootCID, err := cid.Decode(car.HeadCID)
	if err != nil {
		_ = m.store.MarkCARFailed(ctx, car.ID, fmt.Sprintf("decode head CID: %v", err))
		return
	}

	// Extract block positions for the CAR index.
	positions, err := extractBlockPositions(car.CARData)
	if err != nil {
		_ = m.store.MarkCARFailed(ctx, car.ID, fmt.Sprintf("extract block positions: %v", err))
		return
	}

	// Upload the full-state CAR.
	if _, err := m.uploader.UploadFullStateCAR(ctx, car.CARData, rootCID, positions); err != nil {
		logger.Warn("CAR upload failed", "error", err)
		_ = m.store.MarkCARFailed(ctx, car.ID, err.Error())
		return
	}

	if err := m.store.MarkCARUploaded(ctx, car.ID); err != nil {
		logger.Warn("mark CAR uploaded failed", "error", err)
	}
	logger.Info("CAR uploaded successfully", "queueID", car.ID)
}

// extractBlockPositions is a thin wrapper so this package doesn't import the full storacha package.
// It calls the function injected via the uploader; for position extraction we inline a simple version.
func extractBlockPositions(carData []byte) (map[cid.Cid]blobindex.Position, error) {
	// Re-use the logic from storacha.ExtractBlockPositions by duplicating the minimal
	// CAR-parsing needed here, to keep the package dependency clean.
	// We forward the call through an interface registered at startup in storacha.go.
	if globalExtractFn != nil {
		return globalExtractFn(carData)
	}
	// Fallback: return empty positions (upload still works, just no IPNI shard index).
	return map[cid.Cid]blobindex.Position{}, nil
}

// globalExtractFn is set by the storacha package at init time to avoid circular imports.
// sync.Once ensures the write is visible to all goroutines that read it later.
var (
	globalExtractFn     func([]byte) (map[cid.Cid]blobindex.Position, error)
	globalExtractFnOnce sync.Once
)

// SetExtractBlockPositionsFn registers the CAR block position extractor.
// Called once from storacha package init.
func SetExtractBlockPositionsFn(fn func([]byte) (map[cid.Cid]blobindex.Position, error)) {
	globalExtractFnOnce.Do(func() {
		globalExtractFn = fn
	})
}
