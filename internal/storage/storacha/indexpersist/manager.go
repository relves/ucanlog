// storage/storacha/indexpersist/manager.go
package indexpersist

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/relves/ucanlog/internal/storage"
)

// Uploader handles uploading CAR data to storage.
type Uploader interface {
	UploadCAR(ctx context.Context, data []byte) (rootCID string, err error)
}

// IndexProvider provides access to the current index state.
type IndexProvider interface {
	GetIndex() map[string]string
}

// Manager handles periodic index CAR building and uploading.
type Manager struct {
	cfg           Config
	uploader      Uploader
	indexProvider IndexProvider
	stateStore    storage.StateStore
	logDID        string
	logger        *slog.Logger

	mu                sync.Mutex
	dirty             bool
	lastHash          string
	meta              IndexMeta
	persistInProgress bool
	pendingCtx        context.Context
}

// NewManager creates a new index persistence manager.
func NewManager(cfg Config, uploader Uploader, indexProvider IndexProvider) *Manager {
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	if cfg.MinInterval == 0 {
		cfg.MinInterval = 10 * time.Second
	}
	if cfg.PathPrefix == "" {
		cfg.PathPrefix = "index/"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Manager{
		cfg:           cfg,
		uploader:      uploader,
		indexProvider: indexProvider,
		dirty:         true, // Start dirty to trigger initial upload
		logger:        cfg.Logger,
	}
}

// SetStateStore configures the manager to use StateStore for metadata persistence.
func (m *Manager) SetStateStore(store storage.StateStore, logDID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stateStore = store
	m.logDID = logDID
	m.loadMeta()
}

// Start begins the periodic upload loop. Blocks until ctx is cancelled.
func (m *Manager) Start(ctx context.Context) {
	ticker := time.NewTicker(m.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.maybeUpload(ctx); err != nil {
				m.logger.Error("index persistence error", "error", err)
			}
		}
	}
}

// MarkDirty signals that the index has changed.
func (m *Manager) MarkDirty() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dirty = true
}

// GetMeta returns the current index metadata.
func (m *Manager) GetMeta() IndexMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.meta
}

// GetLatestCID returns the root CID of the most recently uploaded index.
func (m *Manager) GetLatestCID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.meta.RootCID
}

// maybeUpload checks if upload is needed and performs it.
func (m *Manager) maybeUpload(ctx context.Context) error {
	m.mu.Lock()
	if !m.dirty {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	// Get current index
	index := m.indexProvider.GetIndex()
	//log.Printf("DEBUG: Index persistence checking - got %d entries", len(index))
	// for k := range index {
	// 	log.Printf("DEBUG: Index entry: %s", k)
	// }

	// Skip upload if index is empty
	if len(index) == 0 {
		m.logger.Debug("index empty, skipping upload")
		m.mu.Lock()
		m.dirty = false
		m.mu.Unlock()
		return nil
	}

	// Compute a simple hash to detect changes
	hash := computeIndexHash(index)

	m.mu.Lock()
	if hash == m.lastHash {
		m.dirty = false
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	// Build CAR
	carData, rootCID, err := BuildIndexCAR(ctx, index)
	if err != nil {
		return fmt.Errorf("failed to build CAR: %w", err)
	}

	// Upload
	uploadedCID, err := m.uploader.UploadCAR(ctx, carData)
	if err != nil {
		return fmt.Errorf("failed to upload CAR: %w", err)
	}

	// Update state
	m.mu.Lock()
	m.meta.Version++
	m.meta.RootCID = uploadedCID
	m.meta.LastUploaded = time.Now()
	m.meta.EntryCount = len(index)
	m.lastHash = hash
	m.dirty = false
	meta := m.meta
	m.mu.Unlock()

	// Persist metadata
	if err := m.saveMeta(); err != nil {
		m.logger.Warn("failed to save index meta", "error", err)
	}

	// Log
	m.logger.Info("index CAR uploaded", "cid", rootCID, "version", meta.Version, "entries", meta.EntryCount)

	// Callback
	if m.cfg.OnUpload != nil {
		m.cfg.OnUpload(uploadedCID, meta)
	}

	return nil
}

// ForceUpload triggers an immediate upload regardless of dirty state.
func (m *Manager) ForceUpload(ctx context.Context) error {
	m.MarkDirty()
	return m.maybeUpload(ctx)
}

// TriggerPersistAsync starts index persistence in the background if needed.
// The context should be a background context with delegation attached (not a request context).
// Callers should use storacha.TriggerIndexPersistence which handles the context transformation.
// Returns immediately; persistence runs asynchronously.
//
// Concurrency behavior:
// - Only one persist runs at a time (persistInProgress guard)
// - Multiple concurrent writers queue their contexts; last one wins
// - After persist completes, if index changed, follow-up uses queued context
// - This ensures all successful writes eventually get persisted
func (m *Manager) TriggerPersistAsync(ctx context.Context) {
	m.mu.Lock()

	// Only update pendingCtx if we don't already have one stored.
	// This preserves a valid delegation context when rate-limited,
	// so it can be used when MinInterval passes and persist starts.
	if m.pendingCtx == nil {
		m.pendingCtx = ctx
	}

	if !m.dirty || m.persistInProgress {
		m.mu.Unlock()
		return
	}

	// Rate limit: skip if recent persist (MinInterval of 0 disables rate limiting)
	if m.cfg.MinInterval > 0 && time.Since(m.meta.LastUploaded) < m.cfg.MinInterval {
		m.mu.Unlock()
		return
	}

	m.persistInProgress = true
	// Take the pending ctx (which has a valid delegation)
	persistCtx := m.pendingCtx
	m.pendingCtx = nil
	m.mu.Unlock()

	go m.runPersist(persistCtx)
}

// runPersist executes the persist and handles follow-up if index changed.
func (m *Manager) runPersist(ctx context.Context) {
	// Capture index state before upload
	index := m.indexProvider.GetIndex()
	capturedHash := computeIndexHash(index)

	// Skip if unchanged from last successful upload
	m.mu.Lock()
	if capturedHash == m.lastHash {
		m.dirty = false
		m.persistInProgress = false
		m.mu.Unlock()
		return
	}
	m.mu.Unlock()

	// Build and upload
	err := m.doUpload(ctx, index, capturedHash)

	m.mu.Lock()
	m.persistInProgress = false

	if err != nil {
		m.logger.Error("async index persistence error", "error", err)
		// Stay dirty; pendingCtx may have fresh delegation for retry
		m.mu.Unlock()
		return
	}

	// Only mark clean if index unchanged since capture
	currentHash := computeIndexHash(m.indexProvider.GetIndex())
	if capturedHash == currentHash {
		m.dirty = false
		m.pendingCtx = nil
		m.mu.Unlock()
		return
	}

	// Index changed while uploading - need follow-up persist
	if m.pendingCtx != nil {
		pendingCtx := m.pendingCtx
		m.pendingCtx = nil
		m.persistInProgress = true // Set back to true for follow-up
		m.mu.Unlock()
		// Direct recursion bypasses rate limiting (this is part of the same logical operation)
		go m.runPersist(pendingCtx)
		return
	}
	m.mu.Unlock()
}

// doUpload handles the actual CAR build and upload.
func (m *Manager) doUpload(ctx context.Context, index map[string]string, hash string) error {
	carData, rootCID, err := BuildIndexCAR(ctx, index)
	if err != nil {
		return fmt.Errorf("failed to build CAR: %w", err)
	}

	uploadedCID, err := m.uploader.UploadCAR(ctx, carData)
	if err != nil {
		return fmt.Errorf("failed to upload CAR: %w", err)
	}

	// Update metadata
	m.mu.Lock()
	m.meta.Version++
	m.meta.RootCID = uploadedCID
	m.meta.LastUploaded = time.Now()
	m.meta.EntryCount = len(index)
	m.lastHash = hash
	meta := m.meta
	m.mu.Unlock()

	// Persist metadata to StateStore
	if err := m.saveMeta(); err != nil {
		m.logger.Warn("failed to save index meta", "error", err)
	}

	m.logger.Info("index CAR uploaded", "cid", rootCID, "version", meta.Version, "entries", meta.EntryCount)

	// Callback
	if m.cfg.OnUpload != nil {
		m.cfg.OnUpload(uploadedCID, meta)
	}

	return nil
}

func (m *Manager) loadMeta() {
	if m.stateStore == nil || m.logDID == "" {
		return
	}

	ctx := context.Background()
	meta, err := m.stateStore.GetIndexPersistence(ctx, m.logDID)
	if err != nil {
		m.logger.Warn("failed to load index persistence meta", "error", err)
		return
	}
	if meta == nil {
		return // No metadata yet
	}

	m.meta.LastUploaded = meta.LastUploadTime
	m.meta.RootCID = meta.LastUploadedCID
	// EntryCount and Version are not persisted in StateStore, keep defaults
}

func (m *Manager) saveMeta() error {
	if m.stateStore == nil || m.logDID == "" {
		return nil
	}

	m.mu.Lock()
	meta := m.meta
	m.mu.Unlock()

	ctx := context.Background()
	if err := m.stateStore.SetIndexPersistence(ctx, m.logDID, meta.LastUploaded, uint64(meta.EntryCount), meta.RootCID); err != nil {
		return fmt.Errorf("failed to save index persistence meta: %w", err)
	}

	return nil
}

// computeIndexHash creates a simple hash of the index for change detection.
func computeIndexHash(index map[string]string) string {
	// Simple approach: serialize to JSON and hash
	// Could use a more efficient hash, but this is fine for our use case
	data, _ := json.Marshal(index)
	return fmt.Sprintf("%x", len(data)) + string(data[:min(64, len(data))])
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
