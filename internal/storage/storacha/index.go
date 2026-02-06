package storacha

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/relves/ucanlog/internal/storage"
)

// CIDIndex maps Tessera storage paths to Storacha CIDs.
// Thread-safe for concurrent access.
//
// Paths follow Tessera's layout:
//   - "checkpoint" - latest checkpoint
//   - "tile/entries/NNN/NNN/..." - entry bundles
//   - "tile/L/NNN/NNN/..." - merkle tree tiles at level L
type CIDIndex struct {
	Paths      map[string]string `json:"paths"`
	mu         sync.RWMutex
	stateStore storage.StateStore
	logDID     string
	logger     *slog.Logger
}

// NewCIDIndex creates an empty CID index.
func NewCIDIndex() *CIDIndex {
	return &CIDIndex{
		Paths:  make(map[string]string),
		logger: slog.Default(),
	}
}

// NewCIDIndexFromMap creates a CID index from an existing map.
func NewCIDIndexFromMap(data map[string]string) *CIDIndex {
	if data == nil {
		data = make(map[string]string)
	}
	return &CIDIndex{
		Paths:  data,
		logger: slog.Default(),
	}
}

// SetStateStore configures the index to sync writes to StateStore.
func (idx *CIDIndex) SetStateStore(store storage.StateStore, logDID string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.stateStore = store
	idx.logDID = logDID
}

// SetLogger configures the logger for structured logging.
func (idx *CIDIndex) SetLogger(logger *slog.Logger) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if logger == nil {
		logger = slog.Default()
	}
	idx.logger = logger
}

// Set stores a CID for a path and syncs to StateStore if configured.
// Returns an error if the StateStore sync fails.
func (idx *CIDIndex) Set(path, cid string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.Paths[path] = cid

	// Sync to state store if configured
	if idx.stateStore != nil && idx.logDID != "" {
		if err := idx.stateStore.SetCID(context.Background(), idx.logDID, path, cid); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves the CID for a path.
func (idx *CIDIndex) Get(path string) (string, bool) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	cid, ok := idx.Paths[path]
	return cid, ok
}

// Delete removes a path from the index.
func (idx *CIDIndex) Delete(path string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	delete(idx.Paths, path)
}

// DeletePrefix removes all paths with the given prefix.
// Returns the number of entries deleted.
func (idx *CIDIndex) DeletePrefix(prefix string) int {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	count := 0
	for path := range idx.Paths {
		if len(path) >= len(prefix) && path[:len(prefix)] == prefix {
			delete(idx.Paths, path)
			count++
		}
	}

	// Sync to state store if configured
	if idx.stateStore != nil && idx.logDID != "" && count > 0 {
		ctx := context.Background()
		if err := idx.stateStore.DeleteCIDsWithPrefix(ctx, idx.logDID, prefix); err != nil {
			// Log but don't fail - in-memory state is already updated
			if idx.logger == nil {
				idx.logger = slog.Default()
			}
			idx.logger.Warn("failed to sync CID deletes to state store", "error", err)
		}
	}

	return count
}

// MarshalJSON serializes the index to JSON.
func (idx *CIDIndex) MarshalJSON() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return json.Marshal(struct {
		Paths map[string]string `json:"paths"`
	}{
		Paths: idx.Paths,
	})
}

// UnmarshalJSON deserializes the index from JSON.
func (idx *CIDIndex) UnmarshalJSON(data []byte) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	var d struct {
		Paths map[string]string `json:"paths"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}

	idx.Paths = d.Paths
	if idx.Paths == nil {
		idx.Paths = make(map[string]string)
	}
	return nil
}

// Size returns the number of entries in the index.
func (idx *CIDIndex) Size() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return len(idx.Paths)
}
