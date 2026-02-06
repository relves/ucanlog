package tlog

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/relves/ucanlog/internal/storage"
)

// CIDStore tracks the latest index CAR root CID for each log.
type CIDStore interface {
	GetLatestCID(logID string) (string, error)
	SetLatestCID(logID string, cid string) error
}

// FileCIDStore implements CIDStore by reading from the filesystem.
// Each log's latest CID is stored at {basePath}/logs/{logID}/.state/latest-index-cid
type FileCIDStore struct {
	basePath string
	mu       sync.RWMutex
	cache    map[string]string // In-memory cache for performance
}

// NewFileCIDStore creates a new file-based CID store.
func NewFileCIDStore(basePath string) *FileCIDStore {
	return &FileCIDStore{
		basePath: basePath,
		cache:    make(map[string]string),
	}
}

// GetLatestCID retrieves the latest index CAR root CID for a log.
func (s *FileCIDStore) GetLatestCID(logID string) (string, error) {
	// Check cache first
	s.mu.RLock()
	if cid, ok := s.cache[logID]; ok {
		s.mu.RUnlock()
		return cid, nil
	}
	s.mu.RUnlock()

	// Read from disk
	cidPath := filepath.Join(s.basePath, "logs", logID, ".state", "latest-index-cid")
	data, err := os.ReadFile(cidPath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("no index CID found for log %s", logID)
		}
		return "", fmt.Errorf("failed to read CID file: %w", err)
	}

	cid := string(data)

	// Update cache
	s.mu.Lock()
	s.cache[logID] = cid
	s.mu.Unlock()

	return cid, nil
}

// SetLatestCID updates the latest index CAR root CID for a log.
func (s *FileCIDStore) SetLatestCID(logID string, cid string) error {
	// Update cache
	s.mu.Lock()
	s.cache[logID] = cid
	s.mu.Unlock()

	// Note: The actual file write is handled by indexpersist.Manager.
	// This method is called from the OnUpload callback to update the cache.
	return nil
}

// StateStoreGetterFunc is a function that returns a StateStore for a log DID.
type StateStoreGetterFunc func(logDID string) (storage.StateStore, error)

// StateStoreCIDStore implements CIDStore using SQLite-backed StateStore.
// Reads from index_persistence table for latest CIDs.
type StateStoreCIDStore struct {
	getStore StateStoreGetterFunc
	mu       sync.RWMutex
	cache    map[string]string // In-memory cache for performance
}

// NewStateStoreCIDStore creates a new StateStore-backed CID store.
func NewStateStoreCIDStore(getStore StateStoreGetterFunc) *StateStoreCIDStore {
	return &StateStoreCIDStore{
		getStore: getStore,
		cache:    make(map[string]string),
	}
}

// GetLatestCID retrieves the latest index CAR root CID for a log.
func (s *StateStoreCIDStore) GetLatestCID(logID string) (string, error) {
	// Check cache first
	s.mu.RLock()
	if cid, ok := s.cache[logID]; ok {
		s.mu.RUnlock()
		return cid, nil
	}
	s.mu.RUnlock()

	// Read from StateStore
	store, err := s.getStore(logID)
	if err != nil {
		return "", fmt.Errorf("failed to get store for log %s: %w", logID, err)
	}

	ctx := context.Background()
	meta, err := store.GetIndexPersistence(ctx, logID)
	if err != nil {
		return "", fmt.Errorf("failed to get index persistence: %w", err)
	}

	if meta == nil || meta.LastUploadedCID == "" {
		return "", fmt.Errorf("no index CID found for log %s", logID)
	}

	// Update cache
	s.mu.Lock()
	s.cache[logID] = meta.LastUploadedCID
	s.mu.Unlock()

	return meta.LastUploadedCID, nil
}

// SetLatestCID updates the latest index CAR root CID for a log.
func (s *StateStoreCIDStore) SetLatestCID(logID string, cid string) error {
	// Update cache
	s.mu.Lock()
	s.cache[logID] = cid
	s.mu.Unlock()

	// Note: The actual database write is handled by indexpersist.Manager.
	// This method is called from the OnUpload callback to update the cache.
	return nil
}
