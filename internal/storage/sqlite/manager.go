package sqlite

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/relves/ucanlog/internal/storage"
)

// StoreManager manages multiple LogStore instances with caching.
type StoreManager struct {
	basePath string
	stores   map[string]*LogStore // mainLogDID -> store
	mu       sync.RWMutex
}

// NewStoreManager creates a new StoreManager.
func NewStoreManager(basePath string) *StoreManager {
	return &StoreManager{
		basePath: basePath,
		stores:   make(map[string]*LogStore),
	}
}

// GetStore returns the LogStore for the given log DID.
// If logDID ends with "-revocations", it returns the store for the main log.
// Stores are cached and reused.
func (m *StoreManager) GetStore(logDID string) (*LogStore, error) {
	// Normalize to main log DID
	mainLogDID := strings.TrimSuffix(logDID, "-revocations")

	// Check cache first
	m.mu.RLock()
	if store, ok := m.stores[mainLogDID]; ok {
		m.mu.RUnlock()
		return store, nil
	}
	m.mu.RUnlock()

	// Open new store
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if store, ok := m.stores[mainLogDID]; ok {
		return store, nil
	}

	store, err := OpenLogStore(m.basePath, mainLogDID)
	if err != nil {
		return nil, err
	}

	m.stores[mainLogDID] = store
	return store, nil
}

// CloseAll closes all cached stores.
func (m *StoreManager) CloseAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for _, store := range m.stores {
		if err := store.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	m.stores = make(map[string]*LogStore)
	return errors.Join(errs...)
}

// ListLogDIDs returns the DIDs of all known logs by enumerating subdirectories.
func (m *StoreManager) ListLogDIDs() ([]string, error) {
	logsDir := filepath.Join(m.basePath, "logs")
	entries, err := os.ReadDir(logsDir)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var dids []string
	for _, e := range entries {
		if e.IsDir() {
			dbPath := filepath.Join(logsDir, e.Name(), "log.db")
			if _, statErr := os.Stat(dbPath); statErr == nil {
				dids = append(dids, e.Name())
			}
		}
	}
	return dids, nil
}

// BasePath returns the base path for log storage.
func (m *StoreManager) BasePath() string {
	return m.basePath
}

// GetStateStore returns the StateStore for the given log DID.
// This is a convenience method that returns storage.StateStore interface.
func (m *StoreManager) GetStateStore(logDID string) (storage.StateStore, error) {
	return m.GetStore(logDID)
}


