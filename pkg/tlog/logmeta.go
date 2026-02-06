// Package tlog provides log metadata storage for per-log configuration.
package tlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// LogMeta contains per-log metadata including customer space information.
type LogMeta struct {
	// LogDID is the DID of the log (derived from log ID key)
	LogDID string `json:"log_did"`

	// SpaceDID is the customer's Storacha space DID
	SpaceDID string `json:"space_did"`

	// HeadIndexCID is the CID of the latest index CAR root
	HeadIndexCID string `json:"head_index_cid,omitempty"`

	// TreeSize is the current log size (number of entries)
	TreeSize uint64 `json:"tree_size"`

	// CreatedAt is when the log was created
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt is when the log was last modified
	UpdatedAt time.Time `json:"updated_at"`
}

// LogMetaStore handles persistence of log metadata.
type LogMetaStore struct {
	basePath string
	cache    map[string]*LogMeta
	mu       sync.RWMutex
}

// NewLogMetaStore creates a new log metadata store.
func NewLogMetaStore(basePath string) *LogMetaStore {
	return &LogMetaStore{
		basePath: basePath,
		cache:    make(map[string]*LogMeta),
	}
}

// metaPath returns the file path for a log's metadata.
func (s *LogMetaStore) metaPath(logID string) string {
	return filepath.Join(s.basePath, "logs", logID, "meta.json")
}

// Get retrieves log metadata by log ID.
func (s *LogMetaStore) Get(logID string) (*LogMeta, error) {
	// Check cache first
	s.mu.RLock()
	if meta, ok := s.cache[logID]; ok {
		s.mu.RUnlock()
		return meta, nil
	}
	s.mu.RUnlock()

	// Load from disk
	path := s.metaPath(logID)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("log metadata not found for %s", logID)
		}
		return nil, fmt.Errorf("failed to read log metadata: %w", err)
	}

	var meta LogMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse log metadata: %w", err)
	}

	// Update cache
	s.mu.Lock()
	s.cache[logID] = &meta
	s.mu.Unlock()

	return &meta, nil
}

// Save persists log metadata.
func (s *LogMetaStore) Save(logID string, meta *LogMeta) error {
	path := s.metaPath(logID)

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create metadata directory: %w", err)
	}

	// Update timestamp
	meta.UpdatedAt = time.Now()

	data, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to serialize log metadata: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write log metadata: %w", err)
	}

	// Update cache
	s.mu.Lock()
	s.cache[logID] = meta
	s.mu.Unlock()

	return nil
}

// Create creates new log metadata.
func (s *LogMetaStore) Create(logID, logDID, spaceDID string) (*LogMeta, error) {
	now := time.Now()
	meta := &LogMeta{
		LogDID:    logDID,
		SpaceDID:  spaceDID,
		TreeSize:  0,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.Save(logID, meta); err != nil {
		return nil, err
	}

	return meta, nil
}

// UpdateHead updates the head index CID and tree size for a log.
func (s *LogMetaStore) UpdateHead(logID string, headCID string, treeSize uint64) error {
	meta, err := s.Get(logID)
	if err != nil {
		return err
	}

	meta.HeadIndexCID = headCID
	meta.TreeSize = treeSize
	return s.Save(logID, meta)
}

// Exists checks if log metadata exists.
func (s *LogMetaStore) Exists(logID string) bool {
	// Check cache
	s.mu.RLock()
	if _, ok := s.cache[logID]; ok {
		s.mu.RUnlock()
		return true
	}
	s.mu.RUnlock()

	// Check disk
	path := s.metaPath(logID)
	_, err := os.Stat(path)
	return err == nil
}

// Delete removes log metadata.
func (s *LogMetaStore) Delete(logID string) error {
	s.mu.Lock()
	delete(s.cache, logID)
	s.mu.Unlock()

	path := s.metaPath(logID)
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete log metadata: %w", err)
	}

	return nil
}
