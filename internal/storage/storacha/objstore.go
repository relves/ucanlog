package storacha

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ipfs/go-cid"
)

// objStore is an in-memory state buffer for log resources.
// All network I/O is performed by flush/cold-start paths, not here.
type objStore struct {
	logger        *slog.Logger
	mu            sync.RWMutex
	stateMap      map[string][]byte
	finalizedCIDs map[string]cid.Cid
}

// newObjStore creates a new in-memory object store.
func newObjStore(logger *slog.Logger) *objStore {
	if logger == nil {
		logger = slog.Default()
	}

	return &objStore{
		logger:        logger,
		stateMap:      make(map[string][]byte),
		finalizedCIDs: make(map[string]cid.Cid),
	}
}

// SetFinalizedCID records that a path's data is stored externally under the given CID.
// The path remains in stateMap so in-process reads (ReadEntryBundle) still work, but
// it is excluded from Snapshot() so it is not re-embedded in the hybrid CAR.
func (s *objStore) SetFinalizedCID(path string, c cid.Cid) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalizedCIDs[path] = c
}

// GetFinalizedCIDs returns a copy of the finalized CID map.
func (s *objStore) GetFinalizedCIDs() map[string]cid.Cid {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]cid.Cid, len(s.finalizedCIDs))
	for k, v := range s.finalizedCIDs {
		out[k] = v
	}
	return out
}

// LoadFinalizedCIDs replaces the finalized CID map (used during cold start).
func (s *objStore) LoadFinalizedCIDs(cids map[string]cid.Cid) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finalizedCIDs = make(map[string]cid.Cid, len(cids))
	for k, v := range cids {
		s.finalizedCIDs[k] = v
	}
}

// setObject writes object data into the in-memory state map.
func (s *objStore) setObject(ctx context.Context, path string, data []byte) error {
	s.mu.Lock()
	s.stateMap[path] = append([]byte(nil), data...)
	s.mu.Unlock()
	return nil
}

// getObject retrieves object data from the in-memory state map.
func (s *objStore) getObject(ctx context.Context, path string) ([]byte, error) {
	s.mu.RLock()
	data, ok := s.stateMap[path]
	s.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("path not found in state: %s", path)
	}
	return append([]byte(nil), data...), nil
}

// setObjectIfNoneMatch writes only if the path doesn't already exist.
// Returns (true, nil) if written, (false, nil) if already exists.
func (s *objStore) setObjectIfNoneMatch(ctx context.Context, path string, data []byte) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.stateMap[path]; ok {
		return false, nil
	}
	// Also reject writes to paths that have been finalized externally.
	if _, ok := s.finalizedCIDs[path]; ok {
		return false, nil
	}
	s.stateMap[path] = append([]byte(nil), data...)
	return true, nil
}

// Snapshot returns a deep copy of the current in-memory state, excluding finalized
// bundle paths (those are referenced by CID link in the hybrid CAR instead).
func (s *objStore) Snapshot() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	out := make(map[string][]byte, len(s.stateMap))
	for k, v := range s.stateMap {
		if _, finalized := s.finalizedCIDs[k]; finalized {
			continue
		}
		out[k] = append([]byte(nil), v...)
	}
	return out
}

// Load replaces the in-memory state with a deep-copied snapshot.
func (s *objStore) Load(data map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.stateMap = make(map[string][]byte, len(data))
	for k, v := range data {
		s.stateMap[k] = append([]byte(nil), v...)
	}
}
