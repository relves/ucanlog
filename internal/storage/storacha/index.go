package storacha

import (
	"encoding/json"
	"sync"
)

// CIDIndex maps Tessera storage paths to Storacha CIDs.
// Thread-safe for concurrent access.
//
// Paths follow Tessera's layout:
//   - "checkpoint" - latest checkpoint
//   - "tile/entries/NNN/NNN/..." - entry bundles
//   - "tile/L/NNN/NNN/..." - merkle tree tiles at level L
type CIDIndex struct {
	Paths map[string]string `json:"paths"`
	mu    sync.RWMutex
}

// NewCIDIndex creates an empty CID index.
func NewCIDIndex() *CIDIndex {
	return &CIDIndex{
		Paths: make(map[string]string),
	}
}

// NewCIDIndexFromMap creates a CID index from an existing map.
func NewCIDIndexFromMap(data map[string]string) *CIDIndex {
	if data == nil {
		data = make(map[string]string)
	}
	return &CIDIndex{
		Paths: data,
	}
}

// Set stores a CID for a path.
func (idx *CIDIndex) Set(path, cid string) error {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.Paths[path] = cid
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
