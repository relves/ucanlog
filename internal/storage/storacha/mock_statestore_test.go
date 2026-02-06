package storacha

import (
	"context"
	"sync"
	"time"

	"github.com/relves/ucanlog/internal/storage"
)

// mockStateStore implements storage.StateStore for testing.
type mockStateStore struct {
	mu sync.Mutex

	heads       map[string]headState
	cidIndexes  map[string]map[string]string
	treeStates  map[string]treeState
	revocations map[string]bool
	indexMeta   map[string]*storage.IndexPersistenceMeta
	gcProgress  map[string]uint64
}

type headState struct {
	indexCID string
	treeSize uint64
}

type treeState struct {
	size uint64
	root []byte
}

func newMockStateStore() *mockStateStore {
	return &mockStateStore{
		heads:       make(map[string]headState),
		cidIndexes:  make(map[string]map[string]string),
		treeStates:  make(map[string]treeState),
		revocations: make(map[string]bool),
		indexMeta:   make(map[string]*storage.IndexPersistenceMeta),
		gcProgress:  make(map[string]uint64),
	}
}

func (m *mockStateStore) GetHead(ctx context.Context, logDID string) (string, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Read from treeState and indexMeta
	ts := m.treeStates[logDID]
	meta := m.indexMeta[logDID]
	var cid string
	if meta != nil {
		cid = meta.LastUploadedCID
	}
	return cid, ts.size, nil
}

func (m *mockStateStore) GetCIDIndex(ctx context.Context, logDID string) (map[string]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.cidIndexes[logDID]
	if idx == nil {
		return make(map[string]string), nil
	}
	// Return a copy
	result := make(map[string]string, len(idx))
	for k, v := range idx {
		result[k] = v
	}
	return result, nil
}

func (m *mockStateStore) SetCID(ctx context.Context, logDID, path, cid string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cidIndexes[logDID] == nil {
		m.cidIndexes[logDID] = make(map[string]string)
	}
	m.cidIndexes[logDID][path] = cid
	return nil
}

func (m *mockStateStore) SetCIDs(ctx context.Context, logDID string, mappings map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.cidIndexes[logDID] == nil {
		m.cidIndexes[logDID] = make(map[string]string)
	}
	for k, v := range mappings {
		m.cidIndexes[logDID][k] = v
	}
	return nil
}

func (m *mockStateStore) GetTreeState(ctx context.Context, logDID string) (uint64, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts := m.treeStates[logDID]
	return ts.size, ts.root, nil
}

func (m *mockStateStore) SetTreeState(ctx context.Context, logDID string, size uint64, root []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.treeStates[logDID] = treeState{size: size, root: root}
	return nil
}

func (m *mockStateStore) AddRevocation(ctx context.Context, delegationCID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.revocations[delegationCID] = true
	return nil
}

func (m *mockStateStore) IsRevoked(ctx context.Context, delegationCID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.revocations[delegationCID], nil
}

func (m *mockStateStore) GetRevocations(ctx context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var result []string
	for cid := range m.revocations {
		result = append(result, cid)
	}
	return result, nil
}

func (m *mockStateStore) GetIndexPersistence(ctx context.Context, logDID string) (*storage.IndexPersistenceMeta, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.indexMeta[logDID], nil
}

func (m *mockStateStore) SetIndexPersistence(ctx context.Context, logDID string, uploadTime time.Time, uploadedSize uint64, uploadedCID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.indexMeta[logDID] = &storage.IndexPersistenceMeta{
		LastUploadTime:   uploadTime,
		LastUploadedSize: uploadedSize,
		LastUploadedCID:  uploadedCID,
	}
	return nil
}

func (m *mockStateStore) DeleteCIDsWithPrefix(ctx context.Context, logDID, prefix string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	idx := m.cidIndexes[logDID]
	if idx == nil {
		return nil
	}
	for path := range idx {
		if len(path) >= len(prefix) && path[:len(prefix)] == prefix {
			delete(idx, path)
		}
	}
	return nil
}

func (m *mockStateStore) GetGCProgress(ctx context.Context, logDID string) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.gcProgress[logDID], nil
}

func (m *mockStateStore) SetGCProgress(ctx context.Context, logDID string, fromSize uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.gcProgress[logDID] = fromSize
	return nil
}
