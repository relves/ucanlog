package storacha

import (
	"context"
	"sync"

	"github.com/relves/ucanlog/internal/storage"
)

// mockStateStore implements storage.StateStore for testing.
type mockStateStore struct {
	mu sync.Mutex

	heads          map[string]headState
	treeStates     map[string]treeState
	revocations    map[string]bool
	latestHeadCARs map[string]latestHeadCAR
	uploadQueue    []storage.PendingCAR
	uploadBlobs    map[int64][]storage.PendingBlob
}

type latestHeadCAR struct {
	headCID  string
	treeSize uint64
	carData  []byte
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
		heads:          make(map[string]headState),
		treeStates:     make(map[string]treeState),
		revocations:    make(map[string]bool),
		latestHeadCARs: make(map[string]latestHeadCAR),
		uploadBlobs:    make(map[int64][]storage.PendingBlob),
	}
}

func (m *mockStateStore) GetHead(ctx context.Context, logDID string) (string, uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lhc, ok := m.latestHeadCARs[logDID]; ok {
		return lhc.headCID, lhc.treeSize, nil
	}
	ts := m.treeStates[logDID]
	return "", ts.size, nil
}

func (m *mockStateStore) SetLatestHeadCAR(ctx context.Context, logDID string, treeSize uint64, headCID string, carData []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latestHeadCARs[logDID] = latestHeadCAR{headCID: headCID, treeSize: treeSize, carData: carData}
	return nil
}

func (m *mockStateStore) GetLatestHeadCAR(ctx context.Context, logDID string) (string, uint64, []byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lhc, ok := m.latestHeadCARs[logDID]; ok {
		return lhc.headCID, lhc.treeSize, lhc.carData, nil
	}
	return "", 0, nil, nil
}

func (m *mockStateStore) EnqueueAndUpdateHead(ctx context.Context, logDID string, treeSize uint64, headCID string, carData []byte, blobs []storage.PendingBlob) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latestHeadCARs[logDID] = latestHeadCAR{headCID: headCID, treeSize: treeSize, carData: carData}
	id := int64(len(m.uploadQueue) + 1)
	m.uploadQueue = append(m.uploadQueue, storage.PendingCAR{
		ID: id, LogDID: logDID, TreeSize: treeSize, HeadCID: headCID, CARData: carData,
	})
	m.uploadBlobs[id] = blobs
	return id, nil
}

func (m *mockStateStore) DequeuePendingCARs(ctx context.Context, limit int) ([]storage.PendingCAR, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if limit > len(m.uploadQueue) {
		limit = len(m.uploadQueue)
	}
	return m.uploadQueue[:limit], nil
}

func (m *mockStateStore) GetPendingBlobsForCAR(ctx context.Context, queueID int64) ([]storage.PendingBlob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.uploadBlobs[queueID], nil
}

func (m *mockStateStore) MarkCARUploaded(ctx context.Context, id int64) error          { return nil }
func (m *mockStateStore) MarkCARFailed(ctx context.Context, id int64, e string) error  { return nil }
func (m *mockStateStore) MarkBlobUploaded(ctx context.Context, id int64) error         { return nil }
func (m *mockStateStore) MarkBlobFailed(ctx context.Context, id int64, e string) error { return nil }

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
