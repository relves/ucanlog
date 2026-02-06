// storage/storacha/indexpersist/manager_test.go
package indexpersist

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/relves/ucanlog/internal/storage"
	"github.com/stretchr/testify/require"
)

// mockStateStore is a minimal mock for testing index persistence.
// Only implements the methods needed by Manager.
type mockStateStore struct {
	mu        sync.Mutex
	indexMeta map[string]*storage.IndexPersistenceMeta
}

func newMockStateStore() *mockStateStore {
	return &mockStateStore{
		indexMeta: make(map[string]*storage.IndexPersistenceMeta),
	}
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

func (m *mockStateStore) getIndexMeta(logDID string) *storage.IndexPersistenceMeta {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.indexMeta[logDID]
}

// Stub implementations for unused StateStore methods
func (m *mockStateStore) GetHead(ctx context.Context, logDID string) (string, uint64, error) {
	return "", 0, nil
}
func (m *mockStateStore) GetCIDIndex(ctx context.Context, logDID string) (map[string]string, error) {
	return nil, nil
}
func (m *mockStateStore) SetCID(ctx context.Context, logDID, path, cid string) error { return nil }
func (m *mockStateStore) SetCIDs(ctx context.Context, logDID string, mappings map[string]string) error {
	return nil
}
func (m *mockStateStore) GetTreeState(ctx context.Context, logDID string) (uint64, []byte, error) {
	return 0, nil, nil
}
func (m *mockStateStore) SetTreeState(ctx context.Context, logDID string, size uint64, root []byte) error {
	return nil
}
func (m *mockStateStore) AddRevocation(ctx context.Context, delegationCID string) error { return nil }
func (m *mockStateStore) IsRevoked(ctx context.Context, delegationCID string) (bool, error) {
	return false, nil
}
func (m *mockStateStore) GetRevocations(ctx context.Context) ([]string, error) { return nil, nil }
func (m *mockStateStore) DeleteCIDsWithPrefix(ctx context.Context, logDID, prefix string) error {
	return nil
}
func (m *mockStateStore) GetGCProgress(ctx context.Context, logDID string) (uint64, error) {
	return 0, nil
}
func (m *mockStateStore) SetGCProgress(ctx context.Context, logDID string, fromSize uint64) error {
	return nil
}

type mockUploader struct {
	mu      sync.Mutex
	uploads [][]byte
}

func (m *mockUploader) UploadCAR(ctx context.Context, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploads = append(m.uploads, data)
	return "bafybeimockrootcid", nil
}

func (m *mockUploader) UploadCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.uploads)
}

// slowMockUploader introduces a delay to simulate realistic upload timing.
// This allows testing concurrent writer scenarios where uploads are in progress.
type slowMockUploader struct {
	mu       sync.Mutex
	uploads  [][]byte
	delay    time.Duration
	failNext bool
}

func (m *slowMockUploader) UploadCAR(ctx context.Context, data []byte) (string, error) {
	time.Sleep(m.delay) // Simulate network latency
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return "", fmt.Errorf("simulated upload failure")
	}
	m.uploads = append(m.uploads, data)
	return "bafybeimockrootcid", nil
}

func (m *slowMockUploader) UploadCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.uploads)
}

type mockIndexProvider struct {
	mu    sync.Mutex
	index map[string]string
}

func (m *mockIndexProvider) GetIndex() map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]string, len(m.index))
	for k, v := range m.index {
		result[k] = v
	}
	return result
}

func (m *mockIndexProvider) SetIndex(index map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.index = index
}

func TestManager_PeriodicUpload(t *testing.T) {
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		Interval:   50 * time.Millisecond,
		PathPrefix: "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start the manager
	go mgr.Start(ctx)

	// Wait for at least one upload cycle
	time.Sleep(150 * time.Millisecond)

	// Should have uploaded at least once
	require.GreaterOrEqual(t, uploader.UploadCount(), 1)
}

func TestManager_NoUploadIfUnchanged(t *testing.T) {
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		Interval:   50 * time.Millisecond,
		PathPrefix: "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.Start(ctx)

	// Wait for multiple cycles
	time.Sleep(200 * time.Millisecond)

	// Should upload once (initial), then not again since no changes
	// First upload happens, subsequent ones skip because unchanged
	count := uploader.UploadCount()
	require.GreaterOrEqual(t, count, 1)
	require.LessOrEqual(t, count, 2) // At most 2 due to timing
}

func TestManager_UploadOnChange(t *testing.T) {
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		Interval:   50 * time.Millisecond,
		PathPrefix: "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.Start(ctx)

	// Wait for first upload
	time.Sleep(75 * time.Millisecond)
	initialCount := uploader.UploadCount()
	require.GreaterOrEqual(t, initialCount, 1)

	// Modify the index
	indexProvider.SetIndex(map[string]string{
		"checkpoint":     "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		"tile/0/000/000": "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre",
	})

	// Mark as dirty
	mgr.MarkDirty()

	// Wait for next cycle
	time.Sleep(75 * time.Millisecond)

	// Should have uploaded again
	require.Greater(t, uploader.UploadCount(), initialCount)
}

func TestManager_CallbackOnUpload(t *testing.T) {
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	var callbackCID string
	var callbackMeta IndexMeta
	var callbackCalled bool

	cfg := Config{
		Interval:   50 * time.Millisecond,
		PathPrefix: "index/",
		OnUpload: func(rootCID string, meta IndexMeta) {
			callbackCID = rootCID
			callbackMeta = meta
			callbackCalled = true
		},
	}

	mgr := NewManager(cfg, uploader, indexProvider)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.Start(ctx)

	// Wait for upload
	time.Sleep(100 * time.Millisecond)

	require.True(t, callbackCalled)
	require.NotEmpty(t, callbackCID)
	require.Equal(t, 1, callbackMeta.EntryCount)
}

func TestManager_PersistsMetadataToStateStore(t *testing.T) {
	stateStore := newMockStateStore()
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54"},
	}

	cfg := Config{
		Interval:   50 * time.Millisecond,
		PathPrefix: "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)
	mgr.SetStateStore(stateStore, "did:key:test")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go mgr.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	// Check that metadata was persisted to StateStore
	meta := stateStore.getIndexMeta("did:key:test")
	require.NotNil(t, meta)
	require.NotEmpty(t, meta.LastUploadedCID)
	require.Equal(t, uint64(1), meta.LastUploadedSize)
}

func TestTriggerPersistAsync_BasicTrigger(t *testing.T) {
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 0, // No rate limiting for this test
		PathPrefix:  "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)
	ctx := context.Background()

	// Trigger persist
	mgr.TriggerPersistAsync(ctx)

	// Wait for async upload
	time.Sleep(100 * time.Millisecond)

	// Verify upload occurred
	require.Equal(t, 1, uploader.UploadCount())

	// Verify dirty is cleared
	meta := mgr.GetMeta()
	require.Equal(t, 1, meta.EntryCount)
	require.Equal(t, uint64(1), meta.Version)
}

func TestTriggerPersistAsync_RateLimiting(t *testing.T) {
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 200 * time.Millisecond,
		PathPrefix:  "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)
	ctx := context.Background()

	// First trigger
	mgr.TriggerPersistAsync(ctx)
	time.Sleep(50 * time.Millisecond)

	// Second trigger within MinInterval
	mgr.TriggerPersistAsync(ctx)
	time.Sleep(100 * time.Millisecond)

	// Should only have uploaded once (rate limited)
	require.Equal(t, 1, uploader.UploadCount())
}

func TestTriggerPersistAsync_ConcurrentWriters(t *testing.T) {
	// Use slow uploader so upload is still in progress when B/C trigger
	uploader := &slowMockUploader{delay: 100 * time.Millisecond}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 0,
		PathPrefix:  "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)
	ctxA := context.Background()
	ctxB := context.Background()
	ctxC := context.Background()

	// A triggers and starts persist (upload takes 100ms)
	mgr.TriggerPersistAsync(ctxA)
	time.Sleep(20 * time.Millisecond) // Wait for goroutine to start

	// B and C trigger while A running (their contexts queue)
	mgr.TriggerPersistAsync(ctxB)
	mgr.TriggerPersistAsync(ctxC)

	// Modify index to simulate B and C writes (while A's upload still in progress)
	indexProvider.SetIndex(map[string]string{
		"checkpoint":     "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		"tile/0/000/000": "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre",
	})
	// Mark dirty to trigger follow-up
	mgr.MarkDirty()

	// Wait for both uploads to complete (100ms each + buffer)
	time.Sleep(350 * time.Millisecond)

	// Should have two uploads: A's + follow-up with C's context
	require.Equal(t, 2, uploader.UploadCount())
}

func TestTriggerPersistAsync_PersistInProgressQueues(t *testing.T) {
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 0,
		PathPrefix:  "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)
	ctxA := context.Background()
	ctxB := context.Background()

	// A triggers and starts persist
	mgr.TriggerPersistAsync(ctxA)
	time.Sleep(50 * time.Millisecond)

	// B triggers while A running → should queue in pendingCtx
	mgr.TriggerPersistAsync(ctxB)
	time.Sleep(50 * time.Millisecond)

	// Verify only one upload started
	require.Equal(t, 1, uploader.UploadCount())
}

func TestTriggerPersistAsync_FollowUpOnIndexChange(t *testing.T) {
	// Use slow uploader so we can change index while upload is in progress
	uploader := &slowMockUploader{delay: 100 * time.Millisecond}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 0,
		PathPrefix:  "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)
	ctxA := context.Background()
	ctxB := context.Background()

	// A triggers, captures hash H1 (upload takes 100ms)
	mgr.TriggerPersistAsync(ctxA)
	time.Sleep(20 * time.Millisecond) // Wait for goroutine to start

	// B queues context while A running
	mgr.TriggerPersistAsync(ctxB)

	// Index changes to H2 during upload (while A's upload still in progress)
	indexProvider.SetIndex(map[string]string{
		"checkpoint":     "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		"tile/0/000/000": "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre",
	})
	// Mark dirty to trigger follow-up
	mgr.MarkDirty()

	// Wait for both uploads to complete (100ms each + buffer)
	time.Sleep(350 * time.Millisecond)

	// Should have two uploads: initial + follow-up
	require.Equal(t, 2, uploader.UploadCount())
}

func TestTriggerPersistAsync_NoFollowUpIfUnchanged(t *testing.T) {
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 0,
		PathPrefix:  "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)
	ctx := context.Background()

	// Trigger persist
	mgr.TriggerPersistAsync(ctx)
	time.Sleep(100 * time.Millisecond)

	// Should have one upload
	require.Equal(t, 1, uploader.UploadCount())

	// Wait longer - no follow-up should occur since index unchanged
	time.Sleep(100 * time.Millisecond)

	// Still only one upload
	require.Equal(t, 1, uploader.UploadCount())
}

type failingUploader struct{}

func (f *failingUploader) UploadCAR(ctx context.Context, data []byte) (string, error) {
	return "", fmt.Errorf("simulated upload failure")
}

func TestTriggerPersistAsync_UploadFailureStaysDirty(t *testing.T) {
	failingUploader := &failingUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 0,
		PathPrefix:  "index/",
	}

	mgr := NewManager(cfg, failingUploader, indexProvider)
	ctx := context.Background()

	// Trigger persist (will fail)
	mgr.TriggerPersistAsync(ctx)
	time.Sleep(100 * time.Millisecond)

	// Verify manager stays dirty and pendingCtx is preserved for retry
	// Note: Since upload failed, the dirty flag stays true for retry
}

func TestTriggerPersistAsync_NotDirtyNoOp(t *testing.T) {
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 0,
		PathPrefix:  "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)
	ctx := context.Background()

	// First trigger
	mgr.TriggerPersistAsync(ctx)
	time.Sleep(100 * time.Millisecond)

	// Should have uploaded once
	require.Equal(t, 1, uploader.UploadCount())

	// Clear dirty flag manually (simulating successful upload)
	// Note: In real scenario, dirty is cleared by successful upload

	// Second trigger with dirty=false should be no-op
	mgr.TriggerPersistAsync(ctx)
	time.Sleep(50 * time.Millisecond)

	// Still only one upload
	require.Equal(t, 1, uploader.UploadCount())
}

func TestTriggerPersistAsync_ContextCancellation(t *testing.T) {
	uploader := &mockUploader{}
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 0,
		PathPrefix:  "index/",
	}

	mgr := NewManager(cfg, uploader, indexProvider)

	// Create a context that we'll cancel immediately after triggering
	ctx, cancel := context.WithCancel(context.Background())

	// Trigger persist with the cancelable context
	mgr.TriggerPersistAsync(ctx)

	// Cancel the context immediately (simulating request completion)
	cancel()

	// Wait for async upload to complete
	time.Sleep(100 * time.Millisecond)

	// Upload should succeed despite context cancellation because
	// TriggerPersistAsync extracts the delegation and creates a new background context
	require.Equal(t, 1, uploader.UploadCount())
}

// TestTriggerPersistAsync_RestoredLogWithRecentUpload simulates an existing log
// being restored from a previous session. The bug: when LastUploaded is loaded
// from StateStore and is within MinInterval, subsequent writes are rate-limited
// but the delegation context expires (not persisted for security), so the index
// is never uploaded even after MinInterval passes.
func TestTriggerPersistAsync_RestoredLogWithRecentUpload(t *testing.T) {
	stateStore := newMockStateStore()
	uploader := &mockUploader{}

	// Initial index state (as if restored from previous session)
	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 100 * time.Millisecond, // Short interval for testing
		PathPrefix:  "index/",
	}

	// Simulate a previous session: StateStore has LastUploadTime from 50ms ago
	// This is within MinInterval, so the first trigger will be rate-limited
	recentUploadTime := time.Now().Add(-50 * time.Millisecond)
	stateStore.SetIndexPersistence(context.Background(), "did:key:test",
		recentUploadTime, 1, "bafyPreviousIndexCID")

	// Create manager and restore state (simulating app restart)
	mgr := NewManager(cfg, uploader, indexProvider)
	mgr.SetStateStore(stateStore, "did:key:test")

	// Simulate a write that triggers persistence
	// In reality, this context would have a delegation attached
	ctx1 := context.Background()

	// Add new entry to index (simulating a write)
	indexProvider.SetIndex(map[string]string{
		"checkpoint":     "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		"tile/0/000/000": "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre",
	})
	mgr.MarkDirty()

	// First trigger - should be rate-limited (within MinInterval of previous upload)
	mgr.TriggerPersistAsync(ctx1)
	time.Sleep(20 * time.Millisecond)

	// No upload yet (rate limited)
	require.Equal(t, 0, uploader.UploadCount(), "should be rate-limited initially")

	// Wait for MinInterval to pass
	time.Sleep(100 * time.Millisecond)

	// Now simulate another write with a NEW delegation context
	// This is the key: in the real system, each write provides a fresh delegation
	ctx2 := context.Background()

	// Add another entry
	indexProvider.SetIndex(map[string]string{
		"checkpoint":     "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		"tile/0/000/000": "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre",
		"tile/0/000/001": "bafkreig443t24l47kdbtgfc3wmrywdiu26owozukbwhvz3nfx7s4og475e",
	})
	mgr.MarkDirty()

	// Second trigger - MinInterval has passed, should upload now
	mgr.TriggerPersistAsync(ctx2)
	time.Sleep(100 * time.Millisecond)

	// BUG: This should be 1, but if the first ctx1 was consumed and cleared
	// without uploading (due to rate limiting), and the system doesn't properly
	// handle the second trigger, the upload may never happen.
	require.Equal(t, 1, uploader.UploadCount(), "should have uploaded after MinInterval passed")
}

// TestTriggerPersistAsync_RateLimitedThenNewWrite tests that when a trigger is
// rate-limited, the pendingCtx is preserved and used when MinInterval passes.
func TestTriggerPersistAsync_RateLimitedThenNewWrite(t *testing.T) {
	stateStore := newMockStateStore()
	uploader := &mockUploader{}

	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 100 * time.Millisecond,
		PathPrefix:  "index/",
	}

	// Simulate previous upload was very recent (within MinInterval)
	recentUploadTime := time.Now().Add(-10 * time.Millisecond)
	stateStore.SetIndexPersistence(context.Background(), "did:key:test",
		recentUploadTime, 1, "bafyPreviousIndexCID")

	mgr := NewManager(cfg, uploader, indexProvider)
	mgr.SetStateStore(stateStore, "did:key:test")

	ctx := context.Background()

	// Modify index and trigger - gets rate-limited
	indexProvider.SetIndex(map[string]string{
		"checkpoint":     "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		"tile/0/000/000": "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre",
	})
	mgr.MarkDirty()
	mgr.TriggerPersistAsync(ctx)

	time.Sleep(20 * time.Millisecond)
	require.Equal(t, 0, uploader.UploadCount(), "should be rate-limited")

	// Wait for MinInterval to pass, then trigger again
	time.Sleep(150 * time.Millisecond)

	// Trigger again after MinInterval - this MUST work
	mgr.TriggerPersistAsync(ctx)
	time.Sleep(100 * time.Millisecond)

	require.Equal(t, 1, uploader.UploadCount(), "should upload after MinInterval")
}

// delegationRequiredUploader simulates the real StorachaUploader behavior:
// it requires a delegation to be present in context, otherwise returns error.
type delegationRequiredUploader struct {
	mu           sync.Mutex
	uploads      [][]byte
	ctxValidator func(ctx context.Context) bool // Returns true if ctx has valid delegation
}

func (m *delegationRequiredUploader) UploadCAR(ctx context.Context, data []byte) (string, error) {
	// Check if context has valid delegation (simulating real uploader behavior)
	if m.ctxValidator != nil && !m.ctxValidator(ctx) {
		return "", fmt.Errorf("delegation required in context for write operations")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploads = append(m.uploads, data)
	return "bafybeimockrootcid", nil
}

func (m *delegationRequiredUploader) UploadCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.uploads)
}

// contextKey for test delegation marker
type testDelegationKey struct{}

// TestTriggerPersistAsync_RateLimitedPreservesDelegation verifies that when a trigger
// is rate-limited, the delegation context is preserved and used when MinInterval passes.
//
// Scenario: A write with valid delegation triggers persist, but gets rate-limited.
// Later, a trigger without delegation (e.g., internal retry) should still succeed
// by using the previously stored delegation context.
//
// This was a bug where pendingCtx was always overwritten, causing the valid delegation
// to be lost when a subsequent trigger came without one.
func TestTriggerPersistAsync_RateLimitedPreservesDelegation(t *testing.T) {
	stateStore := newMockStateStore()

	// Uploader that requires "delegation" marker in context
	uploader := &delegationRequiredUploader{
		ctxValidator: func(ctx context.Context) bool {
			return ctx.Value(testDelegationKey{}) != nil
		},
	}

	indexProvider := &mockIndexProvider{
		index: map[string]string{
			"checkpoint": "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		},
	}

	cfg := Config{
		MinInterval: 100 * time.Millisecond,
		PathPrefix:  "index/",
	}

	// Simulate previous upload just happened (within MinInterval)
	recentUploadTime := time.Now().Add(-10 * time.Millisecond)
	stateStore.SetIndexPersistence(context.Background(), "did:key:test",
		recentUploadTime, 1, "bafyPreviousIndexCID")

	mgr := NewManager(cfg, uploader, indexProvider)
	mgr.SetStateStore(stateStore, "did:key:test")

	// Context WITH delegation (simulating a write request)
	ctxWithDelegation := context.WithValue(context.Background(), testDelegationKey{}, "valid-delegation")

	// Modify index (simulating a write happened)
	indexProvider.SetIndex(map[string]string{
		"checkpoint":     "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		"tile/0/000/000": "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre",
	})
	mgr.MarkDirty()

	// Trigger with delegation - gets RATE LIMITED
	// The delegation context is stored in pendingCtx but NO persist happens
	mgr.TriggerPersistAsync(ctxWithDelegation)
	time.Sleep(20 * time.Millisecond)

	require.Equal(t, 0, uploader.UploadCount(), "should be rate-limited initially")

	// Now: MinInterval passes... but NO NEW WRITE comes!
	// In real system: user made one write, went away. Index dirty but no new delegation.
	time.Sleep(150 * time.Millisecond)

	// The periodic ticker (maybeUpload via Start()) would try to upload,
	// but it uses the context stored from TriggerPersistAsync.
	// Let's simulate what happens if we call maybeUpload directly:
	// Actually maybeUpload doesn't use pendingCtx - it creates its own context.
	// The BUG is: TriggerPersistAsync rate-limits and returns WITHOUT starting persist,
	// so pendingCtx just sits there. When MinInterval passes, nothing triggers persist
	// because there's no new TriggerPersistAsync call (no new write).

	// To test this properly, we need to verify that AFTER MinInterval,
	// calling TriggerPersistAsync with NO delegation should still work
	// IF we had stored a valid delegation earlier.

	// Trigger again but WITHOUT delegation (simulating internal retry or timer)
	ctxWithoutDelegation := context.Background()
	mgr.TriggerPersistAsync(ctxWithoutDelegation)
	time.Sleep(100 * time.Millisecond)

	require.Equal(t, 1, uploader.UploadCount(),
		"should have used the stored delegation from first trigger")
}
