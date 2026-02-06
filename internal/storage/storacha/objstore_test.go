package storacha

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/relves/ucanlog/internal/storage/storacha/storachatest"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/stretchr/testify/require"
)

type blockingClient struct {
	*MockClient
	entered     chan struct{}
	release     chan struct{}
	inFlight    int32
	maxInFlight int32
}

func newBlockingClient() *blockingClient {
	return &blockingClient{
		MockClient: NewMockClient(),
		entered:    make(chan struct{}, 16),
		release:    make(chan struct{}),
	}
}

func (c *blockingClient) UploadBlob(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	inFlight := atomic.AddInt32(&c.inFlight, 1)
	for {
		max := atomic.LoadInt32(&c.maxInFlight)
		if inFlight <= max {
			break
		}
		if atomic.CompareAndSwapInt32(&c.maxInFlight, max, inFlight) {
			break
		}
	}
	select {
	case c.entered <- struct{}{}:
	default:
	}
	<-c.release
	atomic.AddInt32(&c.inFlight, -1)

	return c.MockClient.UploadBlob(ctx, spaceDID, data, dlg)
}

func TestObjStore_SetAndGet(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	store := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())

	ctx := context.Background()
	// Add delegation to context (MockClient ignores it but it's required)
	ctx = WithDelegation(ctx, storachatest.MockDelegation())

	path := "tile/entries/000/001"
	data := []byte("bundle data here")

	// Set object
	err := store.setObject(ctx, path, data)
	require.NoError(t, err)

	// Verify CID stored in index
	cid, ok := index.Get(path)
	require.True(t, ok)
	require.NotEmpty(t, cid)

	// Get object
	retrieved, err := store.getObject(ctx, path)
	require.NoError(t, err)
	require.Equal(t, data, retrieved)
}

func TestObjStore_SetObject_AllowsConcurrentUploads(t *testing.T) {
	client := newBlockingClient()
	index := NewCIDIndex()

	store := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())

	ctx := context.Background()
	ctx = WithDelegation(ctx, storachatest.MockDelegation())

	const workers = 5
	var wg sync.WaitGroup
	wg.Add(workers)

	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		path := fmt.Sprintf("tile/concurrent/%d", i)
		go func(p string) {
			defer wg.Done()
			errs <- store.setObject(ctx, p, []byte("bundle data here"))
		}(path)
	}

	deadline := time.After(500 * time.Millisecond)
	entered := 0
	for entered < 2 {
		select {
		case <-client.entered:
			entered++
		case <-deadline:
			close(client.release)
			wg.Wait()
			require.GreaterOrEqual(t, atomic.LoadInt32(&client.maxInFlight), int32(2), "expected concurrent uploads; max in-flight was %d", atomic.LoadInt32(&client.maxInFlight))
			return
		}
	}

	close(client.release)
	wg.Wait()

	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	require.GreaterOrEqual(t, atomic.LoadInt32(&client.maxInFlight), int32(2), "expected concurrent uploads; max in-flight was %d", atomic.LoadInt32(&client.maxInFlight))
}

func TestObjStore_GetNotFound(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	store := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())

	ctx := context.Background()
	_, err := store.getObject(ctx, "nonexistent/path")
	require.Error(t, err)
}

func TestObjStore_SetObjectIfNoneMatch(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	store := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())

	ctx := context.Background()
	// Add delegation to context (MockClient ignores it but it's required)
	ctx = WithDelegation(ctx, storachatest.MockDelegation())

	path := "tile/0/000/000"
	data1 := []byte("tile data v1")
	data2 := []byte("tile data v2")

	// First write should succeed
	written, err := store.setObjectIfNoneMatch(ctx, path, data1)
	require.NoError(t, err)
	require.True(t, written)

	// Second write should return false (already exists)
	written, err = store.setObjectIfNoneMatch(ctx, path, data2)
	require.NoError(t, err)
	require.False(t, written)

	// Data should still be v1
	retrieved, _ := store.getObject(ctx, path)
	require.Equal(t, data1, retrieved)
}

func TestObjStore_DeleteWithPrefix(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	store := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())

	ctx := context.Background()
	// Add delegation to context (MockClient ignores it but it's required)
	ctx = WithDelegation(ctx, storachatest.MockDelegation())

	// Create multiple objects with same prefix
	store.setObject(ctx, "tile/entries/000/001.p/128", []byte("partial1"))
	store.setObject(ctx, "tile/entries/000/001.p/200", []byte("partial2"))
	store.setObject(ctx, "tile/entries/000/002", []byte("other"))

	// Delete prefix
	err := store.deleteObjectsWithPrefix(ctx, "tile/entries/000/001.p/")
	require.NoError(t, err)

	// Partials should be gone
	_, ok := index.Get("tile/entries/000/001.p/128")
	require.False(t, ok)
	_, ok = index.Get("tile/entries/000/001.p/200")
	require.False(t, ok)

	// Other should remain
	_, ok = index.Get("tile/entries/000/002")
	require.True(t, ok)
}

func TestObjStore_MarksDirtyOnWrite(t *testing.T) {
	index := NewCIDIndex()
	client := NewMockClient()

	// Create a mock dirty tracker
	dirtyCount := 0
	onDirty := func() { dirtyCount++ }

	store := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	store.SetOnDirty(onDirty)

	ctx := context.Background()
	// Add delegation to context (MockClient ignores it but it's required)
	ctx = WithDelegation(ctx, storachatest.MockDelegation())

	err := store.setObject(ctx, "tile/0/000/000", []byte("test data"))
	require.NoError(t, err)

	require.Equal(t, 1, dirtyCount)
}
