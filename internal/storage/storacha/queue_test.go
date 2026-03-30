package storacha

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transparency-dev/tessera"
)

func TestQueue_BatchedAdd(t *testing.T) {
	ctx := context.Background()

	mockClient := NewMockClient()
	stateStore := newMockStateStore()
	driver, err := New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: stateStore,
		LogDID:     "did:key:test",
		Client:     mockClient,
	})
	require.NoError(t, err)

	storage := driver.(*Storage)
	signer := &dummySigner{}

	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(signer).
		WithBatching(10, 100*time.Millisecond)

	appender, reader, err := storage.Appender(ctx, opts)
	require.NoError(t, err)

	var wg sync.WaitGroup
	futures := make([]tessera.IndexFuture, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			entry := tessera.NewEntry([]byte{byte(idx)})
			futures[idx] = appender.Add(ctx, entry)
		}(i)
	}
	wg.Wait()

	indices := make(map[uint64]bool)
	for _, f := range futures {
		idx, err := f()
		require.NoError(t, err)
		require.False(t, indices[idx.Index], "duplicate index %d", idx.Index)
		indices[idx.Index] = true
	}

	require.Len(t, indices, 20)

	size, err := reader.IntegratedSize(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(20), size)
}

func TestQueue_FlushOnMaxAge(t *testing.T) {
	// Skip: Timer-based batching is disabled in Appender (maxAge forced to 0)
	// because it's incompatible with per-request delegations.
	t.Skip("timer-based batching disabled in Appender")

	ctx := context.Background()

	mockClient := NewMockClient()
	stateStore := newMockStateStore()
	driver, err := New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: stateStore,
		LogDID:     "did:key:test",
		Client:     mockClient,
	})
	require.NoError(t, err)

	storage := driver.(*Storage)
	signer := &dummySigner{}

	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(signer).
		WithBatching(100, 50*time.Millisecond)

	appender, reader, err := storage.Appender(ctx, opts)
	require.NoError(t, err)

	for i := 0; i < 3; i++ {
		entry := tessera.NewEntry([]byte{byte(i)})
		future := appender.Add(ctx, entry)
		idx, err := future()
		require.NoError(t, err)
		require.Equal(t, uint64(i), idx.Index)
	}

	time.Sleep(100 * time.Millisecond)

	size, err := reader.IntegratedSize(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), size)
}

func TestQueue_CloseDrainsPendingItemsWithError(t *testing.T) {
	ctx := context.Background()

	// flushFn blocks until released so items stay pending when Close() is called
	release := make(chan struct{})
	q := newEntryQueue(10, func(ctx context.Context, items []queueItem) error {
		<-release
		return nil
	})

	// Add one item to trigger a flush goroutine, then add more while it's blocked
	entry1 := tessera.NewEntry([]byte("a"))
	f1 := q.Add(ctx, entry1)

	// Wait briefly so doFlush goroutine is running and holding the flush
	time.Sleep(5 * time.Millisecond)

	// Add a second item that will sit in the pending queue
	entry2 := tessera.NewEntry([]byte("b"))
	f2 := q.Add(ctx, entry2)

	// Close should drain pending items with an error rather than leaving them blocked
	done := make(chan struct{})
	go func() {
		q.Close()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Close() hung — pending items not drained")
	}

	// Unblock the in-flight flush so it can complete
	close(release)

	// The second item (pending at close) must return an error, not hang
	_, err := f2()
	require.Error(t, err, "pending item should get error on close")

	// The first item (in-flight flush) eventually resolves (success or error)
	f1() //nolint:errcheck — we don't care about the in-flight result, just that it returns
}

func TestQueue_EmptyQueue(t *testing.T) {
	flushCalled := false
	q := newEntryQueue(10, func(ctx context.Context, items []queueItem) error {
		flushCalled = true
		return nil
	})

	q.Close()
	require.False(t, flushCalled)
}

func TestQueue_MaxSizeFlush(t *testing.T) {
	ctx := context.Background()

	mockClient := NewMockClient()
	stateStore := newMockStateStore()
	driver, err := New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: stateStore,
		LogDID:     "did:key:test",
		Client:     mockClient,
	})
	require.NoError(t, err)

	storage := driver.(*Storage)
	signer := &dummySigner{}

	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(signer).
		WithBatching(5, 10*time.Second)

	appender, reader, err := storage.Appender(ctx, opts)
	require.NoError(t, err)

	futures := make([]tessera.IndexFuture, 5)
	for i := 0; i < 5; i++ {
		entry := tessera.NewEntry([]byte{byte(i)})
		futures[i] = appender.Add(ctx, entry)
	}

	for i := 0; i < 5; i++ {
		idx, err := futures[i]()
		require.NoError(t, err)
		require.Equal(t, uint64(i), idx.Index)
	}

	size, err := reader.IntegratedSize(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(5), size)
}
