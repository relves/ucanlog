package storacha

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/relves/ucanlog/internal/storage/storacha/indexpersist"
	"github.com/relves/ucanlog/internal/storage/storacha/storachatest"
	"github.com/stretchr/testify/require"
	"github.com/transparency-dev/tessera"
)

func TestNew_RequiresSpaceDID(t *testing.T) {
	ctx := context.Background()
	_, err := New(ctx, Config{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "SpaceDID")
}

func TestNew_RequiresStateStore(t *testing.T) {
	ctx := context.Background()
	_, err := New(ctx, Config{
		SpaceDID: "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "StateStore")
}

func TestCIDIndex_PathOperations(t *testing.T) {
	idx := NewCIDIndex()

	idx.Set("tile/entries/000/001", "bafycid1")
	cid, ok := idx.Get("tile/entries/000/001")
	require.True(t, ok)
	require.Equal(t, "bafycid1", cid)

	_, ok = idx.Get("nonexistent")
	require.False(t, ok)

	idx.Set("tile/entries/000/001", "bafycid2")
	cid, _ = idx.Get("tile/entries/000/001")
	require.Equal(t, "bafycid2", cid)
}

func TestCIDIndex_Persistence(t *testing.T) {
	tmpDir := t.TempDir()
	indexPath := filepath.Join(tmpDir, "index.json")

	idx1 := NewCIDIndex()
	idx1.Set("checkpoint", "bafycheckpoint")
	idx1.Set("tile/0/000/000", "bafytile0")
	idx1.Set("tile/entries/000/000", "bafybundle0")

	data, err := idx1.MarshalJSON()
	require.NoError(t, err)
	err = os.WriteFile(indexPath, data, 0644)
	require.NoError(t, err)

	idx2 := NewCIDIndex()
	data, err = os.ReadFile(indexPath)
	require.NoError(t, err)
	err = idx2.UnmarshalJSON(data)
	require.NoError(t, err)

	cid, ok := idx2.Get("checkpoint")
	require.True(t, ok)
	require.Equal(t, "bafycheckpoint", cid)

	cid, ok = idx2.Get("tile/0/000/000")
	require.True(t, ok)
	require.Equal(t, "bafytile0", cid)
}

func TestCIDIndex_DeletePrefix(t *testing.T) {
	idx := NewCIDIndex()

	// Add paths - some partials and some complete bundles
	idx.Set("tile/entries/000/000", "bafybundle0")          // complete
	idx.Set("tile/entries/000/000.p/128", "bafypartial128") // partial
	idx.Set("tile/entries/000/000.p/200", "bafypartial200") // partial
	idx.Set("tile/entries/000/001", "bafybundle1")          // complete (different bundle)
	idx.Set("tile/0/000/000", "bafytile0")                  // tile
	idx.Set("tile/0/000/000.p/128", "bafytilepartial")      // tile partial

	require.Equal(t, 6, idx.Size())

	// Delete partials for entry bundle 0
	deleted := idx.DeletePrefix("tile/entries/000/000.p/")
	require.Equal(t, 2, deleted)
	require.Equal(t, 4, idx.Size())

	// Verify partials are gone
	_, ok := idx.Get("tile/entries/000/000.p/128")
	require.False(t, ok)
	_, ok = idx.Get("tile/entries/000/000.p/200")
	require.False(t, ok)

	// Verify complete bundle still exists
	cid, ok := idx.Get("tile/entries/000/000")
	require.True(t, ok)
	require.Equal(t, "bafybundle0", cid)

	// Verify other bundle and tile are untouched
	_, ok = idx.Get("tile/entries/000/001")
	require.True(t, ok)
	_, ok = idx.Get("tile/0/000/000")
	require.True(t, ok)
	_, ok = idx.Get("tile/0/000/000.p/128")
	require.True(t, ok)
}

func TestCIDIndex_DeletePrefix_NoMatches(t *testing.T) {
	idx := NewCIDIndex()
	idx.Set("tile/entries/000/000", "bafybundle0")

	deleted := idx.DeletePrefix("nonexistent/prefix/")
	require.Equal(t, 0, deleted)
	require.Equal(t, 1, idx.Size())
}

func TestStorage_Appender(t *testing.T) {
	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	stateStore := newMockStateStore()
	driver, err := New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: stateStore,
		LogDID:     "did:key:test",
		Client:     NewMockClient(),
	})
	require.NoError(t, err)

	storage := driver.(*Storage)
	opts := tessera.NewAppendOptions().WithCheckpointSigner(&dummySigner{})

	appender, reader, err := storage.Appender(ctx, opts)
	require.NoError(t, err)
	require.NotNil(t, appender)
	require.NotNil(t, reader)
}

type dummySigner struct{}

func (d *dummySigner) Name() string                { return "test" }
func (d *dummySigner) Sign([]byte) ([]byte, error) { return []byte("sig"), nil }
func (d *dummySigner) KeyHash() uint32             { return 0 }

func TestStorage_FullIntegration(t *testing.T) {
	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

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
	// Set maxSize to match numEntries so batch flushes immediately
	opts := tessera.NewAppendOptions().WithCheckpointSigner(signer).WithBatching(10, 0)

	appender, reader, err := storage.Appender(ctx, opts)
	require.NoError(t, err)
	require.NotNil(t, appender)
	require.NotNil(t, reader)

	numEntries := 10
	futures := make([]tessera.IndexFuture, numEntries)

	for i := 0; i < numEntries; i++ {
		entry := tessera.NewEntry([]byte(fmt.Sprintf("entry %d", i)))
		futures[i] = appender.Add(ctx, entry)
	}

	for i := 0; i < numEntries; i++ {
		idx, err := futures[i]()
		require.NoError(t, err)
		require.Equal(t, uint64(i), idx.Index)
	}

	size, err := reader.IntegratedSize(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(numEntries), size)

	bundleData, err := reader.ReadEntryBundle(ctx, 0, 10)
	require.NoError(t, err)
	require.NotEmpty(t, bundleData)

	tileData, err := reader.ReadTile(ctx, 0, 0, 10)
	require.NoError(t, err)
	require.NotEmpty(t, tileData)
}

func TestStorage_FullBundle(t *testing.T) {
	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

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
	opts := tessera.NewAppendOptions().WithCheckpointSigner(signer)

	appender, reader, err := storage.Appender(ctx, opts)
	require.NoError(t, err)

	numEntries := 256
	futures := make([]tessera.IndexFuture, numEntries)

	for i := 0; i < numEntries; i++ {
		entry := tessera.NewEntry([]byte{byte(i)})
		futures[i] = appender.Add(ctx, entry)
	}

	for i := 0; i < numEntries; i++ {
		idx, err := futures[i]()
		require.NoError(t, err)
		require.Equal(t, uint64(i), idx.Index)
	}

	bundleData, err := reader.ReadEntryBundle(ctx, 0, 0)
	require.NoError(t, err)
	require.NotEmpty(t, bundleData)

	tileData, err := reader.ReadTile(ctx, 0, 0, 0)
	require.NoError(t, err)
	require.NotEmpty(t, tileData)
}

func TestStorage_ConcurrentAdds(t *testing.T) {
	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

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
	// Set maxSize to match numEntries so batch flushes immediately
	opts := tessera.NewAppendOptions().WithCheckpointSigner(signer).WithBatching(20, 0)

	appender, reader, err := storage.Appender(ctx, opts)
	require.NoError(t, err)

	numEntries := 20
	futures := make([]tessera.IndexFuture, numEntries)

	for i := 0; i < numEntries; i++ {
		entry := tessera.NewEntry([]byte{byte(i)})
		futures[i] = appender.Add(ctx, entry)
	}

	indices := make(map[uint64]bool)
	for i := 0; i < numEntries; i++ {
		idx, err := futures[i]()
		require.NoError(t, err)
		require.False(t, indices[idx.Index], "duplicate index %d", idx.Index)
		indices[idx.Index] = true
	}

	require.Len(t, indices, numEntries)

	size, err := reader.IntegratedSize(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(numEntries), size)
}

func TestStorage_IndexPersistenceIntegration(t *testing.T) {
	// This test verifies the wiring, not the actual upload
	stateStore := newMockStateStore()

	cfg := Config{
		SpaceDID:   "did:key:test",
		StateStore: stateStore,
		LogDID:     "did:key:testlog",
		Client:     NewMockClient(),
		IndexPersistence: &indexpersist.Config{
			Interval: 100 * time.Millisecond,
		},
	}

	ctx := context.Background()
	storage, err := New(ctx, cfg)
	require.NoError(t, err)
	require.NotNil(t, storage)

	// Metadata is now persisted to StateStore instead of files
}
