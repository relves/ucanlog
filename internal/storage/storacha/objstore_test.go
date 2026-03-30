package storacha

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"sync"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	"github.com/stretchr/testify/require"
)

func (s *objStore) deleteObjectsWithPrefix(ctx context.Context, prefix string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for path := range s.stateMap {
		if len(path) >= len(prefix) && path[:len(prefix)] == prefix {
			delete(s.stateMap, path)
		}
	}
	return nil
}

func makeTestCID(data []byte) cid.Cid {
	hash := sha256.Sum256(data)
	mh, _ := multihash.Encode(hash[:], multihash.SHA2_256)
	return cid.NewCidV1(cid.Raw, mh)
}

func TestObjStore_SetAndGet(t *testing.T) {
	store := newObjStore(slog.Default())
	ctx := context.Background()

	path := "tile/entries/000/001"
	data := []byte("bundle data here")

	err := store.setObject(ctx, path, data)
	require.NoError(t, err)

	retrieved, err := store.getObject(ctx, path)
	require.NoError(t, err)
	require.Equal(t, data, retrieved)
}

func TestObjStore_GetNotFound(t *testing.T) {
	store := newObjStore(slog.Default())
	ctx := context.Background()

	_, err := store.getObject(ctx, "nonexistent/path")
	require.Error(t, err)
}

func TestObjStore_SetObjectIfNoneMatch(t *testing.T) {
	store := newObjStore(slog.Default())
	ctx := context.Background()

	path := "tile/0/000/000"
	data1 := []byte("tile data v1")
	data2 := []byte("tile data v2")

	written, err := store.setObjectIfNoneMatch(ctx, path, data1)
	require.NoError(t, err)
	require.True(t, written)

	written, err = store.setObjectIfNoneMatch(ctx, path, data2)
	require.NoError(t, err)
	require.False(t, written)

	retrieved, err := store.getObject(ctx, path)
	require.NoError(t, err)
	require.Equal(t, data1, retrieved)
}

func TestObjStore_DeleteWithPrefix(t *testing.T) {
	store := newObjStore(slog.Default())
	ctx := context.Background()

	require.NoError(t, store.setObject(ctx, "tile/entries/000/001.p/128", []byte("partial1")))
	require.NoError(t, store.setObject(ctx, "tile/entries/000/001.p/200", []byte("partial2")))
	require.NoError(t, store.setObject(ctx, "tile/entries/000/002", []byte("other")))

	err := store.deleteObjectsWithPrefix(ctx, "tile/entries/000/001.p/")
	require.NoError(t, err)

	_, err = store.getObject(ctx, "tile/entries/000/001.p/128")
	require.Error(t, err)
	_, err = store.getObject(ctx, "tile/entries/000/001.p/200")
	require.Error(t, err)

	v, err := store.getObject(ctx, "tile/entries/000/002")
	require.NoError(t, err)
	require.Equal(t, []byte("other"), v)
}

func TestObjStore_SnapshotAndLoad(t *testing.T) {
	store := newObjStore(slog.Default())
	ctx := context.Background()

	require.NoError(t, store.setObject(ctx, "checkpoint", []byte("cp")))
	require.NoError(t, store.setObject(ctx, "tile/0/000/000", []byte("tile")))

	snapshot := store.Snapshot()
	snapshot["checkpoint"][0] = 'X'
	snapshot["new"] = []byte("new")

	cp, err := store.getObject(ctx, "checkpoint")
	require.NoError(t, err)
	require.Equal(t, []byte("cp"), cp)
	_, err = store.getObject(ctx, "new")
	require.Error(t, err)

	newStore := newObjStore(slog.Default())
	newStore.Load(snapshot)

	loadedCP, err := newStore.getObject(ctx, "checkpoint")
	require.NoError(t, err)
	require.Equal(t, []byte("Xp"), loadedCP)
}

func TestObjStore_ConcurrentWrites(t *testing.T) {
	store := newObjStore(slog.Default())
	ctx := context.Background()

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(i int) {
			defer wg.Done()
			_ = store.setObject(ctx, "tile/concurrent/"+string(rune('a'+i)), []byte{byte(i)})
		}(i)
	}
	wg.Wait()

	snap := store.Snapshot()
	require.Len(t, snap, workers)
}

func TestObjStore_FinalizedCIDs(t *testing.T) {
	store := newObjStore(slog.Default())
	ctx := context.Background()

	// No finalized CIDs initially
	got := store.GetFinalizedCIDs()
	require.Empty(t, got)

	// Set regular data first
	require.NoError(t, store.setObject(ctx, "tile/entries/000", []byte("bundle data")))

	// Finalize it
	testCID := makeTestCID([]byte("bundle data"))
	store.SetFinalizedCID("tile/entries/000", testCID)

	// Should appear in finalized map
	got = store.GetFinalizedCIDs()
	require.Len(t, got, 1)
	require.Equal(t, testCID, got["tile/entries/000"])

	// Should NOT appear in Snapshot
	snap := store.Snapshot()
	_, ok := snap["tile/entries/000"]
	require.False(t, ok, "finalized bundle should not appear in Snapshot")
}

func TestObjStore_FinalizedCIDs_BlocksWrite(t *testing.T) {
	store := newObjStore(slog.Default())
	ctx := context.Background()

	testCID := makeTestCID([]byte("bundle data"))
	store.SetFinalizedCID("tile/entries/000", testCID)

	// setObjectIfNoneMatch should treat finalized path as "already exists"
	written, err := store.setObjectIfNoneMatch(ctx, "tile/entries/000", []byte("should not write"))
	require.NoError(t, err)
	require.False(t, written)

	// Path still not in stateMap
	snap := store.Snapshot()
	_, ok := snap["tile/entries/000"]
	require.False(t, ok)
}

func TestObjStore_LoadFinalizedCIDs(t *testing.T) {
	store := newObjStore(slog.Default())

	cids := map[string]cid.Cid{
		"tile/entries/000": makeTestCID([]byte("a")),
		"tile/entries/001": makeTestCID([]byte("b")),
	}
	store.LoadFinalizedCIDs(cids)

	got := store.GetFinalizedCIDs()
	require.Len(t, got, 2)
	require.Equal(t, cids["tile/entries/000"], got["tile/entries/000"])
	require.Equal(t, cids["tile/entries/001"], got["tile/entries/001"])
}
