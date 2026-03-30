package storacha

import (
	"context"
	"log/slog"
	"testing"

	"github.com/transparency-dev/tessera/api/layout"

	"github.com/stretchr/testify/require"
)

func TestIsFullBundlePath(t *testing.T) {
	tests := []struct {
		path string
		full bool
	}{
		{layout.EntriesPath(0, 0), true},
		{layout.EntriesPath(1, 0), true},
		{layout.EntriesPath(0, 50), false},
		{layout.EntriesPath(0, 255), false},
		{layout.TilePath(0, 0, 0), false},
		{layout.TilePath(0, 0, 8), false},
		{"checkpoint", false},
		{"_manifest.json", false},
	}
	for _, tc := range tests {
		got := isFullBundlePath(tc.path)
		if got != tc.full {
			t.Errorf("isFullBundlePath(%q) = %v, want %v", tc.path, got, tc.full)
		}
	}
}

func TestFinalizeCompleteBundles(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()
	store := newObjStore(logger)
	client := NewMockClient()

	// Full bundle at index 0 (partial=0)
	fullPath := layout.EntriesPath(0, 0)
	require.NoError(t, store.setObject(ctx, fullPath, []byte("full bundle 256 entries")))

	// Partial bundle at index 1 (partial=50) — should NOT be finalized
	partialPath := layout.EntriesPath(1, 50)
	require.NoError(t, store.setObject(ctx, partialPath, []byte("partial bundle")))

	// Tile — should NOT be finalized
	tilePath := layout.TilePath(0, 0, 0)
	require.NoError(t, store.setObject(ctx, tilePath, []byte("tile data")))

	// Checkpoint — should NOT be finalized
	require.NoError(t, store.setObject(ctx, "checkpoint", []byte("cp")))

	err := finalizeCompleteBundles(ctx, store, client, logger)
	require.NoError(t, err)

	// Full bundle should be finalized
	finalized := store.GetFinalizedCIDs()
	require.Len(t, finalized, 1)
	_, ok := finalized[fullPath]
	require.True(t, ok, "full bundle should be finalized")

	// Full bundle removed from stateMap
	snap := store.Snapshot()
	_, ok = snap[fullPath]
	require.False(t, ok, "full bundle should not be in snapshot after finalization")

	// Partial bundle still in stateMap
	_, ok = snap[partialPath]
	require.True(t, ok, "partial bundle should remain in snapshot")

	// Tile still in stateMap
	_, ok = snap[tilePath]
	require.True(t, ok, "tile should remain in snapshot")

	// Checkpoint still in stateMap
	_, ok = snap["checkpoint"]
	require.True(t, ok, "checkpoint should remain in snapshot")
}

func TestFinalizeCompleteBundles_AlreadyFinalized(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()
	store := newObjStore(logger)
	client := NewMockClient()

	// Manually finalize a path — simulates post-cold-start state
	c := makeTestCID([]byte("already uploaded"))
	store.SetFinalizedCID(layout.EntriesPath(0, 0), c)

	// Run finalizeCompleteBundles — nothing in stateMap so nothing to do
	err := finalizeCompleteBundles(ctx, store, client, logger)
	require.NoError(t, err)

	finalized := store.GetFinalizedCIDs()
	require.Len(t, finalized, 1)
	require.Equal(t, c, finalized[layout.EntriesPath(0, 0)])
}
