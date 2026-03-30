package storacha

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
)

func TestIntegrateEntries_ParallelExecution(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	entries := make([]SequencedEntry, 5)
	for i := 0; i < 5; i++ {
		data := []byte{byte(i)}
		entries[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}

	root, err := integrateEntries(ctx, 0, entries, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root)
	require.Len(t, root, 32)

	_, err = objStore.getObject(ctx, layout.EntriesPath(0, 5))
	require.NoError(t, err, "entry bundle should exist")

	_, err = objStore.getObject(ctx, layout.TilePath(0, 0, 5))
	require.NoError(t, err, "hash tile should exist")
}

func TestIntegrateEntries_LargeIntegration(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	entries := make([]SequencedEntry, 300)
	for i := 0; i < 300; i++ {
		data := []byte{byte(i % 256), byte(i / 256)}
		entries[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}

	root, err := integrateEntries(ctx, 0, entries, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root)

	_, err = objStore.getObject(ctx, layout.EntriesPath(0, 0))
	require.NoError(t, err, "first entry bundle should be full")

	_, err = objStore.getObject(ctx, layout.EntriesPath(1, 44))
	require.NoError(t, err, "second entry bundle should be partial")

	_, err = objStore.getObject(ctx, layout.TilePath(0, 0, 0))
	require.NoError(t, err, "first hash tile should be full")
}

func TestIntegrateEntries_EmptyEntries(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	root, err := integrateEntries(ctx, 0, []SequencedEntry{}, lrs, slog.Default())
	require.NoError(t, err)

	require.Equal(t, rfc6962.DefaultHasher.EmptyRoot(), root)
}

func TestIntegrateEntries_ExtendExisting(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	entries1 := make([]SequencedEntry, 10)
	for i := 0; i < 10; i++ {
		data := []byte{byte(i)}
		entries1[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}

	root1, err := integrateEntries(ctx, 0, entries1, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root1)

	entries2 := make([]SequencedEntry, 5)
	for i := 0; i < 5; i++ {
		data := []byte{byte(10 + i)}
		entries2[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}

	root2, err := integrateEntries(ctx, 10, entries2, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root2)

	require.NotEqual(t, root1, root2, "roots should be different after extending")

	_, err = objStore.getObject(ctx, layout.EntriesPath(0, 10))
	require.NoError(t, err, "first bundle should exist")

	_, err = objStore.getObject(ctx, layout.EntriesPath(0, 15))
	require.NoError(t, err, "extended first bundle should exist")
}

func TestHybridCAR_IntegrationCycle(t *testing.T) {
	ctx := context.Background()
	logger := slog.Default()
	client := NewMockClient()
	objStore := newObjStore(logger)
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	// Integrate 300 entries: bundle 0 = 256 entries (full), bundle 1 = 44 entries (partial)
	entries := make([]SequencedEntry, 300)
	for i := range entries {
		data := []byte{byte(i), byte(i >> 8), byte(i >> 16)}
		entries[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}
	_, err := integrateEntries(ctx, 0, entries, lrs, logger)
	require.NoError(t, err)

	// Add checkpoint
	require.NoError(t, lrs.setCheckpoint(ctx, []byte("checkpoint")))

	fullBundlePath := layout.EntriesPath(0, 0)
	partialBundlePath := layout.EntriesPath(1, 44)

	// Verify full bundle exists before finalization
	snapBefore := objStore.Snapshot()
	require.Contains(t, snapBefore, fullBundlePath, "full bundle should be in snapshot before finalization")
	require.Contains(t, snapBefore, partialBundlePath)

	// Finalize complete bundles
	require.NoError(t, finalizeCompleteBundles(ctx, objStore, client, logger))

	finalized := objStore.GetFinalizedCIDs()
	require.Len(t, finalized, 1)
	_, ok := finalized[fullBundlePath]
	require.True(t, ok, "bundle 0 should be finalized")

	// Snapshot should exclude finalized bundle
	snap := objStore.Snapshot()
	_, ok = snap[fullBundlePath]
	require.False(t, ok, "finalized bundle should be excluded from snapshot")
	require.Contains(t, snap, partialBundlePath)
	require.Contains(t, snap, "checkpoint")

	// Build hybrid CAR
	linkedCIDs := objStore.GetFinalizedCIDs()
	carData, rootCID, err := BuildHybridCAR(ctx, snap, linkedCIDs)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, rootCID)

	// Cold start: parse CAR, extract manifest, load into fresh store
	stateMap, err := parseStateCAR(ctx, bytes.NewReader(carData))
	require.NoError(t, err)

	restoredCIDs, err := extractManifest(stateMap)
	require.NoError(t, err)
	require.Len(t, restoredCIDs, len(finalized), "manifest should have same count as finalized")

	for path, c := range finalized {
		require.Equal(t, c, restoredCIDs[path], "manifest CID mismatch for %s", path)
	}

	freshStore := newObjStore(logger)
	freshStore.Load(stateMap)
	freshStore.LoadFinalizedCIDs(restoredCIDs)

	// All embedded paths from snapshot should be restored
	freshSnap := freshStore.Snapshot()
	for path, data := range snap {
		require.Contains(t, freshSnap, path)
		require.Equal(t, data, freshSnap[path], "data mismatch for %s", path)
	}

	// Finalized CIDs should be restored
	freshFinalized := freshStore.GetFinalizedCIDs()
	require.Equal(t, len(finalized), len(freshFinalized))

	t.Logf("cycle OK: %d embedded, %d finalized bundles, CAR %d bytes",
		len(freshSnap), len(freshFinalized), len(carData))
}
