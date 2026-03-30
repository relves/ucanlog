package storacha

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
)

func marshalBundleEntry(data []byte) []byte {
	buf := make([]byte, 2+len(data))
	buf[0] = byte(len(data) >> 8)
	buf[1] = byte(len(data))
	copy(buf[2:], data)
	return buf
}

func TestUpdateEntryBundles_FirstEntries(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	entries := []SequencedEntry{
		{BundleData: marshalBundleEntry([]byte("entry 0")), LeafHash: rfc6962.DefaultHasher.HashLeaf([]byte("entry 0"))},
		{BundleData: marshalBundleEntry([]byte("entry 1")), LeafHash: rfc6962.DefaultHasher.HashLeaf([]byte("entry 1"))},
		{BundleData: marshalBundleEntry([]byte("entry 2")), LeafHash: rfc6962.DefaultHasher.HashLeaf([]byte("entry 2"))},
	}

	err := updateEntryBundles(ctx, 0, entries, lrs)
	require.NoError(t, err)

	bundlePath := layout.EntriesPath(0, 3)
	_, err = objStore.getObject(ctx, bundlePath)
	require.NoError(t, err, "partial bundle should be stored at %s", bundlePath)
}

func TestUpdateEntryBundles_FullBundle(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	entries := make([]SequencedEntry, 256)
	for i := 0; i < 256; i++ {
		data := []byte{byte(i)}
		entries[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}

	err := updateEntryBundles(ctx, 0, entries, lrs)
	require.NoError(t, err)

	bundlePath := layout.EntriesPath(0, 0)
	_, err = objStore.getObject(ctx, bundlePath)
	require.NoError(t, err, "full bundle should be stored at %s", bundlePath)
}

func TestUpdateEntryBundles_ExtendPartial(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	entries1 := []SequencedEntry{
		{BundleData: marshalBundleEntry([]byte("entry 0")), LeafHash: rfc6962.DefaultHasher.HashLeaf([]byte("entry 0"))},
		{BundleData: marshalBundleEntry([]byte("entry 1")), LeafHash: rfc6962.DefaultHasher.HashLeaf([]byte("entry 1"))},
	}
	err := updateEntryBundles(ctx, 0, entries1, lrs)
	require.NoError(t, err)

	entries2 := []SequencedEntry{
		{BundleData: marshalBundleEntry([]byte("entry 2")), LeafHash: rfc6962.DefaultHasher.HashLeaf([]byte("entry 2"))},
		{BundleData: marshalBundleEntry([]byte("entry 3")), LeafHash: rfc6962.DefaultHasher.HashLeaf([]byte("entry 3"))},
		{BundleData: marshalBundleEntry([]byte("entry 4")), LeafHash: rfc6962.DefaultHasher.HashLeaf([]byte("entry 4"))},
	}
	err = updateEntryBundles(ctx, 2, entries2, lrs)
	require.NoError(t, err)

	bundlePath := layout.EntriesPath(0, 5)
	_, err = objStore.getObject(ctx, bundlePath)
	require.NoError(t, err, "extended bundle should be stored at %s", bundlePath)
}

func TestUpdateEntryBundles_CrossBundleBoundary(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	entries1 := make([]SequencedEntry, 250)
	for i := 0; i < 250; i++ {
		data := []byte{byte(i)}
		entries1[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}
	err := updateEntryBundles(ctx, 0, entries1, lrs)
	require.NoError(t, err)

	entries2 := make([]SequencedEntry, 10)
	for i := 0; i < 10; i++ {
		data := []byte{byte(250 + i)}
		entries2[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}
	err = updateEntryBundles(ctx, 250, entries2, lrs)
	require.NoError(t, err)

	_, err = objStore.getObject(ctx, layout.EntriesPath(0, 0))
	require.NoError(t, err, "first bundle should be full")

	_, err = objStore.getObject(ctx, layout.EntriesPath(1, 4))
	require.NoError(t, err, "second bundle should have 4 entries")
}

func TestMarshalBundleEntry(t *testing.T) {
	data := []byte("test entry")
	result := marshalBundleEntry(data)

	require.Equal(t, byte(len(data)>>8), result[0])
	require.Equal(t, byte(len(data)), result[1])
	require.Equal(t, data, result[2:])
}

func TestMarshalBundleEntry_Empty(t *testing.T) {
	data := []byte{}
	result := marshalBundleEntry(data)

	require.Equal(t, byte(0), result[0])
	require.Equal(t, byte(0), result[1])
	require.Equal(t, 2, len(result))
}

func TestMarshalBundleEntry_Large(t *testing.T) {
	data := bytes.Repeat([]byte{0xAB}, 255)
	result := marshalBundleEntry(data)

	require.Equal(t, byte(255>>8), result[0])
	require.Equal(t, byte(255), result[1])
	require.Equal(t, data, result[2:])
}

// TestUpdateEntryBundles_MultipleBundles verifies that multiple full bundles
// don't accumulate previous bundle data (regression test for buffer reuse bug).
func TestUpdateEntryBundles_MultipleBundles(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	// Add 3 full bundles (256 entries each = 768 total)
	totalEntries := 768
	entries := make([]SequencedEntry, totalEntries)
	for i := 0; i < totalEntries; i++ {
		// Use unique data for each entry to ensure bundles have different CIDs
		data := []byte{byte(i % 256), byte(i / 256)}
		entries[i] = SequencedEntry{
			BundleData: marshalBundleEntry(data),
			LeafHash:   rfc6962.DefaultHasher.HashLeaf(data),
		}
	}

	err := updateEntryBundles(ctx, 0, entries, lrs)
	require.NoError(t, err)

	// Verify all 3 bundles exist
	bundle0Path := layout.EntriesPath(0, 0)
	bundle1Path := layout.EntriesPath(1, 0)
	bundle2Path := layout.EntriesPath(2, 0)

	bundle0Data, err := objStore.getObject(ctx, bundle0Path)
	require.NoError(t, err, "bundle 0 should exist")
	bundle1Data, err := objStore.getObject(ctx, bundle1Path)
	require.NoError(t, err, "bundle 1 should exist")
	bundle2Data, err := objStore.getObject(ctx, bundle2Path)
	require.NoError(t, err, "bundle 2 should exist")

	// Calculate expected size: 256 entries * (2 byte length + 2 byte data)
	expectedBundleSize := 256 * 4

	// Verify that each bundle is the correct size (not accumulating previous data)
	require.Equal(t, expectedBundleSize, len(bundle0Data), "bundle 0 size should be %d bytes", expectedBundleSize)
	require.Equal(t, expectedBundleSize, len(bundle1Data), "bundle 1 size should be %d bytes (not %d+%d)", expectedBundleSize, expectedBundleSize, expectedBundleSize)
	require.Equal(t, expectedBundleSize, len(bundle2Data), "bundle 2 size should be %d bytes (not %d+%d+%d)", expectedBundleSize, expectedBundleSize, expectedBundleSize, expectedBundleSize)

	// Verify bundles contain different bytes (different entries)
	require.NotEqual(t, bundle0Data, bundle1Data, "bundle 0 and 1 should differ")
	require.NotEqual(t, bundle1Data, bundle2Data, "bundle 1 and 2 should differ")
	require.NotEqual(t, bundle0Data, bundle2Data, "bundle 0 and 2 should differ")
}
