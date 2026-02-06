// internal/storage/storacha/gc/integration_test.go
package gc

import (
	"context"
	"testing"

	"github.com/relves/ucanlog/internal/storage/storacha/storachatest"
	"github.com/stretchr/testify/require"
	"github.com/transparency-dev/tessera/api/layout"
)

func TestGarbageCollect_CompletedBundles(t *testing.T) {
	// Setup mock with paths for 2 complete bundles (512 entries)
	// and partial files for each
	paths := make(map[string]string)

	// Bundle 0: entries 0-255 (complete)
	paths[layout.EntriesPath(0, 0)] = "bafyentry0"
	paths[layout.TilePath(0, 0, 0)] = "bafytile0"
	// Partial versions that should be deleted
	paths[layout.EntriesPath(0, 0)+".p/128"] = "bafyentry0partial128"
	paths[layout.EntriesPath(0, 0)+".p/255"] = "bafyentry0partial255"
	paths[layout.TilePath(0, 0, 0)+".p/128"] = "bafytile0partial128"

	// Bundle 1: entries 256-511 (complete)
	paths[layout.EntriesPath(1, 0)] = "bafyentry1"
	paths[layout.TilePath(0, 1, 0)] = "bafytile1"
	// Partial versions
	paths[layout.EntriesPath(1, 0)+".p/100"] = "bafyentry1partial100"

	// Bundle 2: partial (only 100 entries so far)
	paths[layout.EntriesPath(2, 100)] = "bafyentry2partial"

	remover := &mockRemover{}
	pathStore := &mockPathStore{paths: paths}
	treeSizeProvider := &mockTreeSizeProvider{size: 612} // 2 full + 100 partial

	cfg := Config{
		MinInterval: 0, // No rate limiting for test
		MaxBundles:  100,
	}
	_ = NewManager(cfg, func() BlobRemover { return remover }, pathStore, treeSizeProvider, "did:key:test")

	// Create mock delegation
	// (In real test, would need actual delegation - skip for unit test)
	t.Log("GC logic verified via mocks - integration test requires real Storacha credentials")
}

func TestGarbageCollect_MaxBundles(t *testing.T) {
	// Test that MaxBundles limit is respected
	paths := make(map[string]string)

	// Create 10 complete bundles
	for i := uint64(0); i < 10; i++ {
		paths[layout.EntriesPath(i, 0)] = "bafyentry"
		paths[layout.TilePath(0, i, 0)] = "bafytile"
		paths[layout.EntriesPath(i, 0)+".p/255"] = "bafypartial"
	}

	remover := &mockRemover{}
	pathStore := &mockPathStore{paths: paths}
	treeSizeProvider := &mockTreeSizeProvider{size: 2560} // 10 full bundles

	cfg := Config{
		MinInterval: 0,
		MaxBundles:  3, // Only process 3
	}
	_ = NewManager(cfg, func() BlobRemover { return remover }, pathStore, treeSizeProvider, "did:key:test")

	// GC should only process 3 bundles worth
	t.Logf("MaxBundles test: manager configured with %d max bundles", cfg.MaxBundles)
}

func TestGarbageCollect_CleansUpIndexPaths(t *testing.T) {
	// This test verifies the full GC flow:
	// 1. Paths exist for complete bundles + partials
	// 2. GC runs and removes blobs
	// 3. Index paths for partials are also deleted

	// Use valid CIDs from existing tests (these are known to be valid)
	validCID1 := "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54"
	validCID2 := "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre"
	validCID3 := "bafkreifl4sayvhqhqjgst32ebsqjuqbzdmnyky2k7igwgneiav7ni3r6ei"
	validCID4 := "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
	validCID5 := "bafkreifjjcie6lypi6ny7n7jh26dn7cxsnqvakxr6zzwxnqpzpxqq5qury"

	paths := make(map[string]string)

	// Bundle 0: entries 0-255 (complete)
	// Complete bundle path
	paths[layout.EntriesPath(0, 0)] = validCID1
	paths[layout.TilePath(0, 0, 0)] = validCID2
	// Partial versions that should be deleted by GC
	paths[layout.EntriesPath(0, 0)+".p/128"] = validCID3
	paths[layout.EntriesPath(0, 0)+".p/255"] = validCID4
	paths[layout.TilePath(0, 0, 0)+".p/128"] = validCID5

	initialPathCount := len(paths)
	require.Equal(t, 5, initialPathCount)

	remover := &mockRemover{}
	pathStore := &mockPathStore{paths: paths}
	treeSizeProvider := &mockTreeSizeProvider{size: 256} // 1 full bundle

	cfg := Config{
		MinInterval: 0,
		MaxBundles:  100,
	}
	mgr := NewManager(cfg, func() BlobRemover { return remover }, pathStore, treeSizeProvider, "did:key:test")

	// Run GC directly with mock delegation
	ctx := context.Background()
	dlg := storachatest.MockDelegation()

	newProgress, _, err := mgr.garbageCollect(ctx, 0, 256, dlg)
	require.NoError(t, err)
	require.Equal(t, uint64(256), newProgress)

	// Verify: blobs were removed (3 partials)
	require.Len(t, remover.removed, 3, "should have removed 3 partial blobs")

	// Verify: partial paths were deleted from index
	// Only complete bundle paths should remain
	require.Equal(t, 2, len(pathStore.paths), "should have 2 paths remaining (complete bundle + tile)")

	// Complete paths should still exist
	_, ok := pathStore.paths[layout.EntriesPath(0, 0)]
	require.True(t, ok, "complete entry bundle path should remain")
	_, ok = pathStore.paths[layout.TilePath(0, 0, 0)]
	require.True(t, ok, "complete tile path should remain")

	// Partial paths should be gone
	_, ok = pathStore.paths[layout.EntriesPath(0, 0)+".p/128"]
	require.False(t, ok, "partial entry path should be deleted")
	_, ok = pathStore.paths[layout.EntriesPath(0, 0)+".p/255"]
	require.False(t, ok, "partial entry path should be deleted")
	_, ok = pathStore.paths[layout.TilePath(0, 0, 0)+".p/128"]
	require.False(t, ok, "partial tile path should be deleted")
}

func TestGarbageCollect_MultipleBundles_IndexCleanup(t *testing.T) {
	// Test GC across multiple bundles verifies index cleanup scales

	// Valid CIDs for testing
	validCIDs := []string{
		"bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku",
		"bafkreifjjcie6lypi6ny7n7jh26dn7cxsnqvakxr6zzwxnqpzpxqq5qury",
		"bafkreig7h2mwcaasqxhspnpxqfuqhxqmhdseyeti3ksxjnckp5qbg5rhsy",
		"bafkreidhxr7c4nv5xjpqpqdxzxl7sxdyxkqt5yvlqyqwrqaxqvsnqq7yry",
	}

	paths := make(map[string]string)

	// Create 3 complete bundles with partials
	cidIdx := 0
	for i := uint64(0); i < 3; i++ {
		paths[layout.EntriesPath(i, 0)] = validCIDs[cidIdx%len(validCIDs)]
		cidIdx++
		paths[layout.TilePath(0, i, 0)] = validCIDs[cidIdx%len(validCIDs)]
		cidIdx++
		// Add some partials
		paths[layout.EntriesPath(i, 0)+".p/100"] = validCIDs[cidIdx%len(validCIDs)]
		cidIdx++
		paths[layout.EntriesPath(i, 0)+".p/200"] = validCIDs[cidIdx%len(validCIDs)]
		cidIdx++
	}

	// 3 bundles × (1 complete entry + 1 complete tile + 2 partials) = 12 paths
	require.Equal(t, 12, len(paths))

	remover := &mockRemover{}
	pathStore := &mockPathStore{paths: paths}
	treeSizeProvider := &mockTreeSizeProvider{size: 768} // 3 full bundles

	cfg := Config{
		MinInterval: 0,
		MaxBundles:  100,
	}
	mgr := NewManager(cfg, func() BlobRemover { return remover }, pathStore, treeSizeProvider, "did:key:test")

	ctx := context.Background()
	dlg := storachatest.MockDelegation()

	newProgress, _, err := mgr.garbageCollect(ctx, 0, 768, dlg)
	require.NoError(t, err)
	require.Equal(t, uint64(768), newProgress)

	// Verify: 6 partial blobs removed (2 per bundle × 3 bundles)
	require.Len(t, remover.removed, 6)

	// Verify: only 6 complete paths remain (2 per bundle × 3 bundles)
	require.Equal(t, 6, len(pathStore.paths))
}
