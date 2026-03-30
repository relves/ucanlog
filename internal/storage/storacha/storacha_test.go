package storacha

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ipfs/go-cid"
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
	ctx := context.Background()

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


func TestColdStart_RestoresManifest(t *testing.T) {
	ctx := context.Background()

	embedded := map[string][]byte{
		"checkpoint":      []byte("cp data"),
		"tile/0/x000/000": []byte("tile"),
	}
	bundleData := []byte("finalized bundle")
	_, mh, err := ComputeCID(bundleData)
	require.NoError(t, err)
	c := cid.NewCidV1(cid.Raw, mh)
	linked := map[string]cid.Cid{
		"tile/entries/000": c,
	}

	carData, _, err := BuildHybridCAR(ctx, embedded, linked)
	require.NoError(t, err)

	// Simulate cold start parsing
	stateMap, err := parseStateCAR(ctx, bytes.NewReader(carData))
	require.NoError(t, err)

	finalizedCIDs, err := extractManifest(stateMap)
	require.NoError(t, err)
	require.Len(t, finalizedCIDs, 1)
	require.Equal(t, c, finalizedCIDs["tile/entries/000"])

	// _manifest.json should be removed from stateMap
	_, ok := stateMap["_manifest.json"]
	require.False(t, ok, "_manifest.json should be removed from stateMap")

	// Load into fresh objStore
	freshStore := newObjStore(slog.Default())
	freshStore.Load(stateMap)
	freshStore.LoadFinalizedCIDs(finalizedCIDs)

	// Embedded state restored
	snap := freshStore.Snapshot()
	require.Contains(t, snap, "checkpoint")
	require.Contains(t, snap, "tile/0/x000/000")

	// Finalized CIDs restored
	got := freshStore.GetFinalizedCIDs()
	require.Equal(t, c, got["tile/entries/000"])
}

func TestExtractManifest_NoManifest(t *testing.T) {
	stateMap := map[string][]byte{
		"checkpoint": []byte("cp"),
	}
	result, err := extractManifest(stateMap)
	require.NoError(t, err)
	require.Nil(t, result)
	// stateMap unchanged
	require.Contains(t, stateMap, "checkpoint")
}

// TestColdStart_GatewayFallback_RestoresTreeState verifies that when the
// latest_head_car row is lost (so the local CAR cache is unavailable) but the
// headCID is still known via index_persistence, and the gateway can serve the
// CAR, storacha.New() correctly restores tree state from the checkpoint embedded
// in that CAR.
//
// Note: this is a *partial* DB loss scenario — the headCID must survive somewhere
// in the DB (latest_head_car or index_persistence) for the gateway fallback to be
// triggered at all.  True total DB wipeout would leave GetHead returning "" and
// the gateway path would never be entered.
//
// This test documents the bug: after gateway recovery, storacha.New() restores
// the objStore in-memory state but never calls SetTreeState, so GetTreeState
// still returns (0, nil) — the coordinator then assigns index 0 to the next
// entry, silently forking the log.
//
// The test asserts the fix: SetTreeState must be called during gateway recovery
// so that GetTreeState returns the correct size (numFirstBatch) after New().
func TestColdStart_GatewayFallback_RestoresTreeState(t *testing.T) {
	ctx := context.Background()
	const logDID = "did:key:test-recovery"
	const numFirstBatch = 10

	// ── Phase 1: write numFirstBatch entries with a "healthy" state store ────
	firstStore := newMockStateStore()
	mockClient := NewMockClient()

	driver1, err := New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: firstStore,
		LogDID:     logDID,
		Client:     mockClient,
	})
	require.NoError(t, err)

	st1 := driver1.(*Storage)
	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(&dummySigner{}).
		WithBatching(numFirstBatch, 0)

	appender1, _, err := st1.Appender(ctx, opts)
	require.NoError(t, err)

	futures := make([]tessera.IndexFuture, numFirstBatch)
	for i := 0; i < numFirstBatch; i++ {
		futures[i] = appender1.Add(ctx, tessera.NewEntry([]byte(fmt.Sprintf("entry-%d", i))))
	}
	for i := 0; i < numFirstBatch; i++ {
		idxResult, addErr := futures[i]()
		require.NoError(t, addErr)
		require.Equal(t, uint64(i), idxResult.Index, "first batch index mismatch")
	}

	// Capture the CAR that was stored in the "healthy" DB.
	headCID, _, carData, err := firstStore.GetLatestHeadCAR(ctx, logDID)
	require.NoError(t, err)
	require.NotEmpty(t, headCID, "no head CAR stored after first batch")
	require.NotEmpty(t, carData)

	// ── Phase 2: simulate partial DB loss (tree_state gone) ─────────────────
	// tree_state is absent (e.g. table truncated or a crash before the row was
	// written), but latest_head_car still holds the headCID — giving GetHead a
	// CID to return so the gateway fallback can be triggered.
	lostDB := newMockStateStore()
	lostDB.SetLatestHeadCAR(ctx, logDID, uint64(numFirstBatch), headCID, carData) //nolint:errcheck

	// Stand up a local gateway that serves the CAR for any request.
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		w.WriteHeader(http.StatusOK)
		w.Write(carData) //nolint:errcheck
	}))
	t.Cleanup(gateway.Close)

	_, err = New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: lostDB,
		LogDID:     logDID,
		Client:     mockClient,
		GatewayURL: gateway.URL,
		HTTPClient: gateway.Client(),
	})
	require.NoError(t, err)

	// ── Phase 3: assert tree state was restored in the state store ────────────
	// The bug: New() restores objStore but never calls SetTreeState, so size=0.
	// The fix: New() must call SetTreeState(ctx, logDID, size, root) after
	// parsing the checkpoint from the gateway CAR.
	restoredSize, restoredRoot, err := lostDB.GetTreeState(ctx, logDID)
	require.NoError(t, err)
	require.Equal(t, uint64(numFirstBatch), restoredSize,
		"after gateway recovery, tree_state size must equal the log size at backup time (%d), not 0 (which causes silent log fork)",
		numFirstBatch)
	require.NotEmpty(t, restoredRoot,
		"after gateway recovery, tree_state root hash must be non-empty")
}

// TestModeResume_EmptyState_ReturnsError verifies that ModeResume refuses to
// start when the state store is completely empty (total DB wipeout scenario).
func TestModeResume_EmptyState_ReturnsError(t *testing.T) {
	ctx := context.Background()

	stateStore := newMockStateStore()
	_, err := New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: stateStore,
		LogDID:     "did:key:test-resume-empty",
		Client:     NewMockClient(),
		Mode:       ModeResume,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot resume log")
	require.Contains(t, err.Error(), "no head CID found")
}

// TestModeCreate_EmptyState_Succeeds verifies that ModeCreate (the default) works
// fine with an empty state store — this is the normal first-time create path.
func TestModeCreate_EmptyState_Succeeds(t *testing.T) {
	ctx := context.Background()

	stateStore := newMockStateStore()
	_, err := New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: stateStore,
		LogDID:     "did:key:test-create-empty",
		Client:     NewMockClient(),
		Mode:       ModeCreate,
	})
	require.NoError(t, err)
}

// TestModeResume_ValidLocalState_Succeeds verifies that ModeResume succeeds when
// local CAR data is available in the state store (normal restart scenario).
func TestModeResume_ValidLocalState_Succeeds(t *testing.T) {
	ctx := context.Background()
	const logDID = "did:key:test-resume-valid"

	// Phase 1: write entries to populate the state store.
	firstStore := newMockStateStore()
	mockClient := NewMockClient()
	driver1, err := New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: firstStore,
		LogDID:     logDID,
		Client:     mockClient,
		Mode:       ModeCreate,
	})
	require.NoError(t, err)

	st1 := driver1.(*Storage)
	opts := tessera.NewAppendOptions().WithCheckpointSigner(&dummySigner{}).WithBatching(5, 0)
	appender1, _, err := st1.Appender(ctx, opts)
	require.NoError(t, err)

	futures := make([]tessera.IndexFuture, 5)
	for i := 0; i < 5; i++ {
		futures[i] = appender1.Add(ctx, tessera.NewEntry([]byte(fmt.Sprintf("e%d", i))))
	}
	for i := 0; i < 5; i++ {
		_, err := futures[i]()
		require.NoError(t, err)
	}

	// Phase 2: resume from the same state store (local CAR present).
	_, err = New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: firstStore,
		LogDID:     logDID,
		Client:     mockClient,
		Mode:       ModeResume,
	})
	require.NoError(t, err)
}

// TestRecover_ValidHeadCID_RestoresState verifies that Recover() with a reachable
// gateway restores tree_state so that a subsequent ModeResume startup succeeds.
func TestRecover_ValidHeadCID_RestoresState(t *testing.T) {
	ctx := context.Background()
	const logDID = "did:key:test-recover-valid"
	const numEntries = 7

	// Phase 1: write entries to get a headCID and CAR.
	firstStore := newMockStateStore()
	mockClient := NewMockClient()
	driver1, err := New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: firstStore,
		LogDID:     logDID,
		Client:     mockClient,
		Mode:       ModeCreate,
	})
	require.NoError(t, err)

	st1 := driver1.(*Storage)
	opts := tessera.NewAppendOptions().WithCheckpointSigner(&dummySigner{}).WithBatching(numEntries, 0)
	appender1, _, err := st1.Appender(ctx, opts)
	require.NoError(t, err)
	futures := make([]tessera.IndexFuture, numEntries)
	for i := 0; i < numEntries; i++ {
		futures[i] = appender1.Add(ctx, tessera.NewEntry([]byte(fmt.Sprintf("e%d", i))))
	}
	for i := 0; i < numEntries; i++ {
		_, err := futures[i]()
		require.NoError(t, err)
	}

	headCID, _, carData, err := firstStore.GetLatestHeadCAR(ctx, logDID)
	require.NoError(t, err)
	require.NotEmpty(t, headCID)

	// Phase 2: set up gateway serving the CAR, simulate total DB wipeout.
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		w.WriteHeader(http.StatusOK)
		w.Write(carData) //nolint:errcheck
	}))
	t.Cleanup(gateway.Close)

	wipedDB := newMockStateStore()

	// Phase 3: recover.
	err = Recover(ctx, Config{
		LogDID:     logDID,
		StateStore: wipedDB,
		GatewayURL: gateway.URL,
		HTTPClient: gateway.Client(),
	}, headCID)
	require.NoError(t, err)

	// Tree state must be restored.
	size, root, err := wipedDB.GetTreeState(ctx, logDID)
	require.NoError(t, err)
	require.Equal(t, uint64(numEntries), size)
	require.NotEmpty(t, root)

	// Phase 4: ModeResume startup must now succeed.
	_, err = New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: wipedDB,
		LogDID:     logDID,
		Client:     mockClient,
		Mode:       ModeResume,
		GatewayURL: gateway.URL,
		HTTPClient: gateway.Client(),
	})
	require.NoError(t, err)
}

// TestRecover_UnreachableGateway_ReturnsError verifies that Recover() returns an
// error when the gateway cannot serve the CAR.
func TestRecover_UnreachableGateway_ReturnsError(t *testing.T) {
	ctx := context.Background()

	badGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))
	t.Cleanup(badGateway.Close)

	err := Recover(ctx, Config{
		LogDID:     "did:key:test-recover-fail",
		StateStore: newMockStateStore(),
		GatewayURL: badGateway.URL,
		HTTPClient: badGateway.Client(),
	}, "bafybeifake")
	require.Error(t, err)
	require.Contains(t, err.Error(), "recover")
}

// TestColdStart_GatewayUnreachable_ReturnsError verifies that when
// latest_head_car is lost but the headCID is still known via index_persistence
// (partial DB loss), and the IPFS gateway is also unreachable, storacha.New()
// returns an error instead of silently starting a fresh empty log.
//
// The gateway path requires the headCID as input — it comes from the DB.  So
// this scenario is: DB partially survived (headCID known) but the CAR itself
// cannot be fetched.  Starting empty here would fork the log: new entries would
// be assigned indices from 0, irrecoverably colliding with prior entries.  The
// correct behaviour is to refuse to start and surface an error so the operator
// can investigate (restore from backup, wait for gateway, etc.).
func TestColdStart_GatewayUnreachable_ReturnsError(t *testing.T) {
	ctx := context.Background()
	const logDID = "did:key:test-recovery-gateway-down"
	const numFirstBatch = 5

	// ── Phase 1: write entries and capture the headCID ────────────────────────
	firstStore := newMockStateStore()
	mockClient := NewMockClient()

	driver1, err := New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: firstStore,
		LogDID:     logDID,
		Client:     mockClient,
	})
	require.NoError(t, err)

	st1 := driver1.(*Storage)
	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(&dummySigner{}).
		WithBatching(numFirstBatch, 0)

	appender1, _, err := st1.Appender(ctx, opts)
	require.NoError(t, err)

	futures := make([]tessera.IndexFuture, numFirstBatch)
	for i := 0; i < numFirstBatch; i++ {
		futures[i] = appender1.Add(ctx, tessera.NewEntry([]byte(fmt.Sprintf("entry-%d", i))))
	}
	for i := 0; i < numFirstBatch; i++ {
		idxResult, addErr := futures[i]()
		require.NoError(t, addErr)
		require.Equal(t, uint64(i), idxResult.Index)
	}

	headCID, _, _, err := firstStore.GetLatestHeadCAR(ctx, logDID)
	require.NoError(t, err)
	require.NotEmpty(t, headCID)

	// ── Phase 2: simulate partial DB loss + unreachable gateway ─────────────
	// tree_state is gone, but latest_head_car holds the last known headCID —
	// giving GetHead a CID to return so the gateway fallback can attempt to
	// fetch it. The gateway then fails to serve it, which is the scenario under test.
	lostDB := newMockStateStore()
	lostDB.SetLatestHeadCAR(ctx, logDID, uint64(numFirstBatch), headCID, nil) //nolint:errcheck

	// Point at a gateway that always returns 503 (simulates network outage /
	// unpinned CID / any fetch failure).
	unavailableGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
	}))
	t.Cleanup(unavailableGateway.Close)

	_, err = New(ctx, Config{
		SpaceDID:   "did:key:z6MkwDuRThQcyWjqNsK54yKAmzfsiH6BTkASyiucThMtHt1y",
		StateStore: lostDB,
		LogDID:     logDID,
		Client:     mockClient,
		GatewayURL: unavailableGateway.URL,
		HTTPClient: unavailableGateway.Client(),
	})

	// Key assertion: a known headCID means prior data exists.  Starting empty
	// when the gateway fetch fails would silently fork the log from index 0.
	// New() must return an error so the operator can take action.
	require.Error(t, err,
		"storacha.New() must return an error when the headCID is known but the gateway fetch fails — "+
			"silently starting empty would fork the log from index 0")
}
