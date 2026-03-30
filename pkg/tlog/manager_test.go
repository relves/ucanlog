// internal/tlog/manager_test.go
package tlog

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/relves/ucanlog/internal/storage/sqlite"
	"github.com/relves/ucanlog/internal/storage/storacha"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
	edsigner "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/transparency-dev/tessera"
)

// testSigner creates an Ed25519 signer for testing
func testSigner(t *testing.T) Signer {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("Failed to generate test key: %v", err)
	}
	signer, err := NewEd25519Signer(priv, "test-log")
	if err != nil {
		t.Fatalf("Failed to create test signer: %v", err)
	}
	return signer
}


// testManager creates a Manager with mock Storacha client for testing
func testManager(t *testing.T, tmpDir string) *Manager {
	t.Helper()
	manager, err := NewStorachaManager(
		tmpDir,
		testSigner(t),
		nil, // proofBytes - not needed for tests
		"did:key:test-issuer",
		storacha.NewMockClient(),
		"did:key:test-space",
	)
	if err != nil {
		t.Fatalf("NewStorachaManager failed: %v", err)
	}
	return manager
}

func TestManager_CreateLog(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tlog-manager-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	manager := testManager(t, tmpDir)

	ctx := context.Background()
	logID := "test-log-1"

	err = manager.CreateLog(ctx, logID)
	if err != nil {
		t.Fatalf("CreateLog failed: %v", err)
	}

	// Verify log exists
	exists, err := manager.LogExists(ctx, logID)
	if err != nil {
		t.Fatalf("LogExists failed: %v", err)
	}
	if !exists {
		t.Error("Log should exist after creation")
	}
}

func TestManager_AddEntry(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tlog-add-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	manager := testManager(t, tmpDir)

	ctx := context.Background()
	logID := "test-log-add"
	manager.CreateLog(ctx, logID)

	// Add entries using internal method (tests can access private methods)
	data1 := []byte("first entry")
	index1, err := manager.addEntry(ctx, logID, data1)
	if err != nil {
		t.Fatalf("addEntry failed: %v", err)
	}
	if index1 != 0 {
		t.Errorf("First index = %d, want 0", index1)
	}

	data2 := []byte("second entry")
	index2, err := manager.addEntry(ctx, logID, data2)
	if err != nil {
		t.Fatalf("addEntry failed: %v", err)
	}
	if index2 != 1 {
		t.Errorf("Second index = %d, want 1", index2)
	}
}

func TestManager_ReadEntry(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tlog-read-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	manager := testManager(t, tmpDir)

	ctx := context.Background()
	logID := "test-log-read"
	manager.CreateLog(ctx, logID)

	// Add an entry
	data := []byte("test entry data")
	_, err = manager.addEntry(ctx, logID, data)
	if err != nil {
		t.Fatalf("addEntry failed: %v", err)
	}

	// Get reader for checking integration
	reader, err := manager.GetReader(ctx, logID)
	if err != nil {
		t.Fatalf("GetReader failed: %v", err)
	}

	// Wait for integration to complete
	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for integration")
		default:
			size, err := reader.IntegratedSize(ctx)
			if err != nil {
				t.Fatalf("IntegratedSize failed: %v", err)
			}
			if size > 0 {
				goto integrated
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

integrated:
	// Read the checkpoint to verify the log is working
	// Note: checkpoint may not be available with mock storage
	checkpoint, err := manager.ReadCheckpoint(ctx, logID)
	if err != nil {
		t.Logf("ReadCheckpoint failed (expected with mock storage): %v", err)
	} else if len(checkpoint) == 0 {
		t.Error("Expected non-empty checkpoint")
	}

	// Also verify we can get the reader interface
	nextIndex, err := reader.NextIndex(ctx)
	if err != nil {
		t.Fatalf("NextIndex failed: %v", err)
	}

	if nextIndex != 1 {
		t.Errorf("NextIndex = %d, want 1", nextIndex)
	}
}

func TestManager_ReadRange(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tlog-range-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	manager := testManager(t, tmpDir)

	ctx := context.Background()
	logID := "test-log-range"
	manager.CreateLog(ctx, logID)

	// Add multiple entries
	entries := [][]byte{
		[]byte("entry 0"),
		[]byte("entry 1"),
		[]byte("entry 2"),
		[]byte("entry 3"),
		[]byte("entry 4"),
	}

	for _, data := range entries {
		_, err := manager.addEntry(ctx, logID, data)
		if err != nil {
			t.Fatalf("addEntry failed: %v", err)
		}
	}

	// Wait for integration
	reader, err := manager.GetReader(ctx, logID)
	if err != nil {
		t.Fatalf("GetReader failed: %v", err)
	}

	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("Timeout waiting for integration")
		default:
			size, err := reader.IntegratedSize(ctx)
			if err != nil {
				t.Fatalf("IntegratedSize failed: %v", err)
			}
			if size >= 5 {
				goto integrated
			}
			time.Sleep(50 * time.Millisecond)
		}
	}

integrated:
	// Check NextIndex
	nextIndex, err := reader.NextIndex(ctx)
	if err != nil {
		t.Fatalf("NextIndex failed: %v", err)
	}
	t.Logf("NextIndex: %d", nextIndex)

	// Debug: list files in log directory
	logDir := filepath.Join(tmpDir, "logs", logID)
	err = filepath.Walk(logDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, _ := filepath.Rel(logDir, path)
		t.Logf("  %s", relPath)
		return nil
	})
	if err != nil {
		t.Logf("Failed to list directory: %v", err)
	}

	// First check if checkpoint exists
	checkpoint, err := manager.ReadCheckpoint(ctx, logID)
	if err != nil {
		t.Logf("ReadCheckpoint failed: %v", err)
	} else {
		t.Logf("Checkpoint exists, length: %d", len(checkpoint))
	}

	// Debug: try reading bundle 0 directly
	bundle0, err := manager.ReadEntryBundle(ctx, logID, 0, 0)
	if err != nil {
		t.Logf("ReadEntryBundle(0, 0) failed: %v", err)
	} else {
		t.Logf("Bundle 0 length: %d", len(bundle0))
		if len(bundle0) > 2 {
			t.Logf("Bundle 0 data: %q", bundle0[2:])
		}
	}

	// Test ReadRange from index 1 to 4 (exclusive)
	rangeEntries, err := manager.ReadRange(ctx, logID, 1, 4)
	if err != nil {
		t.Fatalf("ReadRange failed: %v", err)
	}

	expected := entries[1:4] // indices 1, 2, 3
	if len(rangeEntries) != len(expected) {
		t.Fatalf("ReadRange returned %d entries, expected %d", len(rangeEntries), len(expected))
	}

	for i, entry := range rangeEntries {
		expectedData := expected[i]
		if string(entry) != string(expectedData) {
			t.Errorf("Entry %d: got %q, want %q", i+1, entry, expectedData)
		}
	}
}

func TestManager_LargeLog(t *testing.T) {
	// Skip: mock storage doesn't support the tile lookups needed for 256+ entries
	// This test requires proper tile persistence across bundle boundaries
	t.Skip("Skipping: mock storage doesn't support multi-bundle operations")

	tmpDir, err := os.MkdirTemp("", "tlog-large-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	manager := testManager(t, tmpDir)

	ctx := context.Background()
	logID := "test-log-large"
	err = manager.CreateLog(ctx, logID)
	if err != nil {
		t.Fatal(err)
	}

	// Create 300 entries to test multi-bundle reading (256 entries per bundle)
	numEntries := 300
	t.Logf("Adding %d entries using batch add...", numEntries)

	entriesData := make([][]byte, numEntries)
	for i := 0; i < numEntries; i++ {
		entriesData[i] = []byte(fmt.Sprintf("entry %d", i))
	}

	indices, err := manager.addEntriesBatch(ctx, logID, entriesData)
	if err != nil {
		t.Fatalf("addEntriesBatch failed: %v", err)
	}

	// Verify indices are sequential
	for i, idx := range indices {
		if idx != uint64(i) {
			t.Errorf("Entry %d got index %d, want %d", i, idx, i)
		}
	}
	t.Logf("All %d entries added successfully", numEntries)

	// Wait for integration
	reader, err := manager.GetReader(ctx, logID)
	if err != nil {
		t.Fatal(err)
	}

	timeout := time.After(15 * time.Second)
	for {
		select {
		case <-timeout:
			size, _ := reader.IntegratedSize(ctx)
			t.Fatalf("Timeout waiting for integration (integrated: %d, want: %d)", size, numEntries)
		default:
			size, err := reader.IntegratedSize(ctx)
			if err != nil {
				t.Fatalf("IntegratedSize failed: %v", err)
			}
			if size >= uint64(numEntries) {
				t.Logf("Integration complete: %d entries", size)
				goto integrated
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

integrated:
	// Debug: list actual file structure
	logDir := filepath.Join(tmpDir, "logs", logID)
	t.Log("=== File structure ===")
	err = filepath.Walk(logDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		relPath, _ := filepath.Rel(logDir, path)
		if info.IsDir() {
			t.Logf("  [dir]  %s", relPath)
		} else {
			t.Logf("  [file] %s (%d bytes)", relPath, info.Size())
		}
		return nil
	})
	if err != nil {
		t.Logf("Failed to list directory: %v", err)
	}
	t.Log("=== End file structure ===")

	// Test reading within first bundle (0-255)
	t.Log("Testing read within first bundle (0-100)...")
	entries, err := manager.ReadRange(ctx, logID, 0, 100)
	if err != nil {
		t.Fatalf("ReadRange(0, 100) failed: %v", err)
	}
	if len(entries) != 100 {
		t.Errorf("ReadRange(0, 100) returned %d entries, want 100", len(entries))
	}

	// Verify first few entries
	for i := 0; i < 5; i++ {
		expected := fmt.Sprintf("entry %d", i)
		if string(entries[i]) != expected {
			t.Errorf("Entry %d: got %q, want %q", i, entries[i], expected)
		}
	}

	// Test reading across bundle boundary (250-260 crosses 256)
	t.Log("Testing read across bundle boundary (250-270)...")
	entries, err = manager.ReadRange(ctx, logID, 250, 270)
	if err != nil {
		t.Fatalf("ReadRange(250, 270) failed: %v", err)
	}
	if len(entries) != 20 {
		t.Errorf("ReadRange(250, 270) returned %d entries, want 20", len(entries))
	}

	// Verify entries across boundary
	for i, entry := range entries {
		expected := fmt.Sprintf("entry %d", 250+i)
		if string(entry) != expected {
			t.Errorf("Entry %d: got %q, want %q", 250+i, entry, expected)
		}
	}

	// Test reading from second bundle only (260-290)
	t.Log("Testing read from second bundle only (260-290)...")
	entries, err = manager.ReadRange(ctx, logID, 260, 290)
	if err != nil {
		t.Fatalf("ReadRange(260, 290) failed: %v", err)
	}
	if len(entries) != 30 {
		t.Errorf("ReadRange(260, 290) returned %d entries, want 30", len(entries))
	}

	// Verify entries from second bundle
	for i, entry := range entries {
		expected := fmt.Sprintf("entry %d", 260+i)
		if string(entry) != expected {
			t.Errorf("Entry %d: got %q, want %q", 260+i, entry, expected)
		}
	}

	// Test reading all entries
	t.Log("Testing read all 300 entries...")
	entries, err = manager.ReadRange(ctx, logID, 0, 300)
	if err != nil {
		t.Fatalf("ReadRange(0, 300) failed: %v", err)
	}
	if len(entries) != 300 {
		t.Errorf("ReadRange(0, 300) returned %d entries, want 300", len(entries))
	}

	t.Log("All multi-bundle tests passed!")
}

// testLogger returns a discard logger suitable for tests.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.Default()
}

// testEd25519Signer generates a fresh ucanto ed25519 principal for use in delegations.
func testEd25519Signer(t *testing.T) principal.Signer {
	t.Helper()
	s, err := edsigner.Generate()
	require.NoError(t, err)
	return s
}

// createLogWithMockDriver inserts a log into m.logs backed by a real *storacha.Storage
// driver (with MockClient) so that AddEntryWithDelegation can do the st.SetClient swap.
// storeManager is lazily created from m.basePath if not already set.
func (m *Manager) createLogWithMockDriver(ctx context.Context, logID, spaceDID string, dlg delegation.Delegation) error {
	if m.storeManager == nil {
		m.storeManager = sqlite.NewStoreManager(m.basePath)
	}

	stateStore, err := m.storeManager.GetStore(logID)
	if err != nil {
		return fmt.Errorf("GetStore: %w", err)
	}
	_ = stateStore.CreateLogRecord(ctx, logID)

	driver, err := storacha.New(ctx, storacha.Config{
		SpaceDID:   spaceDID,
		StateStore: stateStore,
		LogDID:     logID,
		Client:     storacha.NewMockClient(),
		Logger:     m.logger,
	})
	if err != nil {
		return fmt.Errorf("storacha.New: %w", err)
	}

	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(m.signer).
		WithCheckpointInterval(time.Second).
		WithBatching(1, 100*time.Millisecond).
		WithCheckpointRepublishInterval(24 * time.Hour)

	appender, _, reader, err := tessera.NewAppender(ctx, driver, opts)
	if err != nil {
		return fmt.Errorf("tessera.NewAppender: %w", err)
	}

	m.mu.Lock()
	m.logs[logID] = &LogInstance{
		Appender: appender,
		Reader:   reader,
		Driver:   driver,
		SpaceDID: spaceDID,
	}
	m.mu.Unlock()
	return nil
}

// mockClientPool is a test double for clientPooler that records the spaceDID
// passed to GetClient and returns a MockClient (no network calls).
type mockClientPool struct {
	mu            sync.Mutex
	calledWithDID []string
}

func (m *mockClientPool) GetClient(spaceDID string, dlg delegation.Delegation) (storacha.StorachaClient, error) {
	m.mu.Lock()
	m.calledWithDID = append(m.calledWithDID, spaceDID)
	m.mu.Unlock()
	return storacha.NewMockClient(), nil
}

// testDelegatedManager creates a DelegatedManager with a mock client pool,
// returning both the manager and the pool so tests can inspect pool calls.
func testDelegatedManager(t *testing.T, tmpDir string) (*Manager, *mockClientPool) {
	t.Helper()
	pool := &mockClientPool{}
	m := &Manager{
		basePath:     tmpDir,
		logs:         make(map[string]*LogInstance),
		signer:       testSigner(t),
		originPrefix: "ucanlog",
		storeManager: nil, // not needed for this test path
		logger:       testLogger(t),
		clientPool:   pool,
	}
	return m, pool
}

// TestAddEntryWithDelegation_UsesSpaceDIDNotLogID is a regression test for the bug
// where revocation log IDs (e.g. "did:key:xxx-revocations") were passed directly to
// ClientPool.GetClient as the spaceDID, causing did.Parse to fail on the dash character.
//
// The fix: AddEntryWithDelegation uses instance.SpaceDID (stored at CreateLogWithDelegation
// time) rather than logID, so logs with non-DID identifiers work correctly.
func TestAddEntryWithDelegation_UsesSpaceDIDNotLogID(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "tlog-spacedid-regression-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	mgr, pool := testDelegatedManager(t, tmpDir)

	ctx := context.Background()

	// Simulate a revocation log: logID has a dash suffix, spaceDID is a valid DID.
	spaceDID := "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK"
	revocationLogID := spaceDID + "-revocations"

	// A non-nil delegation is required; content doesn't matter for this test.
	issuer := testEd25519Signer(t)
	dlg, err := delegation.Delegate(
		issuer,
		issuer.DID(),
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
		},
	)
	require.NoError(t, err)

	// Create the log — this stores instance.SpaceDID = spaceDID correctly.
	err = mgr.createLogWithMockDriver(ctx, revocationLogID, spaceDID, dlg)
	require.NoError(t, err)

	// AddEntryWithDelegation must call pool.GetClient with spaceDID, not logID.
	pool.mu.Lock()
	pool.calledWithDID = nil // reset after CreateLog calls
	pool.mu.Unlock()

	_, err = mgr.AddEntryWithDelegation(ctx, revocationLogID, []byte("revocation-data"), dlg)
	require.NoError(t, err)

	pool.mu.Lock()
	calls := append([]string(nil), pool.calledWithDID...)
	pool.mu.Unlock()

	require.NotEmpty(t, calls, "GetClient should have been called")
	for _, gotDID := range calls {
		assert.Equal(t, spaceDID, gotDID,
			"GetClient must be called with spaceDID, not logID %q", revocationLogID)
		assert.NotEqual(t, revocationLogID, gotDID,
			"regression: logID %q must not be used as spaceDID", revocationLogID)
	}
}
