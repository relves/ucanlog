package tlog

import (
	"context"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/relves/ucanlog/internal/storage/sqlite"
	"github.com/relves/ucanlog/internal/storage/storacha"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedStateStore writes a minimal valid CAR into the state store so that
// ModeResume startup finds a non-empty headCID and succeeds.  This simulates
// the state left by a normal flush after at least one entry was written.
func seedStateStore(t *testing.T, ctx context.Context, store interface {
	SetLatestHeadCAR(context.Context, string, uint64, string, []byte) error
}, logID string) {
	t.Helper()
	// Minimal checkpoint: "origin\n<size>\n<base64-root>\n<sig>\n"
	rootHash := make([]byte, 32)
	checkpoint := []byte("test-origin\n1\n" + base64.StdEncoding.EncodeToString(rootHash) + "\nsig\n")
	carData, rootCID, err := storacha.BuildHybridCAR(ctx, map[string][]byte{"checkpoint": checkpoint}, nil)
	require.NoError(t, err)
	require.NoError(t, store.SetLatestHeadCAR(ctx, logID, 1, rootCID.String(), carData))
}

// TestLazyRestoreAfterRestart simulates the service restart scenario.
// 1. Create a log with Manager A
// 2. Create a new Manager B (simulating restart - empty logs map)
// 3. Manager B should be able to read from the log via lazy restoration
func TestLazyRestoreAfterRestart(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	logID := "did:key:z6MkLazyTest"

	// Setup shared components
	storeManager := sqlite.NewStoreManager(tmpDir)
	defer storeManager.CloseAll()

	privKey := make([]byte, 64)
	for i := range privKey {
		privKey[i] = byte(i)
	}
	tlogSigner, err := NewEd25519Signer(privKey, "test")
	require.NoError(t, err)

	serviceSigner, err := ed25519signer.Generate()
	require.NoError(t, err)


	// Create log directory and database (simulating previous log creation)
	logDir := filepath.Join(tmpDir, "logs", logID)
	require.NoError(t, os.MkdirAll(logDir, 0755))

	store, err := storeManager.GetStore(logID)
	require.NoError(t, err)
	require.NoError(t, store.CreateLogRecord(ctx, logID))

	// Seed state so ModeResume finds a non-empty headCID (simulates post-flush state).
	seedStateStore(t, ctx, store, logID)

	// Create "new" Manager (simulating restart)
	mgr, err := NewDelegatedManager(DelegatedManagerConfig{
		BasePath:      tmpDir,
		Signer:        tlogSigner,
		PrivateKey:    privKey,
		OriginPrefix:  "test",
		ServiceSigner: serviceSigner,
		StoreManager:  storeManager,
	})
	require.NoError(t, err)

	// Verify logs map is empty
	assert.Equal(t, 0, len(mgr.logs))

	// GetLogInstance should lazily restore the log
	instance, err := mgr.GetLogInstance(ctx, logID)
	require.NoError(t, err)
	assert.NotNil(t, instance)

	// Verify log is now in memory
	assert.Equal(t, 1, len(mgr.logs))

	// Reader should work
	reader, err := mgr.GetReader(ctx, logID)
	require.NoError(t, err)
	assert.NotNil(t, reader)
}

// TestManager_RestoreLog tests that a log can be restored from disk
func TestManager_RestoreLog(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	// Create a mock log directory with SQLite database
	logID := "did:key:z6MkTestLog"
	logDir := filepath.Join(tmpDir, "logs", logID)
	require.NoError(t, os.MkdirAll(logDir, 0755))

	// Initialize SQLite store to create schema and log record
	storeManager := sqlite.NewStoreManager(tmpDir)
	defer storeManager.CloseAll()
	store, err := storeManager.GetStore(logID)
	require.NoError(t, err)
	require.NoError(t, store.CreateLogRecord(ctx, logID))

	// Seed state so ModeResume finds a non-empty headCID (simulates post-flush state).
	seedStateStore(t, ctx, store, logID)

	// Create manager (logs map is empty)
	privKey := make([]byte, 64)
	tlogSigner, _ := NewEd25519Signer(privKey, "test")

	serviceSigner, _ := ed25519signer.Generate()

	mgr, err := NewDelegatedManager(DelegatedManagerConfig{
		BasePath:      tmpDir,
		Signer:        tlogSigner,
		PrivateKey:    privKey,
		OriginPrefix:  "test",
		ServiceSigner: serviceSigner,
		StoreManager:  storeManager,
	})
	require.NoError(t, err)

	// Log should be lazily restored from disk
	instance, err := mgr.GetLogInstance(ctx, logID)
	require.NoError(t, err)
	assert.NotNil(t, instance)
	assert.NotNil(t, instance.Reader)
	assert.Equal(t, logID, instance.SpaceDID)
}

// TestRecoverLog_WithRevocations verifies that RecoverLog restores both the
// main log and its revocations log when a revocationsHeadCID is supplied.
func TestRecoverLog_WithRevocations(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	logID := "did:key:z6MkRecoverTest"
	revLogID := logID + "-revocations"

	storeManager := sqlite.NewStoreManager(tmpDir)
	defer storeManager.CloseAll()

	// Seed both state stores with minimal valid CARs simulating prior flushes.
	mainStore, err := storeManager.GetStore(logID)
	require.NoError(t, err)
	require.NoError(t, mainStore.CreateLogRecord(ctx, logID))
	seedStateStore(t, ctx, mainStore, logID)

	revStore, err := storeManager.GetStore(revLogID)
	require.NoError(t, err)
	require.NoError(t, revStore.CreateLogRecord(ctx, revLogID))
	seedStateStore(t, ctx, revStore, revLogID)

	// Capture the headCIDs before wiping.
	mainHeadCID, _, _, err := mainStore.GetLatestHeadCAR(ctx, logID)
	require.NoError(t, err)
	require.NotEmpty(t, mainHeadCID)
	revHeadCID, _, _, err := revStore.GetLatestHeadCAR(ctx, revLogID)
	require.NoError(t, err)
	require.NotEmpty(t, revHeadCID)

	// Serve both CARs from a local gateway.
	mainCarData := getCARData(t, ctx, mainStore, logID)
	revCarData := getCARData(t, ctx, revStore, revLogID)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.ipld.car")
		if strings.Contains(r.URL.Path, mainHeadCID) {
			w.Write(mainCarData) //nolint:errcheck
		} else {
			w.Write(revCarData) //nolint:errcheck
		}
	}))
	t.Cleanup(gateway.Close)

	// Wipe both stores.
	wipedMain, err := storeManager.GetStore(logID)
	require.NoError(t, err)
	wipedRev, err := storeManager.GetStore(revLogID)
	require.NoError(t, err)

	privKey := make([]byte, 64)
	tlogSigner, _ := NewEd25519Signer(privKey, "test")
	serviceSigner, _ := ed25519signer.Generate()

	mgr, err := NewDelegatedManager(DelegatedManagerConfig{
		BasePath:      tmpDir,
		Signer:        tlogSigner,
		PrivateKey:    privKey,
		OriginPrefix:  "test",
		ServiceSigner: serviceSigner,
		StoreManager:  storeManager,
	})
	require.NoError(t, err)

	// Override gateway URL via env for the manager's RecoverLog.
	t.Setenv("IPFS_GATEWAY_URL", gateway.URL)

	// Recover both logs.
	err = mgr.RecoverLog(ctx, logID, mainHeadCID, revHeadCID)
	require.NoError(t, err)

	// Both state stores must have non-zero tree size restored.
	mainSize, _, err := wipedMain.GetTreeState(ctx, logID)
	require.NoError(t, err)
	require.Greater(t, mainSize, uint64(0), "main log tree state must be restored")

	revSize, _, err := wipedRev.GetTreeState(ctx, revLogID)
	require.NoError(t, err)
	require.Greater(t, revSize, uint64(0), "revocations log tree state must be restored")
}

// getCARData retrieves the stored CAR bytes from a state store (test helper).
func getCARData(t *testing.T, ctx context.Context, store interface {
	GetLatestHeadCAR(context.Context, string) (string, uint64, []byte, error)
}, logID string) []byte {
	t.Helper()
	_, _, data, err := store.GetLatestHeadCAR(ctx, logID)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	return data
}

// TestManager_GetLogInstance_NotFound tests that non-existent logs return error
func TestManager_GetLogInstance_NotFound(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	storeManager := sqlite.NewStoreManager(tmpDir)
	defer storeManager.CloseAll()
	privKey := make([]byte, 64)
	tlogSigner, _ := NewEd25519Signer(privKey, "test")
	serviceSigner, _ := ed25519signer.Generate()

	mgr, err := NewDelegatedManager(DelegatedManagerConfig{
		BasePath:      tmpDir,
		Signer:        tlogSigner,
		PrivateKey:    privKey,
		OriginPrefix:  "test",
		ServiceSigner: serviceSigner,
		StoreManager:  storeManager,
	})
	require.NoError(t, err)

	// Non-existent log should return error
	_, err = mgr.GetLogInstance(ctx, "did:key:z6MkNonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
