package tlog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/relves/ucanlog/internal/storage/sqlite"
	ed25519signer "github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	cidStore := NewStateStoreCIDStore(storeManager.GetStateStore)

	// Create log directory and database (simulating previous log creation)
	logDir := filepath.Join(tmpDir, "logs", logID)
	require.NoError(t, os.MkdirAll(logDir, 0755))

	store, err := storeManager.GetStore(logID)
	require.NoError(t, err)
	require.NoError(t, store.CreateLogRecord(ctx, logID))

	// Create "new" Manager (simulating restart)
	mgr, err := NewDelegatedManager(DelegatedManagerConfig{
		BasePath:      tmpDir,
		Signer:        tlogSigner,
		PrivateKey:    privKey,
		OriginPrefix:  "test",
		ServiceSigner: serviceSigner,
		CIDStore:      cidStore,
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

	// Create manager (logs map is empty)
	privKey := make([]byte, 64)
	tlogSigner, _ := NewEd25519Signer(privKey, "test")
	cidStore := NewStateStoreCIDStore(storeManager.GetStateStore)

	serviceSigner, _ := ed25519signer.Generate()

	mgr, err := NewDelegatedManager(DelegatedManagerConfig{
		BasePath:      tmpDir,
		Signer:        tlogSigner,
		PrivateKey:    privKey,
		OriginPrefix:  "test",
		ServiceSigner: serviceSigner,
		CIDStore:      cidStore,
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

// TestManager_GetLogInstance_NotFound tests that non-existent logs return error
func TestManager_GetLogInstance_NotFound(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	storeManager := sqlite.NewStoreManager(tmpDir)
	defer storeManager.CloseAll()
	privKey := make([]byte, 64)
	tlogSigner, _ := NewEd25519Signer(privKey, "test")
	cidStore := NewStateStoreCIDStore(storeManager.GetStateStore)
	serviceSigner, _ := ed25519signer.Generate()

	mgr, err := NewDelegatedManager(DelegatedManagerConfig{
		BasePath:      tmpDir,
		Signer:        tlogSigner,
		PrivateKey:    privKey,
		OriginPrefix:  "test",
		ServiceSigner: serviceSigner,
		CIDStore:      cidStore,
		StoreManager:  storeManager,
	})
	require.NoError(t, err)

	// Non-existent log should return error
	_, err = mgr.GetLogInstance(ctx, "did:key:z6MkNonexistent")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
