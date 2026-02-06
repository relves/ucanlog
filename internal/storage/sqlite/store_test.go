package sqlite_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/relves/ucanlog/internal/storage/sqlite"
)

func TestLogStore_OpenAndClose(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logDID := "did:key:z6Mktest123"

	store, err := sqlite.OpenLogStore(tmpDir, logDID)
	require.NoError(t, err)
	require.NotNil(t, store)

	dbPath := filepath.Join(tmpDir, "logs", logDID, "log.db")
	_, err = os.Stat(dbPath)
	assert.NoError(t, err, "database file should exist")

	err = store.Close()
	assert.NoError(t, err)
}

func TestLogStore_OpenExisting(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	logDID := "did:key:z6Mktest123"

	store1, err := sqlite.OpenLogStore(tmpDir, logDID)
	require.NoError(t, err)
	require.NoError(t, store1.Close())

	store2, err := sqlite.OpenLogStore(tmpDir, logDID)
	require.NoError(t, err)
	require.NoError(t, store2.Close())
}

func TestLogStore_LogRecord_Create(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	err = store.CreateLogRecord(ctx, "did:key:z6MkMain")
	require.NoError(t, err)

	err = store.CreateLogRecord(ctx, "did:key:z6MkMain-revocations")
	require.NoError(t, err)
}

func TestLogStore_LogRecord_Get(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	err = store.CreateLogRecord(ctx, "did:key:z6MkMain")
	require.NoError(t, err)

	record, err := store.GetLogRecord(ctx, "did:key:z6MkMain")
	require.NoError(t, err)
	assert.Equal(t, "did:key:z6MkMain", record.LogDID)
	assert.False(t, record.CreatedAt.IsZero())
}

func TestLogStore_LogRecord_NotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	_, err = store.GetLogRecord(ctx, "did:key:z6MkNonexistent")
	assert.ErrorIs(t, err, sqlite.ErrNotFound)
}

func TestLogStore_GetHead_Empty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	// GetHead should return empty values when no tree_state or index_persistence data exists
	indexCID, treeSize, err := store.GetHead(ctx, logDID)
	require.NoError(t, err)
	assert.Equal(t, "", indexCID)
	assert.Equal(t, uint64(0), treeSize)
}

func TestLogStore_GetHead_WithData(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	// Set tree state
	require.NoError(t, store.SetTreeState(ctx, logDID, 42, []byte{0x01, 0x02}))

	// Set index persistence
	uploadTime := time.Now().UTC()
	require.NoError(t, store.SetIndexPersistence(ctx, logDID, uploadTime, 1024, "bafyIndexCID"))

	// GetHead should return values from tree_state and index_persistence
	indexCID, treeSize, err := store.GetHead(ctx, logDID)
	require.NoError(t, err)
	assert.Equal(t, "bafyIndexCID", indexCID)
	assert.Equal(t, uint64(42), treeSize)
}

func TestLogStore_CIDIndex_SetAndGet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	err = store.SetCID(ctx, logDID, "tile/0/000", "bafyCID1")
	require.NoError(t, err)

	err = store.SetCIDs(ctx, logDID, map[string]string{
		"tile/0/001": "bafyCID2",
		"checkpoint": "bafyCheckpoint",
	})
	require.NoError(t, err)

	index, err := store.GetCIDIndex(ctx, logDID)
	require.NoError(t, err)
	assert.Equal(t, "bafyCID1", index["tile/0/000"])
	assert.Equal(t, "bafyCID2", index["tile/0/001"])
	assert.Equal(t, "bafyCheckpoint", index["checkpoint"])
}

func TestLogStore_CIDIndex_Upsert(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	require.NoError(t, store.SetCID(ctx, logDID, "checkpoint", "bafyOld"))
	require.NoError(t, store.SetCID(ctx, logDID, "checkpoint", "bafyNew"))

	index, err := store.GetCIDIndex(ctx, logDID)
	require.NoError(t, err)
	assert.Equal(t, "bafyNew", index["checkpoint"])
}

func TestLogStore_CIDIndex_EmptyForNewLog(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	index, err := store.GetCIDIndex(ctx, logDID)
	require.NoError(t, err)
	assert.Empty(t, index)
}

func TestLogStore_TreeState_SetAndGet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	root := []byte{0x01, 0x02, 0x03, 0x04}

	// Set tree state
	err = store.SetTreeState(ctx, logDID, 42, root)
	require.NoError(t, err)

	// Get tree state
	size, gotRoot, err := store.GetTreeState(ctx, logDID)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), size)
	assert.Equal(t, root, gotRoot)
}

func TestLogStore_TreeState_DefaultEmpty(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	// No tree state yet
	size, root, err := store.GetTreeState(ctx, logDID)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), size)
	assert.Nil(t, root)
}

func TestLogStore_TreeState_Update(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	// Set initial
	require.NoError(t, store.SetTreeState(ctx, logDID, 1, []byte{0x01}))

	// Update
	require.NoError(t, store.SetTreeState(ctx, logDID, 2, []byte{0x02, 0x03}))

	size, root, err := store.GetTreeState(ctx, logDID)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), size)
	assert.Equal(t, []byte{0x02, 0x03}, root)
}

func TestLogStore_Revocations_AddAndCheck(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	// Add revocation
	err = store.AddRevocation(ctx, "bafyDelegation1")
	require.NoError(t, err)

	// Check revoked
	revoked, err := store.IsRevoked(ctx, "bafyDelegation1")
	require.NoError(t, err)
	assert.True(t, revoked)

	// Check not revoked
	revoked, err = store.IsRevoked(ctx, "bafyDelegation2")
	require.NoError(t, err)
	assert.False(t, revoked)
}

func TestLogStore_Revocations_GetAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	// Add multiple revocations
	require.NoError(t, store.AddRevocation(ctx, "bafyDel1"))
	require.NoError(t, store.AddRevocation(ctx, "bafyDel2"))
	require.NoError(t, store.AddRevocation(ctx, "bafyDel3"))

	revocations, err := store.GetRevocations(ctx)
	require.NoError(t, err)
	assert.Len(t, revocations, 3)
	assert.Contains(t, revocations, "bafyDel1")
	assert.Contains(t, revocations, "bafyDel2")
	assert.Contains(t, revocations, "bafyDel3")
}

func TestLogStore_Revocations_Idempotent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()

	// Add same revocation twice - should not error
	require.NoError(t, store.AddRevocation(ctx, "bafyDel1"))
	require.NoError(t, store.AddRevocation(ctx, "bafyDel1"))

	revocations, err := store.GetRevocations(ctx)
	require.NoError(t, err)
	assert.Len(t, revocations, 1)
}

func TestLogStore_IndexPersistence_SetAndGet(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	// Set index persistence metadata
	uploadTime := time.Now().UTC()
	err = store.SetIndexPersistence(ctx, logDID, uploadTime, 1024, "bafyIndexCID")
	require.NoError(t, err)

	// Get metadata
	meta, err := store.GetIndexPersistence(ctx, logDID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, uint64(1024), meta.LastUploadedSize)
	assert.Equal(t, "bafyIndexCID", meta.LastUploadedCID)
	assert.WithinDuration(t, uploadTime, meta.LastUploadTime, time.Second)
}

func TestLogStore_IndexPersistence_NotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	// No persistence metadata yet
	meta, err := store.GetIndexPersistence(ctx, logDID)
	require.NoError(t, err)
	assert.Nil(t, meta) // Returns nil for no metadata
}

func TestLogStore_IndexPersistence_Update(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	// Set initial
	uploadTime1 := time.Now().UTC()
	require.NoError(t, store.SetIndexPersistence(ctx, logDID, uploadTime1, 512, "bafyOldCID"))

	// Update
	uploadTime2 := time.Now().UTC().Add(time.Minute)
	require.NoError(t, store.SetIndexPersistence(ctx, logDID, uploadTime2, 2048, "bafyNewCID"))

	// Verify updated values
	meta, err := store.GetIndexPersistence(ctx, logDID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, uint64(2048), meta.LastUploadedSize)
	assert.Equal(t, "bafyNewCID", meta.LastUploadedCID)
	assert.WithinDuration(t, uploadTime2, meta.LastUploadTime, time.Second)
}
