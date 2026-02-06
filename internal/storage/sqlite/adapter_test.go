package sqlite_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/relves/ucanlog/internal/storage"
	"github.com/relves/ucanlog/internal/storage/sqlite"
)

func TestSQLiteAdapter_ImplementsInterface(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-adapter-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	// Verify LogStore implements StateStore
	var _ storage.StateStore = store
}

func TestSQLiteAdapter_GetHead(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-adapter-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store, err := sqlite.OpenLogStore(tmpDir, "did:key:z6MkMain")
	require.NoError(t, err)
	defer store.Close()

	ctx := context.Background()
	logDID := "did:key:z6MkMain"

	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	// Set tree state and index persistence
	require.NoError(t, store.SetTreeState(ctx, logDID, 5, []byte{0x01}))
	uploadTime := time.Now().UTC()
	require.NoError(t, store.SetIndexPersistence(ctx, logDID, uploadTime, 1024, "bafyHead1"))

	// Get head via adapter interface (should read from tree_state and index_persistence)
	indexCID, treeSize, err := store.GetHead(ctx, logDID)
	require.NoError(t, err)
	assert.Equal(t, "bafyHead1", indexCID)
	assert.Equal(t, uint64(5), treeSize)
}
