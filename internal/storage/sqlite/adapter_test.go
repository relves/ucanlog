package sqlite_test

import (
	"context"
	"os"
	"testing"

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

	// Populate latest_head_car (written on every append)
	require.NoError(t, store.SetLatestHeadCAR(ctx, logDID, 5, "bafyHead1", []byte{0x01}))

	indexCID, treeSize, err := store.GetHead(ctx, logDID)
	require.NoError(t, err)
	assert.Equal(t, "bafyHead1", indexCID)
	assert.Equal(t, uint64(5), treeSize)
}
