package sqlite_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/relves/ucanlog/internal/storage/sqlite"
)

func TestStoreManager_GetStore(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-manager-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := sqlite.NewStoreManager(tmpDir)
	defer manager.CloseAll()

	// Get store for main log
	store1, err := manager.GetStore("did:key:z6MkMain")
	require.NoError(t, err)
	require.NotNil(t, store1)

	// Get same store again - should be cached
	store2, err := manager.GetStore("did:key:z6MkMain")
	require.NoError(t, err)
	assert.Same(t, store1, store2)
}

func TestStoreManager_RevocationsSuffix(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-manager-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := sqlite.NewStoreManager(tmpDir)
	defer manager.CloseAll()

	// Get store for main log
	storeMain, err := manager.GetStore("did:key:z6MkMain")
	require.NoError(t, err)

	// Get store via revocations suffix - should be same store
	storeRev, err := manager.GetStore("did:key:z6MkMain-revocations")
	require.NoError(t, err)

	assert.Same(t, storeMain, storeRev)
	assert.Equal(t, "did:key:z6MkMain", storeRev.LogDID())
}

func TestStoreManager_MultipleStores(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-manager-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := sqlite.NewStoreManager(tmpDir)
	defer manager.CloseAll()

	store1, err := manager.GetStore("did:key:z6MkLog1")
	require.NoError(t, err)

	store2, err := manager.GetStore("did:key:z6MkLog2")
	require.NoError(t, err)

	assert.NotSame(t, store1, store2)
	assert.Equal(t, "did:key:z6MkLog1", store1.LogDID())
	assert.Equal(t, "did:key:z6MkLog2", store2.LogDID())
}

func TestStoreManager_CloseAll(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "sqlite-manager-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := sqlite.NewStoreManager(tmpDir)

	_, err = manager.GetStore("did:key:z6MkLog1")
	require.NoError(t, err)

	_, err = manager.GetStore("did:key:z6MkLog2")
	require.NoError(t, err)

	err = manager.CloseAll()
	assert.NoError(t, err)
}
