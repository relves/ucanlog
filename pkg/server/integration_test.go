//go:build integration

package server_test

import (
	"context"
	"encoding/base64"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/relves/ucanlog/internal/storage/sqlite"
	"github.com/relves/ucanlog/pkg/capabilities"
)

func TestGetHead_FromSeparateTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "integration-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := sqlite.NewStoreManager(tmpDir)
	defer manager.CloseAll()

	ctx := context.Background()
	logDID := "did:key:z6MkIntegrationTest"

	// Setup: create log
	store, err := manager.GetStore(logDID)
	require.NoError(t, err)
	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	// Scenario 1: GetHead returns empty values when no data exists
	t.Run("EmptyWhenNoData", func(t *testing.T) {
		indexCID, treeSize, err := store.GetHead(ctx, logDID)
		require.NoError(t, err)
		assert.Equal(t, "", indexCID)
		assert.Equal(t, uint64(0), treeSize)
	})

	// Scenario 2: GetHead reads from tree_state and index_persistence
	t.Run("ReadsFromSeparateTables", func(t *testing.T) {
		// Set tree state
		err = store.SetTreeState(ctx, logDID, 42, []byte{0x01, 0x02})
		require.NoError(t, err)

		// Set index persistence
		uploadTime := time.Now().UTC()
		err = store.SetIndexPersistence(ctx, logDID, uploadTime, 1024, "bafyHead1")
		require.NoError(t, err)

		// GetHead should read from both tables
		indexCID, treeSize, err := store.GetHead(ctx, logDID)
		require.NoError(t, err)
		assert.Equal(t, "bafyHead1", indexCID)
		assert.Equal(t, uint64(42), treeSize)
	})

	// Scenario 3: Updates to tree_state are reflected in GetHead
	t.Run("TreeStateUpdatesReflected", func(t *testing.T) {
		err = store.SetTreeState(ctx, logDID, 100, []byte{0x03})
		require.NoError(t, err)

		indexCID, treeSize, err := store.GetHead(ctx, logDID)
		require.NoError(t, err)
		assert.Equal(t, "bafyHead1", indexCID) // Unchanged
		assert.Equal(t, uint64(100), treeSize) // Updated
	})

	// Scenario 4: Updates to index_persistence are reflected in GetHead
	t.Run("IndexPersistenceUpdatesReflected", func(t *testing.T) {
		uploadTime := time.Now().UTC()
		err = store.SetIndexPersistence(ctx, logDID, uploadTime, 2048, "bafyHead2")
		require.NoError(t, err)

		indexCID, treeSize, err := store.GetHead(ctx, logDID)
		require.NoError(t, err)
		assert.Equal(t, "bafyHead2", indexCID) // Updated
		assert.Equal(t, uint64(100), treeSize) // Unchanged
	})

	// Scenario 5: Simulate optimistic concurrency check with stale head
	t.Run("OptimisticConcurrency_StaleHead", func(t *testing.T) {
		// Current state: indexCID="bafyHead2", treeSize=100
		currentIndexCID, treeSize, err := store.GetHead(ctx, logDID)
		require.NoError(t, err)
		assert.Equal(t, "bafyHead2", currentIndexCID)
		assert.Equal(t, uint64(100), treeSize)

		// Simulate client trying to append with stale head "bafyHead1"
		// This would fail the optimistic concurrency check in appendHandler
		staleIndexCID := "bafyHead1"
		expectedIndexCID := currentIndexCID

		// The check that happens in appendHandler:
		// if currentIndexCID != expectedIndexCID { return HeadMismatch error }
		if currentIndexCID != staleIndexCID {
			// This demonstrates the conflict detection
			// In real handler, this would return a HeadMismatch error
			t.Logf("Head mismatch detected: expected %s but current is %s (tree size: %d)",
				staleIndexCID, currentIndexCID, treeSize)
		}
		assert.NotEqual(t, staleIndexCID, expectedIndexCID, "stale head should not match current")
	})
}

func TestCaveats_IndexCID(t *testing.T) {
	// Verify the updated caveats structure works
	indexCID := "bafyExpectedHead"
	caveats := capabilities.AppendCaveats{
		Data:       base64.StdEncoding.EncodeToString([]byte("test data")),
		IndexCID:   &indexCID,
		Delegation: "base64encodeddelegation",
	}

	assert.Equal(t, "bafyExpectedHead", *caveats.IndexCID)
	assert.NotEmpty(t, caveats.Delegation) // Delegation is required (no omitempty)
}

func TestCreateSuccess_Structure(t *testing.T) {
	// Verify CreateSuccess has the correct fields
	success := capabilities.CreateSuccess{
		LogID:    "did:key:z6MkTestLog",
		IndexCID: "bafyInitialHead",
		TreeSize: 0,
	}

	assert.Equal(t, "did:key:z6MkTestLog", success.LogID)
	assert.Equal(t, "bafyInitialHead", success.IndexCID)
	assert.Equal(t, uint64(0), success.TreeSize)
}

func TestAppendSuccess_Structure(t *testing.T) {
	// Verify AppendSuccess has the correct fields
	success := capabilities.AppendSuccess{
		Index:       42,
		NewIndexCID: "bafyNewHead",
		TreeSize:    43,
	}

	assert.Equal(t, int64(42), success.Index)
	assert.Equal(t, "bafyNewHead", success.NewIndexCID)
	assert.Equal(t, uint64(43), success.TreeSize)
}
