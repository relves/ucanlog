package tlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeSpaceDID(t *testing.T) {
	t.Run("replaces colons with underscores", func(t *testing.T) {
		input := "did:key:z6MkTest123"
		expected := "did_key_z6MkTest123"
		assert.Equal(t, expected, sanitizeSpaceDID(input))
	})

	t.Run("handles multiple colons", func(t *testing.T) {
		input := "did:key:z6Mk:extra:parts"
		expected := "did_key_z6Mk_extra_parts"
		assert.Equal(t, expected, sanitizeSpaceDID(input))
	})

	t.Run("no change for string without colons", func(t *testing.T) {
		input := "simple-id-no-colons"
		assert.Equal(t, input, sanitizeSpaceDID(input))
	})
}

func TestLogPath(t *testing.T) {
	t.Run("creates correct path with sanitized DID", func(t *testing.T) {
		basePath := "/data/logs"
		spaceDID := "did:key:z6MkTest123"
		
		result := logPath(basePath, spaceDID)
		expected := "/data/logs/logs/did_key_z6MkTest123"
		assert.Equal(t, expected, result)
	})
}

func TestRevocationLogPath(t *testing.T) {
	t.Run("creates correct revocation path", func(t *testing.T) {
		basePath := "/data/logs"
		spaceDID := "did:key:z6MkTest123"
		
		result := revocationLogPath(basePath, spaceDID)
		expected := "/data/logs/logs/did_key_z6MkTest123-revocations"
		assert.Equal(t, expected, result)
	})
}

func TestLogMetaStore(t *testing.T) {
	// Create temp directory for tests
	tmpDir, err := os.MkdirTemp("", "logmeta-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store := NewLogMetaStore(tmpDir)

	t.Run("Create and Get", func(t *testing.T) {
		logID := "test-log-1"
		logDID := "did:key:z6MkTest1"
		spaceDID := "did:key:z6MkSpace1"

		meta, err := store.Create(logID, logDID, spaceDID)
		require.NoError(t, err)

		assert.Equal(t, logDID, meta.LogDID)
		assert.Equal(t, spaceDID, meta.SpaceDID)
		assert.Equal(t, uint64(0), meta.TreeSize)
		assert.False(t, meta.CreatedAt.IsZero())
		assert.False(t, meta.UpdatedAt.IsZero())

		// Get should return the same data
		retrieved, err := store.Get(logID)
		require.NoError(t, err)
		assert.Equal(t, meta.LogDID, retrieved.LogDID)
		assert.Equal(t, meta.SpaceDID, retrieved.SpaceDID)
	})

	t.Run("Get non-existent log", func(t *testing.T) {
		_, err := store.Get("non-existent-log")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("Exists", func(t *testing.T) {
		logID := "test-log-exists"
		assert.False(t, store.Exists(logID))

		_, err := store.Create(logID, "did:key:z6MkTest", "did:key:z6MkSpace")
		require.NoError(t, err)

		assert.True(t, store.Exists(logID))
	})

	t.Run("UpdateHead", func(t *testing.T) {
		logID := "test-log-update-head"
		_, err := store.Create(logID, "did:key:z6MkTest", "did:key:z6MkSpace")
		require.NoError(t, err)

		err = store.UpdateHead(logID, "bafytest123", 42)
		require.NoError(t, err)

		meta, err := store.Get(logID)
		require.NoError(t, err)
		assert.Equal(t, "bafytest123", meta.HeadIndexCID)
		assert.Equal(t, uint64(42), meta.TreeSize)
	})

	t.Run("Delete", func(t *testing.T) {
		logID := "test-log-delete"
		_, err := store.Create(logID, "did:key:z6MkTest", "did:key:z6MkSpace")
		require.NoError(t, err)

		assert.True(t, store.Exists(logID))

		err = store.Delete(logID)
		require.NoError(t, err)

		assert.False(t, store.Exists(logID))
	})

	t.Run("Cache works correctly", func(t *testing.T) {
		logID := "test-log-cache"
		_, err := store.Create(logID, "did:key:z6MkTest", "did:key:z6MkSpace")
		require.NoError(t, err)

		// First get populates cache
		meta1, err := store.Get(logID)
		require.NoError(t, err)

		// Second get should return cached value
		meta2, err := store.Get(logID)
		require.NoError(t, err)

		assert.Equal(t, meta1.LogDID, meta2.LogDID)
	})

	t.Run("Persistence across store instances", func(t *testing.T) {
		logID := "test-log-persist"
		_, err := store.Create(logID, "did:key:z6MkTest", "did:key:z6MkSpace")
		require.NoError(t, err)

		// Create new store instance with same base path
		store2 := NewLogMetaStore(tmpDir)

		// Should be able to read persisted data
		meta, err := store2.Get(logID)
		require.NoError(t, err)
		assert.Equal(t, "did:key:z6MkTest", meta.LogDID)
	})

	t.Run("UpdatedAt changes on save", func(t *testing.T) {
		logID := "test-log-timestamp"
		meta, err := store.Create(logID, "did:key:z6MkTest", "did:key:z6MkSpace")
		require.NoError(t, err)

		originalUpdatedAt := meta.UpdatedAt

		// Wait a tiny bit
		time.Sleep(10 * time.Millisecond)

		err = store.UpdateHead(logID, "bafynew", 10)
		require.NoError(t, err)

		meta, err = store.Get(logID)
		require.NoError(t, err)

		assert.True(t, meta.UpdatedAt.After(originalUpdatedAt))
	})
}

func TestLogMetaStorePaths(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "logmeta-path-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	store := NewLogMetaStore(tmpDir)

	logID := "my-test-log"
	_, err = store.Create(logID, "did:key:z6MkTest", "did:key:z6MkSpace")
	require.NoError(t, err)

	// Verify file is created at expected path
	expectedPath := filepath.Join(tmpDir, "logs", logID, "meta.json")
	_, err = os.Stat(expectedPath)
	assert.NoError(t, err, "meta.json should exist at expected path")
}
