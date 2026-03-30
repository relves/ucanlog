package server_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/relves/ucanlog/internal/storage/sqlite"
	"github.com/relves/ucanlog/pkg/server"
)

func TestHandleGetHead_Success(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "http-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := sqlite.NewStoreManager(tmpDir)
	defer manager.CloseAll()

	// Setup test data
	ctx := context.Background()
	logDID := "did:key:z6MkTestLog"

	store, err := manager.GetStore(logDID)
	require.NoError(t, err)
	require.NoError(t, store.CreateLogRecord(ctx, logDID))

	// Populate latest_head_car (written on every append)
	require.NoError(t, store.SetLatestHeadCAR(ctx, logDID, 42, "bafyTestHead", []byte{0x01}))

	// Create handler
	handler := server.NewHTTPHandler(manager)

	// Create request with path parameter
	req := httptest.NewRequest("GET", "/logs/"+logDID+"/head", nil)
	req.SetPathValue("logID", logDID)
	w := httptest.NewRecorder()

	handler.HandleGetHead(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "bafyTestHead", resp["index_cid"])
	assert.Equal(t, float64(42), resp["tree_size"])
}

func TestHandleGetHead_NotFound(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "http-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	manager := sqlite.NewStoreManager(tmpDir)
	defer manager.CloseAll()

	handler := server.NewHTTPHandler(manager)

	req := httptest.NewRequest("GET", "/logs/did:key:z6MkNonexistent/head", nil)
	req.SetPathValue("logID", "did:key:z6MkNonexistent")
	w := httptest.NewRecorder()

	handler.HandleGetHead(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}
