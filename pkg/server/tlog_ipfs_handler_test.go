package server_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/relves/ucanlog/pkg/server"
)

// stubCIDStore implements tlog.CIDStore for tests.
type stubCIDStore struct {
	cids map[string]string // logID -> CID
}

func (s *stubCIDStore) GetLatestCID(logID string) (string, error) {
	cid, ok := s.cids[logID]
	if !ok {
		return "", fmt.Errorf("no index CID found for log %s", logID)
	}
	return cid, nil
}

func (s *stubCIDStore) SetLatestCID(logID string, cid string) error {
	s.cids[logID] = cid
	return nil
}

const testLogID = "did:key:z6MkTlogTest"
const testRootCID = "bafybeifkwve4mu5g5api5qn2dvdvlusqxb5eaz7cboyrymyu62wdc73guu"

// newGatewayServer starts a fake IPFS gateway and returns it plus a handler
// that records which path was requested.
func newGatewayServer(t *testing.T, body string) (*httptest.Server, *string) {
	t.Helper()
	var lastPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, &lastPath
}

func TestHandleCheckpoint_Success(t *testing.T) {
	gateway, lastPath := newGatewayServer(t, "checkpoint-data")
	cidStore := &stubCIDStore{cids: map[string]string{testLogID: testRootCID}}
	h := server.NewTlogIPFSHandler(cidStore, gateway.URL, nil)

	req := httptest.NewRequest("GET", "/logs/"+testLogID+"/checkpoint", nil)
	req.SetPathValue("logID", testLogID)
	w := httptest.NewRecorder()

	h.HandleCheckpoint(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "checkpoint-data", w.Body.String())
	assert.Equal(t, "/ipfs/"+testRootCID+"/checkpoint", *lastPath)
	assert.Equal(t, "text/plain; charset=utf-8", w.Header().Get("Content-Type"))
}

func TestHandleCheckpoint_NoCID(t *testing.T) {
	cidStore := &stubCIDStore{cids: map[string]string{}}
	h := server.NewTlogIPFSHandler(cidStore, "http://unused", nil)

	req := httptest.NewRequest("GET", "/logs/"+testLogID+"/checkpoint", nil)
	req.SetPathValue("logID", testLogID)
	w := httptest.NewRecorder()

	h.HandleCheckpoint(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleTile_Success(t *testing.T) {
	gateway, lastPath := newGatewayServer(t, "tile-data")
	cidStore := &stubCIDStore{cids: map[string]string{testLogID: testRootCID}}
	h := server.NewTlogIPFSHandler(cidStore, gateway.URL, nil)

	req := httptest.NewRequest("GET", "/logs/"+testLogID+"/tile/0/x000/123", nil)
	req.SetPathValue("logID", testLogID)
	req.SetPathValue("level", "0")
	req.SetPathValue("tilePath", "x000/123")
	w := httptest.NewRecorder()

	h.HandleTile(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "tile-data", w.Body.String())
	assert.Equal(t, "/ipfs/"+testRootCID+"/tile/0/x000/123", *lastPath)
	assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "public, max-age=31536000, immutable", w.Header().Get("Cache-Control"))
}

func TestHandleTile_NoCID(t *testing.T) {
	cidStore := &stubCIDStore{cids: map[string]string{}}
	h := server.NewTlogIPFSHandler(cidStore, "http://unused", nil)

	req := httptest.NewRequest("GET", "/logs/"+testLogID+"/tile/0/x000/123", nil)
	req.SetPathValue("logID", testLogID)
	req.SetPathValue("level", "0")
	req.SetPathValue("tilePath", "x000/123")
	w := httptest.NewRecorder()

	h.HandleTile(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleEntries_Success(t *testing.T) {
	gateway, lastPath := newGatewayServer(t, "entries-data")
	cidStore := &stubCIDStore{cids: map[string]string{testLogID: testRootCID}}
	h := server.NewTlogIPFSHandler(cidStore, gateway.URL, nil)

	req := httptest.NewRequest("GET", "/logs/"+testLogID+"/tile/entries/x000/001", nil)
	req.SetPathValue("logID", testLogID)
	req.SetPathValue("entryPath", "x000/001")
	w := httptest.NewRecorder()

	h.HandleEntries(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "entries-data", w.Body.String())
	assert.Equal(t, "/ipfs/"+testRootCID+"/tile/entries/x000/001", *lastPath)
	assert.Equal(t, "application/octet-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "public, max-age=31536000, immutable", w.Header().Get("Cache-Control"))
}

func TestHandleEntries_NoCID(t *testing.T) {
	cidStore := &stubCIDStore{cids: map[string]string{}}
	h := server.NewTlogIPFSHandler(cidStore, "http://unused", nil)

	req := httptest.NewRequest("GET", "/logs/"+testLogID+"/tile/entries/x000/001", nil)
	req.SetPathValue("logID", testLogID)
	req.SetPathValue("entryPath", "x000/001")
	w := httptest.NewRecorder()

	h.HandleEntries(w, req)

	assert.Equal(t, http.StatusNotFound, w.Code)
}

func TestHandleCheckpoint_GatewayError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	cidStore := &stubCIDStore{cids: map[string]string{testLogID: testRootCID}}
	h := server.NewTlogIPFSHandler(cidStore, srv.URL, nil)

	req := httptest.NewRequest("GET", "/logs/"+testLogID+"/checkpoint", nil)
	req.SetPathValue("logID", testLogID)
	w := httptest.NewRecorder()

	h.HandleCheckpoint(w, req)

	assert.Equal(t, http.StatusBadGateway, w.Code)
}
