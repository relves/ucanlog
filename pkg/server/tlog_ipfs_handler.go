package server

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/relves/ucanlog/pkg/tlog"
)

// TlogIPFSHandler provides public HTTP GET endpoints for the tlog-tiles API
// by proxying requests to IPFS gateway using the latest index CAR root CID.
type TlogIPFSHandler struct {
	cidStore   tlog.CIDStore
	gatewayURL string // e.g., "https://ipfs.w3s.link"
	httpClient *http.Client
}

// NewTlogIPFSHandler creates a new IPFS-backed handler for tlog-tiles API endpoints.
func NewTlogIPFSHandler(cidStore tlog.CIDStore, gatewayURL string, httpClient *http.Client) *TlogIPFSHandler {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &TlogIPFSHandler{
		cidStore:   cidStore,
		gatewayURL: gatewayURL,
		httpClient: httpClient,
	}
}

// HandleCheckpoint serves GET /logs/<logID>/checkpoint by proxying to IPFS gateway.
func (h *TlogIPFSHandler) HandleCheckpoint(w http.ResponseWriter, r *http.Request) {
	logID := r.PathValue("logID")
	if logID == "" {
		http.Error(w, "missing logID", http.StatusBadRequest)
		return
	}

	// Validate logID to prevent path traversal
	if !isValidLogID(logID) {
		http.Error(w, "invalid logID", http.StatusBadRequest)
		return
	}

	// Get latest index CAR root CID
	rootCID, err := h.cidStore.GetLatestCID(logID)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get index CID: %v", err), http.StatusNotFound)
		return
	}

	// Fetch from IPFS gateway
	url := fmt.Sprintf("%s/ipfs/%s/checkpoint", h.gatewayURL, rootCID)
	if err := h.proxyFromGateway(w, r.Context(), url, "text/plain; charset=utf-8", "public, max-age=5"); err != nil {
		http.Error(w, fmt.Sprintf("failed to fetch checkpoint: %v", err), http.StatusBadGateway)
		return
	}
}

// HandleTile serves GET /logs/<logID>/tile/<L>/<tilePath> by proxying to IPFS gateway.
func (h *TlogIPFSHandler) HandleTile(w http.ResponseWriter, r *http.Request) {
	logID := r.PathValue("logID")
	levelStr := r.PathValue("level")
	tilePath := r.PathValue("tilePath")

	if logID == "" || levelStr == "" || tilePath == "" {
		http.Error(w, "missing required path parameters", http.StatusBadRequest)
		return
	}

	// Validate logID
	if !isValidLogID(logID) {
		http.Error(w, "invalid logID", http.StatusBadRequest)
		return
	}

	// Get latest index CAR root CID
	rootCID, err := h.cidStore.GetLatestCID(logID)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get index CID: %v", err), http.StatusNotFound)
		return
	}

	// Build IPFS gateway URL preserving the tile path exactly as received
	// tilePath includes the x000/x001/234 format and optional .p/128 suffix
	url := fmt.Sprintf("%s/ipfs/%s/tile/%s/%s", h.gatewayURL, rootCID, levelStr, tilePath)
	if err := h.proxyFromGateway(w, r.Context(), url, "application/octet-stream", "public, max-age=31536000, immutable"); err != nil {
		http.Error(w, fmt.Sprintf("failed to fetch tile: %v", err), http.StatusBadGateway)
		return
	}
}

// HandleEntries serves GET /logs/<logID>/tile/entries/<entryPath> by proxying to IPFS gateway.
func (h *TlogIPFSHandler) HandleEntries(w http.ResponseWriter, r *http.Request) {
	logID := r.PathValue("logID")
	entryPath := r.PathValue("entryPath")

	if logID == "" || entryPath == "" {
		http.Error(w, "missing required path parameters", http.StatusBadRequest)
		return
	}

	// Validate logID
	if !isValidLogID(logID) {
		http.Error(w, "invalid logID", http.StatusBadRequest)
		return
	}

	// Get latest index CAR root CID
	rootCID, err := h.cidStore.GetLatestCID(logID)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to get index CID: %v", err), http.StatusNotFound)
		return
	}

	// Build IPFS gateway URL preserving the entry path exactly as received
	url := fmt.Sprintf("%s/ipfs/%s/tile/entries/%s", h.gatewayURL, rootCID, entryPath)
	if err := h.proxyFromGateway(w, r.Context(), url, "application/octet-stream", "public, max-age=31536000, immutable"); err != nil {
		http.Error(w, fmt.Sprintf("failed to fetch entry bundle: %v", err), http.StatusBadGateway)
		return
	}
}

// proxyFromGateway fetches data from IPFS gateway and streams it to the response.
func (h *TlogIPFSHandler) proxyFromGateway(w http.ResponseWriter, ctx context.Context, url string, contentType string, cacheControl string) error {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("gateway request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gateway returned status %d", resp.StatusCode)
	}

	// Set response headers per tlog-tiles spec
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Cache-Control", cacheControl)
	w.WriteHeader(http.StatusOK)

	// Stream response body
	_, err = io.Copy(w, resp.Body)
	return err
}
