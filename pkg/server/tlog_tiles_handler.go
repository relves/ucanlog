package server

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/relves/ucanlog/pkg/log"
)

// TlogTilesHandler provides public HTTP GET endpoints for the tlog-tiles API
// as specified in https://github.com/C2SP/C2SP/blob/main/tlog-tiles.md
type TlogTilesHandler struct {
	logService *log.LogService
}

// NewTlogTilesHandler creates a new handler for tlog-tiles API endpoints
func NewTlogTilesHandler(logService *log.LogService) *TlogTilesHandler {
	return &TlogTilesHandler{
		logService: logService,
	}
}

// HandleCheckpoint serves GET /logs/<logID>/checkpoint
func (h *TlogTilesHandler) HandleCheckpoint(w http.ResponseWriter, r *http.Request) {
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

	checkpoint, err := h.logService.ReadCheckpoint(context.Background(), logID)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read checkpoint: %v", err), http.StatusNotFound)
		return
	}

	// Set headers per spec: text/plain with short-lived cache
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=5")
	w.WriteHeader(http.StatusOK)
	w.Write(checkpoint)
}

// HandleTile serves GET /logs/<logID>/tile/<L>/<N>[.p/<W>]
func (h *TlogTilesHandler) HandleTile(w http.ResponseWriter, r *http.Request) {
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

	// Parse level
	level, err := strconv.ParseUint(levelStr, 10, 64)
	if err != nil || level > 63 {
		http.Error(w, "invalid level (must be 0-63)", http.StatusBadRequest)
		return
	}

	// Parse tile path and partial width
	index, partialWidth, err := parseTilePath(tilePath)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid tile path: %v", err), http.StatusBadRequest)
		return
	}

	// Read tile from Tessera
	tile, err := h.logService.ReadTile(context.Background(), logID, level, index, partialWidth)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read tile: %v", err), http.StatusNotFound)
		return
	}

	// Set headers per spec: binary with immutable cache
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.WriteHeader(http.StatusOK)
	w.Write(tile)
}

// HandleEntries serves GET /logs/<logID>/tile/entries/<N>[.p/<W>]
func (h *TlogTilesHandler) HandleEntries(w http.ResponseWriter, r *http.Request) {
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

	// Parse entry path and partial width
	index, partialWidth, err := parseTilePath(entryPath)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid entry path: %v", err), http.StatusBadRequest)
		return
	}

	// Read entry bundle from Tessera
	bundle, err := h.logService.ReadEntryBundle(context.Background(), logID, index, partialWidth)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read entry bundle: %v", err), http.StatusNotFound)
		return
	}

	// Set headers per spec: binary with immutable cache
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.WriteHeader(http.StatusOK)
	w.Write(bundle)

	// Note: HTTP compression should be handled by the transport layer (e.g., via middleware),
	// not by manually setting Content-Encoding without actually compressing the data
}

// parseTilePath parses a tile path in the format: x000/x001/234 or x000/x001/234.p/128
// Returns the tile index and partial width (0 for full tiles)
func parseTilePath(path string) (index uint64, partialWidth uint8, err error) {
	// Check for partial tile suffix
	parts := strings.Split(path, ".p/")
	basePath := parts[0]

	if len(parts) == 2 {
		// Parse partial width
		w, err := strconv.ParseUint(parts[1], 10, 8)
		if err != nil || w == 0 || w > 255 {
			return 0, 0, fmt.Errorf("invalid partial width (must be 1-255)")
		}
		partialWidth = uint8(w)
	}

	// Parse the zero-padded path segments
	// Format: x000/x001/234 -> index 1234
	// Each segment is 3 digits, zero-padded with 'x' prefix
	segments := strings.Split(basePath, "/")

	// Build the index from segments
	indexStr := ""
	for _, seg := range segments {
		// Remove 'x' prefix if present
		seg = strings.TrimPrefix(seg, "x")

		// Validate it's a 3-digit number
		if len(seg) != 3 {
			return 0, 0, fmt.Errorf("invalid path segment (must be 3 digits): %s", seg)
		}

		indexStr += seg
	}

	// Parse the full index
	index, err = strconv.ParseUint(indexStr, 10, 64)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid index: %w", err)
	}

	return index, partialWidth, nil
}

// isValidLogID validates a logID to prevent path traversal attacks
func isValidLogID(logID string) bool {
	// LogID should not contain path separators or special characters
	// Allow alphanumeric, hyphens, underscores, and colons (for DID format)
	if strings.ContainsAny(logID, "/\\.") {
		return false
	}
	return len(logID) > 0 && len(logID) < 256
}
