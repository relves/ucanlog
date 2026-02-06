package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/relves/ucanlog/internal/storage/sqlite"
)

// HTTPHandler handles HTTP endpoints for log queries.
type HTTPHandler struct {
	storeManager *sqlite.StoreManager
}

// NewHTTPHandler creates a new HTTP handler.
func NewHTTPHandler(storeManager *sqlite.StoreManager) *HTTPHandler {
	return &HTTPHandler{
		storeManager: storeManager,
	}
}

// HeadResponse is the response for GET /logs/{logID}/head.
type HeadResponse struct {
	IndexCID      string `json:"index_cid"`
	TreeSize      uint64 `json:"tree_size"`
	CheckpointCID string `json:"checkpoint_cid,omitempty"`
}

// HandleGetHead handles GET /logs/{logID}/head.
// Returns the current head CID, tree size, and optional checkpoint CID.
func (h *HTTPHandler) HandleGetHead(w http.ResponseWriter, r *http.Request) {
	logID := r.PathValue("logID")
	if logID == "" {
		http.Error(w, "logID required", http.StatusBadRequest)
		return
	}

	store, err := h.storeManager.GetStore(logID)
	if err != nil {
		slog.Error("failed to get store", "logID", logID, "error", err)
		http.Error(w, "failed to get store", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()

	// Check if log exists
	_, err = store.GetLogRecord(ctx, logID)
	if err != nil {
		if errors.Is(err, sqlite.ErrNotFound) {
			http.Error(w, "log not found", http.StatusNotFound)
			return
		}
		slog.Error("failed to get log record", "logID", logID, "error", err)
		http.Error(w, "failed to get log record", http.StatusInternalServerError)
		return
	}

	// Get head info from tree_state and index_persistence tables
	indexCID, treeSize, err := store.GetHead(ctx, logID)
	if err != nil {
		slog.Error("failed to get head", "logID", logID, "error", err)
		http.Error(w, "failed to get head", http.StatusInternalServerError)
		return
	}

	cidIndex, err := store.GetCIDIndex(ctx, logID)
	if err != nil {
		slog.Error("failed to get CID index", "logID", logID, "error", err)
		http.Error(w, "failed to get CID index", http.StatusInternalServerError)
		return
	}

	resp := HeadResponse{
		IndexCID:      indexCID,
		TreeSize:      treeSize,
		CheckpointCID: cidIndex["checkpoint"],
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
