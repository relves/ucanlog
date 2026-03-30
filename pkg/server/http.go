package server

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"sort"
	"time"

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

// LogSummary is an entry in the response for GET /logs.
type LogSummary struct {
	LogID      string    `json:"logID"`
	UpdatedAt  time.Time `json:"updatedAt"`
	TreeSize   uint64    `json:"treeSize"`
	HeadCID    string    `json:"headCID"`
	RevHeadCID string    `json:"revHeadCID,omitempty"`
}

// HandleListLogs handles GET /logs.
// Returns a JSON array of all known logs ordered by updatedAt descending.
func (h *HTTPHandler) HandleListLogs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	dids, err := h.storeManager.ListLogDIDs()
	if err != nil {
		slog.Error("failed to list log DIDs", "error", err)
		http.Error(w, "failed to list logs", http.StatusInternalServerError)
		return
	}

	summaries := make([]LogSummary, 0, len(dids))
	for _, logID := range dids {
		store, err := h.storeManager.GetStore(logID)
		if err != nil {
			slog.Warn("failed to get store for log", "logID", logID, "error", err)
			continue
		}

		record, err := store.GetLogRecord(ctx, logID)
		if err != nil {
			slog.Warn("failed to get log record", "logID", logID, "error", err)
			continue
		}

		headCID, treeSize, err := store.GetHead(ctx, logID)
		if err != nil {
			slog.Warn("failed to get head", "logID", logID, "error", err)
			continue
		}

		revHeadCID, _, _, err := store.GetLatestHeadCAR(ctx, logID+"-revocations")
		if err != nil && !errors.Is(err, sqlite.ErrNotFound) {
			slog.Warn("failed to get revocations head", "logID", logID, "error", err)
		}

		summaries = append(summaries, LogSummary{
			LogID:      logID,
			UpdatedAt:  record.UpdatedAt,
			TreeSize:   treeSize,
			HeadCID:    headCID,
			RevHeadCID: revHeadCID,
		})
	}

	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].UpdatedAt.After(summaries[j].UpdatedAt)
	})

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summaries)
}

// HeadResponse is the response for GET /logs/{logID}/head.
type HeadResponse struct {
	IndexCID string `json:"index_cid"`
	TreeSize uint64 `json:"tree_size"`
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

	indexCID, treeSize, err := store.GetHead(ctx, logID)
	if err != nil {
		slog.Error("failed to get head", "logID", logID, "error", err)
		http.Error(w, "failed to get head", http.StatusInternalServerError)
		return
	}

	resp := HeadResponse{
		IndexCID: indexCID,
		TreeSize: treeSize,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
