package sqlite

import (
	"context"
	"database/sql"

	"github.com/relves/ucanlog/internal/storage"
)

// Ensure LogStore implements StateStore at compile time.
var _ storage.StateStore = (*LogStore)(nil)

// GetHead returns the current head CID and tree size for a log.
// Queries tree_state for size and index_persistence for the CID.
// Implements storage.StateStore interface.
func (s *LogStore) GetHead(ctx context.Context, logDID string) (string, uint64, error) {
	// Get tree size from tree_state table
	var treeSize uint64
	err := s.db.QueryRowContext(ctx,
		`SELECT COALESCE(size, 0) FROM tree_state WHERE log_did = ?`,
		logDID).Scan(&treeSize)
	if err != nil && err != sql.ErrNoRows {
		return "", 0, err
	}
	// If no rows, treeSize remains 0 (default)

	// Get index CID from index_persistence table
	var indexCID string
	err = s.db.QueryRowContext(ctx,
		`SELECT COALESCE(last_uploaded_cid, '') FROM index_persistence WHERE log_did = ?`,
		logDID).Scan(&indexCID)
	if err != nil && err != sql.ErrNoRows {
		return "", 0, err
	}
	// If no rows, indexCID remains empty (default)

	return indexCID, treeSize, nil
}
