package sqlite

import (
	"context"
	"database/sql"

	"github.com/relves/ucanlog/internal/storage"
)

// Ensure LogStore implements StateStore at compile time.
var _ storage.StateStore = (*LogStore)(nil)

// GetHead returns the current head CID and tree size for a log.
// Reads from latest_head_car, which is updated synchronously on every append.
// Returns ("", 0, nil) if no record exists yet (log has never had an entry appended).
// Implements storage.StateStore interface.
func (s *LogStore) GetHead(ctx context.Context, logDID string) (string, uint64, error) {
	var headCID string
	var treeSize int64
	err := s.db.QueryRowContext(ctx,
		`SELECT head_cid, tree_size FROM latest_head_car WHERE log_did = ?`,
		logDID).Scan(&headCID, &treeSize)
	if err == nil {
		return headCID, uint64(treeSize), nil
	}
	if err == sql.ErrNoRows {
		return "", 0, nil
	}
	return "", 0, err
}
