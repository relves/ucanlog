package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/relves/ucanlog/internal/storage"
	_ "modernc.org/sqlite"
)

// Ensure LogStore also implements QueueStore.
var _ storage.QueueStore = (*LogStore)(nil)

//go:embed schema.sql
var schemaSQL string

type LogStore struct {
	db     *sql.DB
	logDID string
	dbPath string
}

func OpenLogStore(basePath, logDID string) (*LogStore, error) {
	mainLogDID := strings.TrimSuffix(logDID, "-revocations")
	logDir := filepath.Join(basePath, "logs", mainLogDID)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("create log directory: %w", err)
	}

	dbPath := filepath.Join(logDir, "log.db")
	db, err := sql.Open("sqlite", dbPath+
		"?_pragma=journal_mode(WAL)"+
		"&_pragma=foreign_keys(ON)"+
		"&_pragma=busy_timeout(5000)"+ // Wait up to 5s on lock instead of returning SQLITE_BUSY immediately
		"&_pragma=synchronous(NORMAL)"+ // Balance safety/speed (FULL is slower, OFF risks corruption)
		"&_pragma=wal_autocheckpoint(1000)") // Checkpoint every 1000 pages to prevent WAL accumulation
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Limit connection pool - SQLite handles concurrent writes poorly
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(time.Hour)

	if _, err := db.Exec(schemaSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize schema: %w", err)
	}

	return &LogStore{
		db:     db,
		logDID: mainLogDID,
		dbPath: dbPath,
	}, nil
}

func (s *LogStore) Close() error {
	return s.db.Close()
}

func (s *LogStore) LogDID() string {
	return s.logDID
}

func (s *LogStore) DBPath() string {
	return s.dbPath
}

var (
	ErrNotFound     = errors.New("not found")
	ErrHeadMismatch = errors.New("head mismatch")
)

type LogRecord struct {
	LogDID    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (s *LogStore) CreateLogRecord(ctx context.Context, logDID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO logs (log_did, created_at, updated_at)
		 VALUES (?, ?, ?)`,
		logDID, now, now)
	return err
}

// ListLogDIDs returns all log DIDs stored in this database.
func (s *LogStore) ListLogDIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT log_did FROM logs ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var dids []string
	for rows.Next() {
		var did string
		if err := rows.Scan(&did); err != nil {
			return nil, err
		}
		dids = append(dids, did)
	}
	return dids, rows.Err()
}

func (s *LogStore) GetLogRecord(ctx context.Context, logDID string) (*LogRecord, error) {
	var record LogRecord
	var createdAt, updatedAt string

	err := s.db.QueryRowContext(ctx,
		`SELECT log_did, created_at, updated_at
		 FROM logs WHERE log_did = ?`,
		logDID).Scan(&record.LogDID, &createdAt, &updatedAt)

	if err == sql.ErrNoRows {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	var parseErr error
	record.CreatedAt, parseErr = time.Parse(time.RFC3339, createdAt)
	if parseErr != nil {
		slog.Warn("failed to parse created_at timestamp", "logDID", logDID, "value", createdAt, "error", parseErr)
	}
	record.UpdatedAt, parseErr = time.Parse(time.RFC3339, updatedAt)
	if parseErr != nil {
		slog.Warn("failed to parse updated_at timestamp", "logDID", logDID, "value", updatedAt, "error", parseErr)
	}

	return &record, nil
}

// GetTreeState retrieves the Merkle tree state for a log.
// Returns (0, nil, nil) if no tree state exists yet.
func (s *LogStore) GetTreeState(ctx context.Context, logDID string) (size uint64, root []byte, err error) {
	err = s.db.QueryRowContext(ctx,
		`SELECT size, root FROM tree_state WHERE log_did = ?`,
		logDID).Scan(&size, &root)

	if err == sql.ErrNoRows {
		return 0, nil, nil
	}
	if err != nil {
		return 0, nil, err
	}

	return size, root, nil
}

// SetTreeState sets the Merkle tree state for a log (upsert).
func (s *LogStore) SetTreeState(ctx context.Context, logDID string, size uint64, root []byte) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tree_state (log_did, size, root) VALUES (?, ?, ?)
		 ON CONFLICT(log_did) DO UPDATE SET size = excluded.size, root = excluded.root`,
		logDID, size, root)
	return err
}

// AddRevocation marks a delegation as revoked. Idempotent.
func (s *LogStore) AddRevocation(ctx context.Context, delegationCID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO revocations (delegation_cid, revoked_at) VALUES (?, ?)
		 ON CONFLICT(delegation_cid) DO NOTHING`,
		delegationCID, now)
	return err
}

// GetRevocations returns all revoked delegation CIDs.
func (s *LogStore) GetRevocations(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT delegation_cid FROM revocations ORDER BY revoked_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cids []string
	for rows.Next() {
		var cid string
		if err := rows.Scan(&cid); err != nil {
			return nil, err
		}
		cids = append(cids, cid)
	}

	return cids, rows.Err()
}

// IsRevoked checks if a delegation has been revoked.
func (s *LogStore) IsRevoked(ctx context.Context, delegationCID string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM revocations WHERE delegation_cid = ?`,
		delegationCID).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// SetLatestHeadCAR upserts the latest full-state CAR for a log.
func (s *LogStore) SetLatestHeadCAR(ctx context.Context, logDID string, treeSize uint64, headCID string, carData []byte) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO latest_head_car (log_did, tree_size, head_cid, car_data, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(log_did) DO UPDATE SET
		   tree_size  = excluded.tree_size,
		   head_cid   = excluded.head_cid,
		   car_data   = excluded.car_data,
		   updated_at = excluded.updated_at`,
		logDID, treeSize, headCID, carData, now)
	return err
}

// GetLatestHeadCAR retrieves the latest full-state CAR for a log.
// Returns ("", 0, nil, nil) if no record exists.
func (s *LogStore) GetLatestHeadCAR(ctx context.Context, logDID string) (headCID string, treeSize uint64, carData []byte, err error) {
	var ts int64
	err = s.db.QueryRowContext(ctx,
		`SELECT head_cid, tree_size, car_data FROM latest_head_car WHERE log_did = ?`,
		logDID).Scan(&headCID, &ts, &carData)
	if err == sql.ErrNoRows {
		return "", 0, nil, nil
	}
	if err != nil {
		return "", 0, nil, err
	}
	treeSize = uint64(ts)
	return headCID, treeSize, carData, nil
}

// EnqueueAndUpdateHead atomically updates latest_head_car and inserts an upload_queue row.
// Returns the new queue entry ID.
func (s *LogStore) EnqueueAndUpdateHead(ctx context.Context, logDID string, treeSize uint64, headCID string, carData []byte, blobs []storage.PendingBlob) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339)

	// Upsert latest_head_car
	_, err = tx.ExecContext(ctx,
		`INSERT INTO latest_head_car (log_did, tree_size, head_cid, car_data, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(log_did) DO UPDATE SET
		   tree_size  = excluded.tree_size,
		   head_cid   = excluded.head_cid,
		   car_data   = excluded.car_data,
		   updated_at = excluded.updated_at`,
		logDID, treeSize, headCID, carData, now)
	if err != nil {
		return 0, fmt.Errorf("upsert latest_head_car: %w", err)
	}

	// Insert upload_queue row
	res, err := tx.ExecContext(ctx,
		`INSERT INTO upload_queue (log_did, tree_size, head_cid, car_data, status, created_at)
		 VALUES (?, ?, ?, ?, 'pending', ?)`,
		logDID, treeSize, headCID, carData, now)
	if err != nil {
		return 0, fmt.Errorf("insert upload_queue: %w", err)
	}
	queueID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("last insert id: %w", err)
	}

	// Insert blob rows
	for _, blob := range blobs {
		_, err = tx.ExecContext(ctx,
			`INSERT INTO upload_queue_blobs (queue_id, path, blob_cid, blob_data, status)
			 VALUES (?, ?, ?, ?, 'pending')`,
			queueID, blob.Path, blob.BlobCID, blob.BlobData)
		if err != nil {
			return 0, fmt.Errorf("insert blob %s: %w", blob.Path, err)
		}
	}

	return queueID, tx.Commit()
}

// DequeuePendingCARs atomically claims up to `limit` pending upload_queue entries
// (transitioning them from 'pending' to 'uploading') and returns them.
// Items in 'uploading' state are invisible to other workers.
func (s *LogStore) DequeuePendingCARs(ctx context.Context, limit int) ([]storage.PendingCAR, error) {
	// SELECT the candidates first, then UPDATE only those IDs.
	// SQLite doesn't support UPDATE...RETURNING with a subquery LIMIT in all versions,
	// so we do it in two statements inside a transaction.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx,
		`SELECT id, log_did, tree_size, head_cid, car_data FROM upload_queue
		 WHERE status = 'pending'
		 ORDER BY created_at ASC
		 LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}

	var cars []storage.PendingCAR
	var ids []int64
	for rows.Next() {
		var c storage.PendingCAR
		var ts int64
		if err := rows.Scan(&c.ID, &c.LogDID, &ts, &c.HeadCID, &c.CARData); err != nil {
			rows.Close()
			return nil, err
		}
		c.TreeSize = uint64(ts)
		cars = append(cars, c)
		ids = append(ids, c.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Mark all selected rows as 'uploading' so no other worker picks them up.
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx,
			`UPDATE upload_queue SET status = 'uploading' WHERE id = ?`, id); err != nil {
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return cars, nil
}

// GetPendingBlobsForCAR returns pending blobs for a given queue entry.
func (s *LogStore) GetPendingBlobsForCAR(ctx context.Context, queueID int64) ([]storage.PendingBlob, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, queue_id, path, blob_cid, blob_data FROM upload_queue_blobs
		 WHERE queue_id = ? AND status = 'pending'`, queueID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var blobs []storage.PendingBlob
	for rows.Next() {
		var b storage.PendingBlob
		if err := rows.Scan(&b.ID, &b.QueueID, &b.Path, &b.BlobCID, &b.BlobData); err != nil {
			return nil, err
		}
		blobs = append(blobs, b)
	}
	return blobs, rows.Err()
}

// MarkCARUploaded marks a queue entry as uploaded and nulls car_data.
func (s *LogStore) MarkCARUploaded(ctx context.Context, id int64) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE upload_queue SET status='uploaded', uploaded_at=?, car_data=NULL, error=NULL WHERE id=?`,
		now, id)
	return err
}

// MarkCARFailed increments retry_count and records the error.
func (s *LogStore) MarkCARFailed(ctx context.Context, id int64, errMsg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE upload_queue SET status='pending', error=?, retry_count=retry_count+1 WHERE id=?`,
		errMsg, id)
	return err
}

// MarkBlobUploaded marks a blob as uploaded and nulls blob_data.
func (s *LogStore) MarkBlobUploaded(ctx context.Context, id int64) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`UPDATE upload_queue_blobs SET status='uploaded', uploaded_at=?, blob_data=NULL, error=NULL WHERE id=?`,
		now, id)
	return err
}

// MarkBlobFailed records a blob upload error.
func (s *LogStore) MarkBlobFailed(ctx context.Context, id int64, errMsg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE upload_queue_blobs SET error=? WHERE id=?`,
		errMsg, id)
	return err
}
