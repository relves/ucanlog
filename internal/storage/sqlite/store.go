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

func (s *LogStore) GetCIDIndex(ctx context.Context, logDID string) (map[string]string, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT path, cid FROM cid_index WHERE log_did = ?`,
		logDID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	index := make(map[string]string)
	for rows.Next() {
		var path, cid string
		if err := rows.Scan(&path, &cid); err != nil {
			return nil, err
		}
		index[path] = cid
	}

	return index, rows.Err()
}

func (s *LogStore) SetCID(ctx context.Context, logDID, path, cid string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO cid_index (log_did, path, cid) VALUES (?, ?, ?)
		 ON CONFLICT(log_did, path) DO UPDATE SET cid = excluded.cid`,
		logDID, path, cid)
	return err
}

func (s *LogStore) SetCIDs(ctx context.Context, logDID string, mappings map[string]string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO cid_index (log_did, path, cid) VALUES (?, ?, ?)
		 ON CONFLICT(log_did, path) DO UPDATE SET cid = excluded.cid`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for path, cid := range mappings {
		if _, err := stmt.ExecContext(ctx, logDID, path, cid); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// DeleteCIDsWithPrefix removes all CID mappings with the given path prefix.
func (s *LogStore) DeleteCIDsWithPrefix(ctx context.Context, logDID, prefix string) error {
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM cid_index WHERE log_did = ? AND path LIKE ?`,
		logDID, prefix+"%",
	)
	return err
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

// GetIndexPersistence retrieves index persistence metadata.
// Returns nil if no metadata exists yet.
func (s *LogStore) GetIndexPersistence(ctx context.Context, logDID string) (*storage.IndexPersistenceMeta, error) {
	var meta storage.IndexPersistenceMeta
	var uploadTime, uploadedCID sql.NullString
	var uploadedSize sql.NullInt64

	err := s.db.QueryRowContext(ctx,
		`SELECT last_upload_time, last_uploaded_size, last_uploaded_cid
		 FROM index_persistence WHERE log_did = ?`,
		logDID).Scan(&uploadTime, &uploadedSize, &uploadedCID)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if uploadTime.Valid {
		meta.LastUploadTime, _ = time.Parse(time.RFC3339, uploadTime.String)
	}
	if uploadedSize.Valid {
		meta.LastUploadedSize = uint64(uploadedSize.Int64)
	}
	if uploadedCID.Valid {
		meta.LastUploadedCID = uploadedCID.String
	}

	return &meta, nil
}

// SetIndexPersistence sets index persistence metadata (upsert).
func (s *LogStore) SetIndexPersistence(ctx context.Context, logDID string, uploadTime time.Time, uploadedSize uint64, uploadedCID string) error {
	timeStr := uploadTime.UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO index_persistence (log_did, last_upload_time, last_uploaded_size, last_uploaded_cid)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(log_did) DO UPDATE SET
		   last_upload_time = excluded.last_upload_time,
		   last_uploaded_size = excluded.last_uploaded_size,
		   last_uploaded_cid = excluded.last_uploaded_cid`,
		logDID, timeStr, uploadedSize, uploadedCID)
	return err
}

// GetGCProgress retrieves garbage collection progress.
// Returns 0 if no progress exists yet.
func (s *LogStore) GetGCProgress(ctx context.Context, logDID string) (uint64, error) {
	var fromSize int64
	err := s.db.QueryRowContext(ctx,
		`SELECT from_size FROM gc_progress WHERE log_did = ?`,
		logDID,
	).Scan(&fromSize)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return uint64(fromSize), nil
}

// SetGCProgress sets garbage collection progress (upsert).
func (s *LogStore) SetGCProgress(ctx context.Context, logDID string, fromSize uint64) error {
	now := time.Now().UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO gc_progress (log_did, from_size, updated_at)
		 VALUES (?, ?, ?)
		 ON CONFLICT(log_did) DO UPDATE SET from_size = excluded.from_size, updated_at = excluded.updated_at`,
		logDID, fromSize, now,
	)
	return err
}
