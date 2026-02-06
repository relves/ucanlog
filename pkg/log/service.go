package log

import (
	"context"
	"fmt"
	"time"

	"crypto/ed25519"

	"github.com/relves/ucanlog/internal/storage/sqlite"
	"github.com/relves/ucanlog/pkg/tlog"
	"github.com/relves/ucanlog/pkg/types"
	ucanPkg "github.com/relves/ucanlog/pkg/ucan"
	"github.com/storacha/go-ucanto/core/delegation"
)

type LogService struct {
	tlogManager  *tlog.Manager
	ucanIssuer   *ucanPkg.Issuer
	logMetaStore *tlog.LogMetaStore
	serviceDID   string // Service DID for delegation validation
	storeManager *sqlite.StoreManager
}

// LogServiceConfig holds configuration for creating a LogService.
type LogServiceConfig struct {
	TlogManager  *tlog.Manager
	UcanIssuer   *ucanPkg.Issuer
	LogMetaStore *tlog.LogMetaStore
	ServiceDID   string
	StoreManager *sqlite.StoreManager
}

func NewLogService(tlogMgr *tlog.Manager, issuer *ucanPkg.Issuer) *LogService {
	return &LogService{
		tlogManager: tlogMgr,
		ucanIssuer:  issuer,
	}
}

// NewLogServiceWithConfig creates a LogService with full configuration for delegated storage.
func NewLogServiceWithConfig(cfg LogServiceConfig) *LogService {
	return &LogService{
		tlogManager:  cfg.TlogManager,
		ucanIssuer:   cfg.UcanIssuer,
		logMetaStore: cfg.LogMetaStore,
		serviceDID:   cfg.ServiceDID,
		storeManager: cfg.StoreManager,
	}
}

// CreateLogParams contains parameters for creating a log with customer-delegated storage.
type CreateLogParams struct {
	SpaceDID   string
	Delegation delegation.Delegation
}

// Deprecated: CreateLog is deprecated. Use CreateLogWithDelegation instead.
// This method is kept for backward compatibility with tests.
func (s *LogService) CreateLog(ctx context.Context, logIdKey, accountIdKey ed25519.PublicKey) (*LogResult, error) {
	logID := fmt.Sprintf("%x", logIdKey) // Derive log ID from logId key

	if err := s.tlogManager.CreateLog(ctx, logID); err != nil {
		return nil, fmt.Errorf("failed to create log: %w", err)
	}

	revocationLogID := logID + "-revocations"
	if err := s.tlogManager.CreateLog(ctx, revocationLogID); err != nil {
		return nil, fmt.Errorf("failed to create revocation log: %w", err)
	}

	_, err := s.ucanIssuer.IssueRootUCAN(accountIdKey, types.GroupID(logID), 365*24*time.Hour)
	if err != nil {
		return nil, fmt.Errorf("failed to issue root delegation: %w", err)
	}

	return &LogResult{
		LogID: logID,
	}, nil
}

// CreateLogWithDelegation creates a log with customer-provided Storacha space delegation.
func (s *LogService) CreateLogWithDelegation(ctx context.Context, params CreateLogParams) (*LogResult, error) {
	spaceDID := params.SpaceDID

	// Create log using space DID as identity
	if err := s.tlogManager.CreateLogWithDelegation(ctx, spaceDID, spaceDID, params.Delegation); err != nil {
		return nil, fmt.Errorf("failed to create log: %w", err)
	}

	// Create revocation log for this space
	revocationLogID := spaceDID + "-revocations"
	if err := s.tlogManager.CreateLogWithDelegation(ctx, revocationLogID, spaceDID, params.Delegation); err != nil {
		return nil, fmt.Errorf("failed to create revocation log: %w", err)
	}

	// Store metadata (without delegation - it's passed fresh on each request)
	if s.logMetaStore != nil {
		_, err := s.logMetaStore.Create(spaceDID, spaceDID, spaceDID)
		if err != nil {
			return nil, fmt.Errorf("failed to store log metadata: %w", err)
		}
	}

	return &LogResult{
		LogID: spaceDID,
	}, nil
}

// Append adds an entry to a log and returns the assigned index.
// The delegation is used fresh on each request - not cached.
func (s *LogService) Append(ctx context.Context, logID string, data []byte, dlg delegation.Delegation) (uint64, error) {
	return s.tlogManager.AddEntryWithDelegation(ctx, logID, data, dlg)
}

// Read retrieves entries from a log with pagination
func (s *LogService) Read(ctx context.Context, logID string, offset, limit int64) (*ReadResult, error) {
	// Get total entry count first
	reader, err := s.tlogManager.GetReader(ctx, logID)
	if err != nil {
		return nil, fmt.Errorf("failed to get reader: %w", err)
	}

	total, err := reader.NextIndex(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get log size: %w", err)
	}

	// Apply defaults
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100 // Default limit
	}

	// Calculate actual range
	startIdx := uint64(offset)
	endIdx := uint64(offset + limit)
	if endIdx > total {
		endIdx = total
	}

	entries, err := s.tlogManager.ReadRange(ctx, logID, startIdx, endIdx)
	if err != nil {
		return nil, err
	}

	return &ReadResult{
		Entries: entries,
		Total:   int64(total),
	}, nil
}

// Revoke adds a delegation CID to the revocation log and SQLite.
// Note: Only NEW revocations are written to SQLite. Existing Tessera
// revocations are not automatically migrated.
func (s *LogService) Revoke(ctx context.Context, logID, delegationCID string, dlg delegation.Delegation) error {
	revocationLogID := logID + "-revocations"

	entry := types.RevocationEntry{
		Type:      types.RevokeUCAN,
		Target:    []byte(delegationCID),
		Timestamp: time.Now(),
	}

	data, err := entry.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize revocation entry: %w", err)
	}

	_, err = s.tlogManager.AddEntryWithDelegation(ctx, revocationLogID, data, dlg)
	if err != nil {
		return fmt.Errorf("failed to add revocation entry to tessera: %w", err)
	}

	// Write to SQLite for fast lookups
	if s.storeManager != nil {
		store, err := s.storeManager.GetStateStore(logID)
		if err != nil {
			return fmt.Errorf("failed to get store for revocation: %w", err)
		}

		if err := store.AddRevocation(ctx, delegationCID); err != nil {
			return fmt.Errorf("failed to add revocation to sqlite: %w", err)
		}
	}

	return nil
}

// GetRevocations reads all revoked delegation CIDs from SQLite ONLY.
// Does NOT read from Tessera log - historical revocations from before
// this change will not be returned unless loaded via future admin endpoint.
func (s *LogService) GetRevocations(ctx context.Context, logID string) ([]types.RevocationEntry, error) {
	if s.storeManager == nil {
		return nil, nil // No store manager, return empty
	}

	store, err := s.storeManager.GetStateStore(logID)
	if err != nil {
		return nil, fmt.Errorf("failed to get store: %w", err)
	}

	cids, err := store.GetRevocations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get revocations from sqlite: %w", err)
	}

	entries := make([]types.RevocationEntry, 0, len(cids))
	for i, cid := range cids {
		entries = append(entries, types.RevocationEntry{
			Index:  uint64(i),
			Type:   types.RevokeUCAN,
			Target: []byte(cid),
		})
	}

	return entries, nil
}

// IsRevoked checks if a specific delegation CID is revoked.
// Queries SQLite only - not Tessera.
func (s *LogService) IsRevoked(ctx context.Context, logID, delegationCID string) (bool, error) {
	if s.storeManager == nil {
		return false, nil // No store manager, assume not revoked
	}

	store, err := s.storeManager.GetStateStore(logID)
	if err != nil {
		return false, fmt.Errorf("failed to get store: %w", err)
	}

	return store.IsRevoked(ctx, delegationCID)
}

// GetLogMeta retrieves metadata for a log.
func (s *LogService) GetLogMeta(ctx context.Context, logID string) (*tlog.LogMeta, error) {
	if s.logMetaStore == nil {
		return nil, fmt.Errorf("log metadata store not configured")
	}
	return s.logMetaStore.Get(logID)
}

// ReadCheckpoint reads the latest checkpoint from a log (for tlog-tiles API)
func (s *LogService) ReadCheckpoint(ctx context.Context, logID string) ([]byte, error) {
	return s.tlogManager.ReadCheckpoint(ctx, logID)
}

// ReadTile reads a Merkle tree tile from a log (for tlog-tiles API)
func (s *LogService) ReadTile(ctx context.Context, logID string, level, index uint64, partialWidth uint8) ([]byte, error) {
	return s.tlogManager.ReadTile(ctx, logID, level, index, partialWidth)
}

// ReadEntryBundle reads an entry bundle from a log (for tlog-tiles API)
func (s *LogService) ReadEntryBundle(ctx context.Context, logID string, index uint64, partialWidth uint8) ([]byte, error) {
	return s.tlogManager.ReadEntryBundle(ctx, logID, index, partialWidth)
}

// GetBlobFetcher returns a client that can fetch blobs from a space.
// This is used for fetching delegations during revocation.
func (s *LogService) GetBlobFetcher(ctx context.Context, spaceDID string, dlg delegation.Delegation) (tlog.BlobFetcher, error) {
	return s.tlogManager.GetBlobFetcher(ctx, spaceDID, spaceDID, dlg)
}

// GCResult contains the results of a garbage collection run.
type GCResult struct {
	BundlesProcessed int    // Number of bundles processed
	BlobsRemoved     int    // Number of blobs removed
	BytesFreed       uint64 // Bytes freed (estimated)
	NewGCPosition    uint64 // New GC checkpoint position
}

// RunGC runs garbage collection for a log using the provided delegation.
// The delegation must include space/blob/remove capability and be directly
// issued by the space owner (no proof chain allowed).
func (s *LogService) RunGC(ctx context.Context, logID string, dlg delegation.Delegation) (*GCResult, error) {
	// Call tlog manager's RunGC method
	result, err := s.tlogManager.RunGC(ctx, logID, dlg)
	if err != nil {
		return nil, err
	}

	// Convert from storacha.GCResult to log.GCResult
	return &GCResult{
		BundlesProcessed: result.BundlesProcessed,
		BlobsRemoved:     result.BlobsRemoved,
		BytesFreed:       result.BytesFreed,
		NewGCPosition:    result.NewGCPosition,
	}, nil
}
