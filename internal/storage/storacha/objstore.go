package storacha

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/hashicorp/golang-lru/v2"
	"github.com/storacha/go-ucanto/core/delegation"
)

// delegationContextKey is used to store delegation in context for write operations.
type delegationContextKey struct{}

// WithDelegation adds a delegation to the context for write operations.
func WithDelegation(ctx context.Context, dlg delegation.Delegation) context.Context {
	return context.WithValue(ctx, delegationContextKey{}, dlg)
}

// GetDelegation retrieves the delegation from context, or nil if not present.
func GetDelegation(ctx context.Context) delegation.Delegation {
	val := ctx.Value(delegationContextKey{})
	if val == nil {
		return nil
	}
	if dlg, ok := val.(delegation.Delegation); ok {
		return dlg
	}
	return nil
}

// objStore provides object storage operations backed by Storacha.
// It implements the same interface pattern as AWS/GCP drivers.
type objStore struct {
	clientRef  *clientRef
	index      *CIDIndex
	spaceDID   string
	gatewayURL string
	logger     *slog.Logger
	mu         sync.Mutex
	onDirty    func()

	// blobCache caches fetched blobs by CID to avoid slow gateway re-fetches.
	// Uses LRU eviction to bound memory usage. Thread-safe. No TTL.
	blobCache *lru.Cache[string, []byte]
}

// newObjStore creates a new object store.
// Default blob cache: 10,000 entries (pure LRU, no TTL).
func newObjStore(client *clientRef, index *CIDIndex, spaceDID, gatewayURL string, logger *slog.Logger) *objStore {
	// Pure LRU cache with size limit (no TTL)
	// Size: 10,000 entries (~1-2 MB max)
	// Content-addressed blobs are immutable, so no TTL needed
	cache, err := lru.New[string, []byte](10000)
	if err != nil {
		panic(fmt.Sprintf("failed to create LRU cache: %v", err))
	}
	if logger == nil {
		logger = slog.Default()
	}

	return &objStore{
		clientRef:  client,
		index:      index,
		spaceDID:   spaceDID,
		gatewayURL: gatewayURL,
		logger:     logger,
		blobCache:  cache,
	}
}

// setObject uploads data to Storacha and stores the path→CID mapping.
// UploadBlob fails if accept receipt polling fails, ensuring we only store CIDs for confirmed blobs.
// The delegation is retrieved from context (set via WithDelegation).
func (s *objStore) setObject(ctx context.Context, path string, data []byte) error {
	// Get delegation from context
	dlg := GetDelegation(ctx)
	if dlg == nil {
		return fmt.Errorf("delegation required in context for write operations")
	}

	// Upload to Storacha (fails if blob not accepted)
	if s.clientRef == nil {
		return fmt.Errorf("no Storacha client configured: provide Config.Client")
	}
	client := s.clientRef.Get()
	if client == nil {
		return fmt.Errorf("no Storacha client configured: provide Config.Client")
	}
	cid, err := client.UploadBlob(ctx, s.spaceDID, data, dlg)
	if err != nil {
		return fmt.Errorf("failed to upload to Storacha: %w", err)
	}

	s.mu.Lock()
	// Cache the blob data for local retrieval (LRU handles eviction)
	s.blobCache.Add(cid, data)

	// Store mapping (syncs to StateStore via CIDIndex)
	indexErr := s.index.Set(path, cid)
	onDirty := s.onDirty
	s.mu.Unlock()

	if indexErr != nil {
		return fmt.Errorf("failed to sync CID to state store: %w", indexErr)
	}
	s.logger.Debug("setObject", "path", path, "cid", cid, "indexSize", s.index.Size())

	if onDirty != nil {
		onDirty()
	}

	return nil
}

// getObject retrieves data from Storacha by looking up the path's CID.
// Uses an in-memory cache to avoid slow gateway fetches for previously seen blobs.
func (s *objStore) getObject(ctx context.Context, path string) ([]byte, error) {
	cid, ok := s.index.Get(path)
	if !ok {
		return nil, fmt.Errorf("path not found in index: %s", path)
	}

	// Check cache first (content-addressed, so cached data is always valid)
	if data, ok := s.blobCache.Get(cid); ok {
		return data, nil
	}

	// Cache miss - fetch from gateway
	s.logger.Debug("blob cache miss", "cid", cid, "path", path)
	if s.clientRef == nil {
		return nil, fmt.Errorf("no Storacha client configured: provide Config.Client")
	}
	client := s.clientRef.Get()
	if client == nil {
		return nil, fmt.Errorf("no Storacha client configured: provide Config.Client")
	}
	data, err := client.FetchBlob(ctx, cid)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from Storacha: %w", err)
	}

	// Cache for future reads (LRU handles eviction)
	s.blobCache.Add(cid, data)

	return data, nil
}

// setObjectIfNoneMatch uploads only if the path doesn't already exist.
// Returns (true, nil) if written, (false, nil) if already exists.
// UploadBlob fails if accept receipt polling fails, ensuring we only store CIDs for confirmed blobs.
// The delegation is retrieved from context (set via WithDelegation).
func (s *objStore) setObjectIfNoneMatch(ctx context.Context, path string, data []byte) (bool, error) {
	s.mu.Lock()
	// Check if already exists
	if _, ok := s.index.Get(path); ok {
		s.mu.Unlock()
		return false, nil
	}
	s.mu.Unlock()

	// Get delegation from context
	dlg := GetDelegation(ctx)
	if dlg == nil {
		return false, fmt.Errorf("delegation required in context for write operations")
	}

	// Upload to Storacha (fails if blob not accepted)
	if s.clientRef == nil {
		return false, fmt.Errorf("no Storacha client configured: provide Config.Client")
	}
	client := s.clientRef.Get()
	if client == nil {
		return false, fmt.Errorf("no Storacha client configured: provide Config.Client")
	}
	cid, err := client.UploadBlob(ctx, s.spaceDID, data, dlg)
	if err != nil {
		return false, fmt.Errorf("failed to upload to Storacha: %w", err)
	}

	s.mu.Lock()
	if _, ok := s.index.Get(path); ok {
		s.mu.Unlock()
		return false, nil
	}
	// Cache the blob data for local retrieval (LRU handles eviction)
	s.blobCache.Add(cid, data)

	// Store mapping (syncs to StateStore via CIDIndex)
	indexErr := s.index.Set(path, cid)
	onDirty := s.onDirty
	s.mu.Unlock()

	if indexErr != nil {
		return false, fmt.Errorf("failed to sync CID to state store: %w", indexErr)
	}
	s.logger.Debug("setObjectIfNoneMatch", "path", path, "cid", cid, "indexSize", s.index.Size())

	if onDirty != nil {
		onDirty()
	}

	return true, nil
}

// deleteObjectsWithPrefix removes all entries with the given prefix from the index.
// Note: This doesn't delete from Storacha (content-addressed storage is immutable).
// Garbage collection is handled by Storacha's network.
// TODO: This method doesn't currently sync deletes to StateStore - implement if needed.
func (s *objStore) deleteObjectsWithPrefix(ctx context.Context, prefix string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.index.DeletePrefix(prefix)

	if s.onDirty != nil {
		s.onDirty()
	}

	return nil
}

// getCID returns the CID for a path, or empty string if not found.
func (s *objStore) getCID(path string) string {
	cid, _ := s.index.Get(path)
	return cid
}

// GetCID returns the CID for a path, or empty string if not found.
// Implements gc.PathStore interface.
func (s *objStore) GetCID(path string) string {
	cid, _ := s.index.Get(path)
	return cid
}

// DeletePrefix removes all path mappings with the given prefix.
// Implements gc.PathStore interface.
func (s *objStore) DeletePrefix(prefix string) int {
	count := s.index.DeletePrefix(prefix)
	if count > 0 && s.onDirty != nil {
		s.onDirty()
	}
	return count
}

// SetOnDirty sets the callback to call when the index is modified.
func (s *objStore) SetOnDirty(fn func()) {
	s.onDirty = fn
}
