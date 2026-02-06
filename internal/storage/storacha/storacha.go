// Package storacha provides a Tessera storage driver that uses Storacha
// (decentralized storage network) as the backend. It follows the same
// patterns as the POSIX, AWS, and GCP drivers.
package storacha

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/relves/ucanlog/internal/storage"
	"github.com/relves/ucanlog/internal/storage/storacha/gc"
	"github.com/relves/ucanlog/internal/storage/storacha/indexpersist"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/transparency-dev/tessera"
)

// Config holds configuration for Storacha storage.
type Config struct {
	// SpaceDID is the Storacha space DID (did:key:...) to store data in.
	// Required.
	SpaceDID string

	// StateStore provides state storage (SQLite, etc.)
	// Required. Used for CID index, tree state, and coordination.
	StateStore storage.StateStore

	// LogDID is the log identifier for state storage.
	// Required.
	LogDID string

	// GatewayURL is the IPFS gateway URL for retrieving blobs.
	// Default: https://w3s.link
	GatewayURL string

	// ServiceURL is the Storacha upload service URL.
	// Default: https://up.storacha.network
	ServiceURL string

	// Client is the Storacha client for uploads.
	// If nil, a default client will be created (requires credentials).
	Client StorachaClient

	// HTTPClient for outgoing requests.
	// Default: http.DefaultClient
	HTTPClient *http.Client

	// IndexPersistence configures automatic index CAR persistence.
	// If nil, index persistence is disabled.
	IndexPersistence *indexpersist.Config

	// GC configures automatic garbage collection of obsolete partial bundles.
	// If nil, GC is disabled.
	GC *gc.Config

	// Logger for structured logging.
	// Default: slog.Default()
	Logger *slog.Logger
}

// Storage implements tessera.Driver for Storacha-backed storage.
type Storage struct {
	mu              sync.Mutex
	cfg             Config
	clientRef       *clientRef
	index           *CIDIndex
	objStore        *objStore
	indexPersistMgr *indexpersist.Manager
	gcMgr           *gc.Manager
	logger          *slog.Logger
}

// New creates a new Storacha storage driver.
// The driver stores tiles, bundles, and checkpoints in Storacha's
// decentralized network, while maintaining state in the provided StateStore.
func New(ctx context.Context, cfg Config) (tessera.Driver, error) {
	if cfg.SpaceDID == "" {
		return nil, fmt.Errorf("SpaceDID is required")
	}
	if cfg.StateStore == nil {
		return nil, fmt.Errorf("StateStore is required")
	}
	if cfg.LogDID == "" {
		return nil, fmt.Errorf("LogDID is required")
	}

	// Set defaults
	if cfg.GatewayURL == "" {
		cfg.GatewayURL = "https://ipfs.w3s.link"
	}
	if cfg.ServiceURL == "" {
		cfg.ServiceURL = "https://up.storacha.network"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	// Load CID index from StateStore
	cidIndex, err := cfg.StateStore.GetCIDIndex(ctx, cfg.LogDID)
	if err != nil {
		return nil, fmt.Errorf("failed to load CID index: %w", err)
	}

	// Create index wrapper with StateStore sync
	index := NewCIDIndexFromMap(cidIndex)
	index.SetStateStore(cfg.StateStore, cfg.LogDID)
	index.SetLogger(cfg.Logger)

	// Use provided client or create placeholder
	client := cfg.Client
	if client == nil {
		client = &placeholderClient{}
	}
	ref := newClientRef(client)

	// Create objStore (no longer needs stateDir)
	objStore := newObjStore(ref, index, cfg.SpaceDID, cfg.GatewayURL, cfg.Logger)

	// Set up index persistence if configured
	var indexPersistMgr *indexpersist.Manager
	if cfg.IndexPersistence != nil {
		if cfg.IndexPersistence.Logger == nil {
			cfg.IndexPersistence.Logger = cfg.Logger
		}
		uploader := NewStorachaUploader(ref, cfg.SpaceDID)
		indexProvider := &cidIndexProvider{index: index}

		persistCfg := *cfg.IndexPersistence
		mgr := indexpersist.NewManager(persistCfg, uploader, indexProvider)
		mgr.SetStateStore(cfg.StateStore, cfg.LogDID)

		// Wire up dirty callback
		objStore.SetOnDirty(mgr.MarkDirty)

		// Store manager reference (background start removed - triggered by writes now)
		indexPersistMgr = mgr
	}

	// Set up GC if configured
	var gcMgr *gc.Manager
	if cfg.GC != nil {
		if cfg.GC.Logger == nil {
			cfg.GC.Logger = cfg.Logger
		}
		treeSizeProvider := &storageTreeSizeProvider{objStore: objStore, stateStore: cfg.StateStore, logDID: cfg.LogDID}
		gcMgr = gc.NewManager(*cfg.GC, func() gc.BlobRemover { return ref.Get() }, objStore, treeSizeProvider, cfg.SpaceDID)
		gcMgr.SetStateStore(cfg.StateStore, cfg.LogDID)
	}

	return &Storage{
		cfg:             cfg,
		clientRef:       ref,
		index:           index,
		objStore:        objStore,
		indexPersistMgr: indexPersistMgr,
		gcMgr:           gcMgr,
		logger:          cfg.Logger,
	}, nil
}

// placeholderClient is used when no client is provided.
// It returns errors for all operations, prompting proper configuration.
type placeholderClient struct{}

func (p *placeholderClient) UploadBlob(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	return "", fmt.Errorf("no Storacha client configured: provide Config.Client")
}

func (p *placeholderClient) UploadCAR(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	return "", fmt.Errorf("no Storacha client configured: provide Config.Client")
}

func (p *placeholderClient) FetchBlob(ctx context.Context, cid string) ([]byte, error) {
	return nil, fmt.Errorf("no Storacha client configured: provide Config.Client")
}

func (p *placeholderClient) RemoveBlob(ctx context.Context, spaceDID string, digest []byte, dlg delegation.Delegation) error {
	return fmt.Errorf("no Storacha client configured: provide Config.Client")
}

// SetClient updates the client used by this storage driver.
// This allows upgrading from a read-only gateway client to a delegated client.
func (s *Storage) SetClient(client StorachaClient) {
	s.clientRef.Set(client)
}

// EnableIndexPersistence starts index CAR persistence with the given config.
// This is called after upgrading from a read-only client to enable uploads.
func (s *Storage) EnableIndexPersistence(cfg *indexpersist.Config) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cfg == nil {
		return
	}
	if cfg.Logger == nil {
		cfg.Logger = s.logger
	}

	// Create uploader with current client
	uploader := NewStorachaUploader(s.clientRef, s.cfg.SpaceDID)
	indexProvider := &cidIndexProvider{index: s.index}

	persistCfg := *cfg
	mgr := indexpersist.NewManager(persistCfg, uploader, indexProvider)
	mgr.SetStateStore(s.cfg.StateStore, s.cfg.LogDID)

	// Wire up dirty callback
	s.objStore.SetOnDirty(mgr.MarkDirty)

	// Store manager reference (instead of starting background loop)
	s.indexPersistMgr = mgr
}

// TriggerIndexPersistence triggers async index persistence using the delegation from context.
// This should be called after write operations complete.
// Safe to call even if index persistence is not configured.
//
// The delegation is extracted from the request context and attached to a new background
// context. This prevents "context canceled" errors when the request completes before
// the async upload finishes.
func (s *Storage) TriggerIndexPersistence(ctx context.Context) {
	s.mu.Lock()
	mgr := s.indexPersistMgr
	s.mu.Unlock()

	if mgr != nil {
		// Extract delegation from request context and attach to background context.
		// This creates a context that won't be canceled when the request completes,
		// but still carries the delegation needed for write operations.
		dlg := GetDelegation(ctx)
		bgCtx := WithDelegation(context.Background(), dlg)
		mgr.TriggerPersistAsync(bgCtx)
	}
}

// GCResult contains the results of a garbage collection run.
type GCResult struct {
	BundlesProcessed int    // Number of bundles processed
	BlobsRemoved     int    // Number of blobs removed
	BytesFreed       uint64 // Bytes freed (estimated)
	NewGCPosition    uint64 // New GC checkpoint position
}

// RunGC runs garbage collection synchronously with the provided delegation.
// The delegation must include space/blob/remove capability.
// Returns the number of bundles processed and any errors.
func (s *Storage) RunGC(ctx context.Context, dlg delegation.Delegation) (*GCResult, error) {
	s.mu.Lock()
	mgr := s.gcMgr
	s.mu.Unlock()

	if mgr == nil {
		return nil, fmt.Errorf("garbage collection not configured")
	}

	// Get current tree size from state store
	treeSize, _, err := s.cfg.StateStore.GetTreeState(ctx, s.cfg.LogDID)
	if err != nil {
		return nil, fmt.Errorf("failed to get tree state: %w", err)
	}
	if treeSize == 0 {
		return &GCResult{}, nil // Nothing to collect
	}

	// Get GC progress from state store
	fromSize, err := s.cfg.StateStore.GetGCProgress(ctx, s.cfg.LogDID)
	if err != nil {
		return nil, fmt.Errorf("failed to get GC progress: %w", err)
	}

	if fromSize >= treeSize {
		return &GCResult{NewGCPosition: fromSize}, nil // Already up to date
	}

	// Run GC using the manager's RunGCSync method
	result := &GCResult{
		BlobsRemoved: 0,
		BytesFreed:   0,
	}

	newFromSize, blobsRemoved, err := mgr.RunGCSync(ctx, fromSize, treeSize, dlg)
	if err != nil {
		return nil, fmt.Errorf("garbage collection failed: %w", err)
	}

	// Calculate bundles processed (256 entries per bundle)
	bundlesProcessed := int((newFromSize - fromSize) / 256)
	result.BundlesProcessed = bundlesProcessed
	result.BlobsRemoved = blobsRemoved
	result.NewGCPosition = newFromSize

	// Update progress in state store
	if err := s.cfg.StateStore.SetGCProgress(ctx, s.cfg.LogDID, newFromSize); err != nil {
		return nil, fmt.Errorf("failed to save GC progress: %w", err)
	}

	return result, nil
}

// storageTreeSizeProvider provides tree size from StateStore.
type storageTreeSizeProvider struct {
	objStore   *objStore
	stateStore storage.StateStore
	logDID     string
}

func (p *storageTreeSizeProvider) GetTreeSize() uint64 {
	if p.stateStore == nil {
		return 0
	}
	size, _, err := p.stateStore.GetTreeState(context.Background(), p.logDID)
	if err != nil {
		return 0
	}
	return size
}

// cidIndexProvider wraps CIDIndex to implement IndexProvider.
// Now that we reuse the same Storage driver in RecreateAppender,
// it's safe to read from the in-memory index directly.
type cidIndexProvider struct {
	index *CIDIndex
}

func (p *cidIndexProvider) GetIndex() map[string]string {
	p.index.mu.RLock()
	defer p.index.mu.RUnlock()
	result := make(map[string]string, len(p.index.Paths))
	for k, v := range p.index.Paths {
		result[k] = v
	}
	return result
}
