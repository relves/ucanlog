// internal/tlog/manager.go
package tlog

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/relves/ucanlog/internal/storage/sqlite"
	"github.com/relves/ucanlog/internal/storage/storacha"
	"github.com/relves/ucanlog/internal/storage/storacha/indexpersist"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
	"github.com/transparency-dev/tessera"
)

// sanitizeSpaceDID converts a space DID into a filesystem-safe string.
// DIDs contain colons (e.g., "did:key:z6Mk...") which can be problematic
// on some filesystems, so we replace them with underscores.
func sanitizeSpaceDID(spaceDID string) string {
	return strings.ReplaceAll(spaceDID, ":", "_")
}

// logPath returns the storage path for a log's main data.
func logPath(basePath, spaceDID string) string {
	safeDID := sanitizeSpaceDID(spaceDID)
	return fmt.Sprintf("%s/logs/%s", basePath, safeDID)
}

// revocationLogPath returns the storage path for a log's revocation data.
func revocationLogPath(basePath, spaceDID string) string {
	safeDID := sanitizeSpaceDID(spaceDID)
	return fmt.Sprintf("%s/logs/%s-revocations", basePath, safeDID)
}

// Signer signs checkpoints for transparent logs
type Signer interface {
	Name() string
	Sign([]byte) ([]byte, error)
	KeyHash() uint32
}

// LogInstance holds both the appender and reader for a log.
type LogInstance struct {
	Appender *tessera.Appender
	Reader   tessera.LogReader
	Driver   tessera.Driver // Store driver for reuse in RecreateAppender
	SpaceDID string         // Customer's space DID (for delegated storage)
}

// Manager handles Tessera tlog operations.
type Manager struct {
	basePath       string
	logs           map[string]*LogInstance
	mu             sync.RWMutex
	signer         Signer
	privateKey     []byte // Store private key for creating per-log signers
	originPrefix   string // Prefix for log origins (e.g., "ucanlog")
	storachaClient storacha.StorachaClient
	spaceDID       string
	cidStore       CIDStore
	storeManager   *sqlite.StoreManager // SQLite state storage
	logger         *slog.Logger

	// For customer-delegated storage
	serviceSigner principal.Signer     // Service's identity for signing invocations
	clientPool    *storacha.ClientPool // Pool of per-log delegated clients
}

// NewManager creates a new tlog manager.
func NewManager(basePath string, signer Signer) (*Manager, error) {
	if signer == nil {
		// Fall back to dummy signer if none provided (for backward compatibility)
		signer = &dummySigner{}
	}
	return &Manager{
		basePath: basePath,
		logs:     make(map[string]*LogInstance),
		signer:   signer,
		logger:   slog.Default(),
	}, nil
}

// NewStorachaManager creates a new tlog manager backed by Storacha/IPFS storage.
// privateKey is the Ed25519 private key used for signing checkpoints.
// originPrefix is the prefix for log origins (e.g., "ucanlog" results in origins like "ucanlog/logs/<logID>").
func NewStorachaManager(basePath string, signer Signer, privateKey []byte, originPrefix string, storachaClient storacha.StorachaClient, spaceDID string, cidStore CIDStore) (*Manager, error) {
	if signer == nil {
		signer = &dummySigner{}
	}
	if storachaClient == nil {
		return nil, fmt.Errorf("storachaClient is required")
	}
	if spaceDID == "" {
		return nil, fmt.Errorf("spaceDID is required")
	}
	if cidStore == nil {
		return nil, fmt.Errorf("cidStore is required")
	}
	if originPrefix == "" {
		originPrefix = "ucanlog"
	}

	// Create StoreManager for SQLite state storage
	storeManager := sqlite.NewStoreManager(basePath)

	return &Manager{
		basePath:       basePath,
		logs:           make(map[string]*LogInstance),
		signer:         signer,
		privateKey:     privateKey,
		originPrefix:   originPrefix,
		storachaClient: storachaClient,
		spaceDID:       spaceDID,
		cidStore:       cidStore,
		storeManager:   storeManager,
		logger:         slog.Default(),
	}, nil
}

// DelegatedManagerConfig configures a Manager for customer-delegated storage.
type DelegatedManagerConfig struct {
	BasePath      string
	Signer        Signer
	PrivateKey    []byte
	OriginPrefix  string
	ServiceSigner principal.Signer
	CIDStore      CIDStore
	StoreManager  *sqlite.StoreManager // Optional: if nil, will be created from BasePath
	Logger        *slog.Logger
}

// NewDelegatedManager creates a tlog manager that uses customer-delegated Storacha storage.
// Each log uses the customer's own Storacha space via UCAN delegation.
func NewDelegatedManager(cfg DelegatedManagerConfig) (*Manager, error) {
	if cfg.ServiceSigner == nil {
		return nil, fmt.Errorf("ServiceSigner is required for delegated storage")
	}
	if cfg.CIDStore == nil {
		return nil, fmt.Errorf("CIDStore is required")
	}
	if cfg.OriginPrefix == "" {
		cfg.OriginPrefix = "ucanlog"
	}
	if cfg.Signer == nil {
		cfg.Signer = &dummySigner{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	// Create client pool for managing per-log delegated clients
	clientPool, err := storacha.NewClientPool(storacha.ClientPoolConfig{
		ServiceSigner: cfg.ServiceSigner,
		Logger:        cfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create client pool: %w", err)
	}

	// Use provided StoreManager or create one from BasePath
	storeManager := cfg.StoreManager
	if storeManager == nil {
		storeManager = sqlite.NewStoreManager(cfg.BasePath)
	}

	return &Manager{
		basePath:      cfg.BasePath,
		logs:          make(map[string]*LogInstance),
		signer:        cfg.Signer,
		privateKey:    cfg.PrivateKey,
		originPrefix:  cfg.OriginPrefix,
		cidStore:      cfg.CIDStore,
		storeManager:  storeManager,
		logger:        cfg.Logger,
		serviceSigner: cfg.ServiceSigner,
		clientPool:    clientPool,
	}, nil
}

// CreateLog creates a new transparency log.
// Deprecated: Use CreateLogWithDelegation for customer-delegated storage.
func (m *Manager) CreateLog(ctx context.Context, logID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.logs[logID]; exists {
		return fmt.Errorf("log %s already exists", logID)
	}

	var driver tessera.Driver
	var err error

	// Use Storacha driver if configured, otherwise fall back to POSIX
	if m.storachaClient != nil {
		if m.storeManager == nil {
			return fmt.Errorf("store manager not configured")
		}

		// Get StateStore for this log
		stateStore, err := m.storeManager.GetStore(logID)
		if err != nil {
			return fmt.Errorf("failed to get state store: %w", err)
		}

		// Ensure log record exists
		if err := stateStore.CreateLogRecord(ctx, logID); err != nil {
			// Ignore duplicate key errors
		}

		driver, err = storacha.New(ctx, storacha.Config{
			SpaceDID:   m.spaceDID,
			StateStore: stateStore,
			LogDID:     logID,
			Client:     m.storachaClient,
			IndexPersistence: &indexpersist.Config{
				Interval: 30 * time.Second,
				Logger:   m.logger,
				OnUpload: func(rootCID string, meta indexpersist.IndexMeta) {
					// Update CID store when index CAR is uploaded
					if err := m.cidStore.SetLatestCID(logID, rootCID); err != nil {
						m.logger.Warn("failed to update CID store", "logID", logID, "error", err)
					}
				},
			},
			Logger: m.logger,
		})
		if err != nil {
			return fmt.Errorf("failed to create Storacha driver: %w", err)
		}
	} else {
		// Fallback to POSIX driver (for backward compatibility/testing)
		return fmt.Errorf("POSIX driver no longer supported, use NewStorachaManager")
	}

	// Create per-log signer with unique origin
	logOrigin := fmt.Sprintf("%s/logs/%s", m.originPrefix, logID)
	var logSigner Signer
	if m.privateKey != nil {
		var err error
		logSigner, err = NewEd25519Signer(m.privateKey, logOrigin)
		if err != nil {
			return fmt.Errorf("failed to create per-log signer: %w", err)
		}
	} else {
		logSigner = m.signer
	}

	// Create Tessera appender with per-log signer
	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(logSigner).
		WithCheckpointInterval(time.Second).
		WithBatching(1, 100*time.Millisecond).
		WithCheckpointRepublishInterval(24 * time.Hour)

	// Create Tessera appender with per-log signer
	// opts := tessera.NewAppendOptions().
	// 	WithCheckpointSigner(logSigner).
	// 	WithCheckpointInterval(time.Second).
	// 	WithBatching(256, 5*time.Second). // Batch up entries
	// 	WithCheckpointInterval(30*time.Second) // Reduce idle checkpoints

	// Configure witness policy if witness_policy.txt exists
	m.logger.Debug("CreateLog called", "logID", logID, "basePath", m.basePath)
	witnessPolicyPath := fmt.Sprintf("%s/witness_policy.txt", m.basePath)
	m.logger.Debug("looking for witness policy", "path", witnessPolicyPath)

	policyBytes, readErr := os.ReadFile(witnessPolicyPath)
	if readErr != nil {
		m.logger.Debug("failed to read witness policy", "error", readErr)
	} else {
		m.logger.Debug("read witness policy", "bytes", len(policyBytes))
		m.logger.Debug("witness policy content", "policy", string(policyBytes))

		witnessGroup, err := tessera.NewWitnessGroupFromPolicy(policyBytes)
		if err != nil {
			m.logger.Debug("failed to parse witness policy", "error", err)
			return fmt.Errorf("failed to parse witness policy from %s: %w", witnessPolicyPath, err)
		}
		m.logger.Debug("parsed witness policy", "N", witnessGroup.N, "components", len(witnessGroup.Components))

		witnessOpts := &tessera.WitnessOptions{
			Timeout:  tessera.DefaultWitnessTimeout,
			FailOpen: false, // Require witness signatures
		}
		opts.WithWitnesses(witnessGroup, witnessOpts)
		m.logger.Debug("configured witnesses", "logID", logID, "timeout", witnessOpts.Timeout, "failOpen", witnessOpts.FailOpen)
	}

	appender, _, reader, err := tessera.NewAppender(ctx, driver, opts)
	if err != nil {
		return fmt.Errorf("failed to create appender: %w", err)
	}

	m.logs[logID] = &LogInstance{
		Appender: appender,
		Reader:   reader,
		Driver:   driver,
	}
	return nil
}

// CreateLogWithDelegation creates a log using customer-delegated Storacha storage.
// The customer's delegation grants the service write access to their space.
func (m *Manager) CreateLogWithDelegation(ctx context.Context, logID string, spaceDID string, dlg delegation.Delegation) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.logs[logID]; exists {
		return fmt.Errorf("log %s already exists", logID)
	}

	if m.clientPool == nil {
		return fmt.Errorf("delegated storage not configured - use NewDelegatedManager")
	}

	if m.storeManager == nil {
		return fmt.Errorf("store manager not configured - use NewDelegatedManager")
	}

	// Get StateStore for this log
	stateStore, err := m.storeManager.GetStore(logID)
	if err != nil {
		return fmt.Errorf("failed to get state store: %w", err)
	}

	// Ensure log record exists in SQLite
	if err := stateStore.CreateLogRecord(ctx, logID); err != nil {
		// Ignore duplicate key errors - log record may already exist
		// SQLite will return an error if the record already exists
	}

	// Get or create a delegated client for this log's space
	client, err := m.clientPool.GetClient(logID, spaceDID, dlg)
	if err != nil {
		return fmt.Errorf("failed to get delegated client: %w", err)
	}

	// Create Storacha driver using StateStore
	driver, err := storacha.New(ctx, storacha.Config{
		SpaceDID:   spaceDID,
		StateStore: stateStore,
		LogDID:     logID,
		Client:     client,
		IndexPersistence: &indexpersist.Config{
			Interval: 30 * time.Second,
			Logger:   m.logger,
			OnUpload: func(rootCID string, meta indexpersist.IndexMeta) {
				if err := m.cidStore.SetLatestCID(logID, rootCID); err != nil {
					m.logger.Warn("failed to update CID store", "logID", logID, "error", err)
				}
			},
		},
		Logger: m.logger,
	})
	if err != nil {
		return fmt.Errorf("failed to create Storacha driver: %w", err)
	}

	// Create per-log signer
	logOrigin := fmt.Sprintf("%s/logs/%s", m.originPrefix, logID)
	var logSigner Signer
	if m.privateKey != nil {
		logSigner, err = NewEd25519Signer(m.privateKey, logOrigin)
		if err != nil {
			return fmt.Errorf("failed to create per-log signer: %w", err)
		}
	} else {
		logSigner = m.signer
	}

	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(logSigner).
		WithCheckpointInterval(time.Second).
		WithBatching(1, 100*time.Millisecond).
		WithCheckpointRepublishInterval(24 * time.Hour)

	// Configure witness policy if exists
	witnessPolicyPath := fmt.Sprintf("%s/witness_policy.txt", m.basePath)
	if policyBytes, err := os.ReadFile(witnessPolicyPath); err == nil {
		witnessGroup, err := tessera.NewWitnessGroupFromPolicy(policyBytes)
		if err != nil {
			return fmt.Errorf("failed to parse witness policy: %w", err)
		}
		opts.WithWitnesses(witnessGroup, &tessera.WitnessOptions{
			Timeout:  tessera.DefaultWitnessTimeout,
			FailOpen: false,
		})
	}

	appender, _, reader, err := tessera.NewAppender(ctx, driver, opts)
	if err != nil {
		return fmt.Errorf("failed to create appender: %w", err)
	}

	m.logs[logID] = &LogInstance{
		Appender: appender,
		Reader:   reader,
		Driver:   driver,
		SpaceDID: spaceDID,
	}
	return nil
}

// UpdateDelegation updates the delegation for an existing log.
// This allows customers to refresh expired delegations.
// Note: With per-request delegation passing, this method now just verifies the log exists.
// The new delegation will be used on the next write via AddEntryWithDelegation.
func (m *Manager) UpdateDelegation(ctx context.Context, logID string, dlg delegation.Delegation) error {
	m.mu.RLock()
	_, exists := m.logs[logID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("log %s not found", logID)
	}

	if m.clientPool == nil {
		return fmt.Errorf("delegated storage not configured")
	}

	// Delegations are now passed per-request, not cached.
	// This method is kept for API compatibility but doesn't need to update anything.
	return nil
}

// GetLogSpaceDID returns the customer's space DID for a log.
func (m *Manager) GetLogSpaceDID(ctx context.Context, logID string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	instance, exists := m.logs[logID]
	if !exists {
		return "", fmt.Errorf("log %s not found", logID)
	}

	return instance.SpaceDID, nil
}

// LogExists checks if a log exists.
func (m *Manager) LogExists(ctx context.Context, logID string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, exists := m.logs[logID]
	return exists, nil
}

// GetBlobFetcher returns a client that can fetch blobs from the space.
// This is used for fetching delegations during revocation.
func (m *Manager) GetBlobFetcher(ctx context.Context, logID string, spaceDID string, dlg delegation.Delegation) (BlobFetcher, error) {
	if m.clientPool == nil {
		return nil, fmt.Errorf("delegated storage not configured - use NewDelegatedManager")
	}

	client, err := m.clientPool.GetClient(logID, spaceDID, dlg)
	if err != nil {
		return nil, fmt.Errorf("failed to get client: %w", err)
	}

	return client, nil
}

// BlobFetcher is an interface for fetching blobs by CID.
type BlobFetcher interface {
	FetchBlob(ctx context.Context, cid string) ([]byte, error)
}

// GetLogInstance retrieves a log instance by ID.
// If the log is not in memory but exists on disk, it is lazily restored.
func (m *Manager) GetLogInstance(ctx context.Context, logID string) (*LogInstance, error) {
	m.mu.RLock()
	instance, exists := m.logs[logID]
	m.mu.RUnlock()

	if exists {
		return instance, nil
	}

	// Not in memory - try to restore from disk
	return m.restoreLog(ctx, logID)
}

// restoreLog restores a log from disk with a read-only gateway client.
// This is called lazily when a log is accessed but not in memory.
// The log can be upgraded to full write access when a delegation is provided.
func (m *Manager) restoreLog(ctx context.Context, logID string) (*LogInstance, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if instance, exists := m.logs[logID]; exists {
		return instance, nil
	}

	// Verify log directory exists
	logDir := filepath.Join(m.basePath, "logs", logID)
	if _, err := os.Stat(logDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("log %s not found", logID)
	}

	// Get StateStore for this log
	stateStore, err := m.storeManager.GetStore(logID)
	if err != nil {
		return nil, fmt.Errorf("failed to get state store for %s: %w", logID, err)
	}

	// Verify log record exists in SQLite
	if _, err := stateStore.GetLogRecord(ctx, logID); err != nil {
		return nil, fmt.Errorf("log %s not found in database: %w", logID, err)
	}

	// Create read-only gateway client for reads
	gatewayURL := "https://w3s.link" // Default gateway
	if envURL := os.Getenv("IPFS_GATEWAY_URL"); envURL != "" {
		gatewayURL = envURL
	}
	readOnlyClient := storacha.NewGatewayClient(gatewayURL)

	// Create Storacha driver with read-only client
	// spaceDID == logID in our architecture
	// NOTE: Index persistence is disabled for read-only mode - it will be enabled
	// when the client is upgraded to delegated mode via AddEntryWithDelegation
	driver, err := storacha.New(ctx, storacha.Config{
		SpaceDID:   logID,
		StateStore: stateStore,
		LogDID:     logID,
		Client:     readOnlyClient,
		Logger:     m.logger,
		// IndexPersistence is nil - disabled for read-only mode
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create driver for %s: %w", logID, err)
	}

	// Create per-log signer
	logOrigin := fmt.Sprintf("%s/logs/%s", m.originPrefix, logID)
	var logSigner Signer
	if m.privateKey != nil {
		logSigner, err = NewEd25519Signer(m.privateKey, logOrigin)
		if err != nil {
			return nil, fmt.Errorf("failed to create signer for %s: %w", logID, err)
		}
	} else {
		logSigner = m.signer
	}

	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(logSigner).
		WithCheckpointInterval(time.Second).
		WithBatching(1, 100*time.Millisecond).
		WithCheckpointRepublishInterval(24 * time.Hour)

	// Configure witness policy if exists
	witnessPolicyPath := fmt.Sprintf("%s/witness_policy.txt", m.basePath)
	if policyBytes, err := os.ReadFile(witnessPolicyPath); err == nil {
		witnessGroup, err := tessera.NewWitnessGroupFromPolicy(policyBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to parse witness policy: %w", err)
		}
		opts.WithWitnesses(witnessGroup, &tessera.WitnessOptions{
			Timeout:  tessera.DefaultWitnessTimeout,
			FailOpen: false,
		})
	}

	appender, _, reader, err := tessera.NewAppender(ctx, driver, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create appender for %s: %w", logID, err)
	}

	instance := &LogInstance{
		Appender: appender,
		Reader:   reader,
		Driver:   driver,
		SpaceDID: logID,
	}
	m.logs[logID] = instance

	m.logger.Info("lazily restored log", "logID", logID, "mode", "read-only")
	return instance, nil
}

// GetAppender retrieves a log appender by ID.
func (m *Manager) GetAppender(ctx context.Context, logID string) (*tessera.Appender, error) {
	instance, err := m.GetLogInstance(ctx, logID)
	if err != nil {
		return nil, err
	}
	return instance.Appender, nil
}

// AddEntry adds an entry to the specified log and returns its index.
// addEntry is an internal method that adds an entry to a log.
// It expects delegation to already be in the context.
// External callers should use AddEntryWithDelegation instead.
func (m *Manager) addEntry(ctx context.Context, logID string, data []byte) (uint64, error) {
	m.logger.Debug("addEntry: getting appender")
	appender, err := m.GetAppender(ctx, logID)
	if err != nil {
		return 0, err
	}

	m.logger.Debug("addEntry: new entry")
	// Add entry using Tessera
	future := appender.Add(ctx, tessera.NewEntry(data))

	m.logger.Debug("addEntry: awaiting index assignment")
	// Wait for the index to be assigned
	index, err := future()
	if err != nil {
		return 0, fmt.Errorf("failed to add entry: %w", err)
	}

	return index.Index, nil
}

// upgradeLogClient upgrades a log's driver from read-only to delegated client.
// This is called when the first write arrives with a delegation.
// It also enables index CAR persistence now that uploads are possible.
func (m *Manager) upgradeLogClient(logID string, client storacha.StorachaClient) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	instance, exists := m.logs[logID]
	if !exists {
		return fmt.Errorf("log %s not found", logID)
	}

	// Type assert to get the concrete Storage type
	storage, ok := instance.Driver.(*storacha.Storage)
	if !ok {
		return fmt.Errorf("driver is not a Storacha storage")
	}

	storage.SetClient(client)

	// Enable index persistence now that we have a writable client
	storage.EnableIndexPersistence(&indexpersist.Config{
		Interval: 30 * time.Second,
		Logger:   m.logger,
		OnUpload: func(rootCID string, meta indexpersist.IndexMeta) {
			if err := m.cidStore.SetLatestCID(logID, rootCID); err != nil {
				m.logger.Warn("failed to update CID store", "logID", logID, "error", err)
			}
		},
	})

	m.logger.Info("upgraded log client with index persistence", "logID", logID)
	return nil
}

// AddEntryWithDelegation adds an entry to a log using the provided delegation.
// The delegation is passed through the context to ensure each write uses its own delegation.
// If the log was lazily restored with a read-only client, this upgrades it to a delegated client.
func (m *Manager) AddEntryWithDelegation(ctx context.Context, logID string, data []byte, dlg delegation.Delegation) (uint64, error) {
	if m.clientPool == nil {
		return 0, fmt.Errorf("delegated storage not configured")
	}
	if dlg == nil {
		return 0, fmt.Errorf("delegation required for write operations")
	}

	// Get or create delegated client from pool
	// spaceDID == logID in our architecture
	client, err := m.clientPool.GetClient(logID, logID, dlg)
	if err != nil {
		return 0, fmt.Errorf("failed to get delegated client: %w", err)
	}

	// Upgrade the log's driver to use the delegated client
	// This is a no-op if already using this client
	if err := m.upgradeLogClient(logID, client); err != nil {
		// Log might not exist yet - try to restore it first
		if _, restoreErr := m.GetLogInstance(ctx, logID); restoreErr != nil {
			return 0, fmt.Errorf("log %s not found: %w", logID, restoreErr)
		}
		// Retry upgrade after restoration
		if err := m.upgradeLogClient(logID, client); err != nil {
			return 0, fmt.Errorf("failed to upgrade client: %w", err)
		}
	}

	// Add delegation to context for the write operation
	ctx = storacha.WithDelegation(ctx, dlg)

	seq, err := m.addEntry(ctx, logID, data)
	if err != nil {
		return 0, err
	}

	// Trigger async index persistence using this delegation
	// The persistence runs in background; write latency is not affected
	m.mu.Lock()
	instance, exists := m.logs[logID]
	m.mu.Unlock()
	if exists {
		if storage, ok := instance.Driver.(*storacha.Storage); ok {
			storage.TriggerIndexPersistence(ctx)
		}
	}

	return seq, nil
}

// RunGC runs garbage collection for a log using the provided delegation.
// The delegation must include space/blob/remove capability.
// Returns GC results including bundles processed and errors.
func (m *Manager) RunGC(ctx context.Context, logID string, dlg delegation.Delegation) (*storacha.GCResult, error) {
	if dlg == nil {
		return nil, fmt.Errorf("delegation required for GC operations")
	}

	// Get the log instance
	m.mu.RLock()
	instance, exists := m.logs[logID]
	m.mu.RUnlock()

	if !exists {
		// Try to restore the log first
		var err error
		instance, err = m.GetLogInstance(ctx, logID)
		if err != nil {
			return nil, fmt.Errorf("log %s not found: %w", logID, err)
		}
	}

	// Type assert to get the concrete Storage type
	storage, ok := instance.Driver.(*storacha.Storage)
	if !ok {
		return nil, fmt.Errorf("log %s does not use Storacha storage", logID)
	}

	// Run GC using the storage driver's RunGC method
	result, err := storage.RunGC(ctx, dlg)
	if err != nil {
		return nil, fmt.Errorf("garbage collection failed for log %s: %w", logID, err)
	}

	return result, nil
}

// RecreateAppender recreates the appender for a log to avoid state corruption.
// It reuses the existing driver to preserve the index persistence manager's
// connection to the same objStore and its onDirty callback.
func (m *Manager) RecreateAppender(ctx context.Context, logID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	instance, exists := m.logs[logID]
	if !exists {
		return fmt.Errorf("log %s not found", logID)
	}

	// Reuse the existing driver - this preserves the index persistence manager
	driver := instance.Driver
	if driver == nil {
		return fmt.Errorf("log %s has no driver", logID)
	}

	// Create fresh appender using the SAME driver.
	// Storage.Appender() reuses objStore (and its onDirty callback).
	m.logger.Debug("recreate appender called", "logID", logID, "basePath", m.basePath)

	// Create per-log signer with unique origin
	logOrigin := fmt.Sprintf("%s/logs/%s", m.originPrefix, logID)
	var logSigner Signer
	if m.privateKey != nil {
		var err error
		logSigner, err = NewEd25519Signer(m.privateKey, logOrigin)
		if err != nil {
			return fmt.Errorf("failed to create per-log signer: %w", err)
		}
	} else {
		logSigner = m.signer
	}

	// For options see https://pkg.go.dev/github.com/transparency-dev/tessera@main#AppendOptions
	opts := tessera.NewAppendOptions().
		WithCheckpointSigner(logSigner).
		WithCheckpointInterval(time.Second).
		WithBatching(1, 100*time.Millisecond)

	// Configure witness policy if witness_policy.txt exists
	witnessPolicyPath := fmt.Sprintf("%s/witness_policy.txt", m.basePath)
	m.logger.Debug("recreate appender looking for witness policy", "path", witnessPolicyPath)

	policyBytes, readErr := os.ReadFile(witnessPolicyPath)
	if readErr != nil {
		m.logger.Debug("recreate appender failed to read witness policy", "error", readErr)
	} else {
		m.logger.Debug("recreate appender read witness policy", "bytes", len(policyBytes))

		witnessGroup, err := tessera.NewWitnessGroupFromPolicy(policyBytes)
		if err != nil {
			m.logger.Debug("recreate appender failed to parse witness policy", "error", err)
			return fmt.Errorf("failed to parse witness policy from %s: %w", witnessPolicyPath, err)
		}
		m.logger.Debug("recreate appender parsed witness policy", "N", witnessGroup.N, "components", len(witnessGroup.Components))

		witnessOpts := &tessera.WitnessOptions{
			Timeout:  tessera.DefaultWitnessTimeout,
			FailOpen: false, // Require witness signatures
		}
		opts.WithWitnesses(witnessGroup, witnessOpts)
		m.logger.Debug("recreate appender configured witnesses", "logID", logID)
	}

	appender, _, reader, err := tessera.NewAppender(ctx, driver, opts)
	if err != nil {
		return fmt.Errorf("failed to recreate appender: %w", err)
	}

	// Update appender/reader, keep the same driver
	instance.Appender = appender
	instance.Reader = reader

	return nil
}

// addEntriesBatch is an internal method that adds multiple entries sequentially.
// It expects delegation to already be in the context.
// External callers should use AddEntryWithDelegation in a loop instead.
func (m *Manager) addEntriesBatch(ctx context.Context, logID string, entries [][]byte) ([]uint64, error) {
	indices := make([]uint64, len(entries))
	for i, data := range entries {
		index, err := m.addEntry(ctx, logID, data)
		if err != nil {
			return nil, fmt.Errorf("failed to add entry %d: %w", i, err)
		}
		indices[i] = index
	}
	return indices, nil
}

// GetReader retrieves a log reader by ID.
func (m *Manager) GetReader(ctx context.Context, logID string) (tessera.LogReader, error) {
	instance, err := m.GetLogInstance(ctx, logID)
	if err != nil {
		return nil, err
	}
	return instance.Reader, nil
}

// ReadEntryBundle reads an entry bundle from the specified log.
func (m *Manager) ReadEntryBundle(ctx context.Context, logID string, index uint64, p uint8) ([]byte, error) {
	reader, err := m.GetReader(ctx, logID)
	if err != nil {
		return nil, err
	}
	return reader.ReadEntryBundle(ctx, index, p)
}

// ReadTile reads a tile from the specified log.
func (m *Manager) ReadTile(ctx context.Context, logID string, level, index uint64, p uint8) ([]byte, error) {
	reader, err := m.GetReader(ctx, logID)
	if err != nil {
		return nil, err
	}
	return reader.ReadTile(ctx, level, index, p)
}

// ReadCheckpoint reads the latest checkpoint from the specified log.
func (m *Manager) ReadCheckpoint(ctx context.Context, logID string) ([]byte, error) {
	reader, err := m.GetReader(ctx, logID)
	if err != nil {
		return nil, err
	}
	return reader.ReadCheckpoint(ctx)
}

// ReadRange reads entries from startIndex to endIndex (exclusive) from the specified log.
// This uses Tessera's bundle-based storage to efficiently read multiple entries.
//
// Note: Tessera stores entries in bundles, and partial bundles contain cumulative entries.
// This implementation provides basic ReadRange functionality by parsing bundles.
// In production, you'd want more sophisticated bundle parsing and caching.
func (m *Manager) ReadRange(ctx context.Context, logID string, startIndex, endIndex uint64) ([][]byte, error) {
	if startIndex >= endIndex {
		return nil, nil
	}

	reader, err := m.GetReader(ctx, logID)
	if err != nil {
		return nil, err
	}

	// For this simple implementation, read all entries up to endIndex
	// and then slice the result to return only the requested range.

	// Read the largest bundle that contains all entries up to endIndex
	allEntries, err := m.readAllEntriesUpTo(ctx, reader, endIndex)
	if err != nil {
		return nil, err
	}

	// Return the slice from startIndex to endIndex
	if startIndex >= uint64(len(allEntries)) {
		return nil, nil
	}
	if endIndex > uint64(len(allEntries)) {
		endIndex = uint64(len(allEntries))
	}

	return allEntries[startIndex:endIndex], nil
}

// entriesPerBundle is the number of entries in a full Tessera bundle (2^8)
const entriesPerBundle = 256

// readAllEntriesUpTo reads all entries from index 0 up to (but not including) the given index.
// Handles multi-bundle reading for logs with more than 256 entries.
func (m *Manager) readAllEntriesUpTo(ctx context.Context, reader tessera.LogReader, upToIndex uint64) ([][]byte, error) {
	if upToIndex == 0 {
		return nil, nil
	}

	// Get the actual log size to determine partial bundle sizes
	logSize, err := reader.NextIndex(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get log size: %w", err)
	}

	// Cap upToIndex at actual log size
	if upToIndex > logSize {
		upToIndex = logSize
	}

	var allEntries [][]byte

	// Calculate how many bundles we need to read
	lastBundleIdx := (upToIndex - 1) / entriesPerBundle

	for bundleIdx := uint64(0); bundleIdx <= lastBundleIdx; bundleIdx++ {
		// Determine if this bundle is full or partial
		bundleStartIdx := bundleIdx * entriesPerBundle
		bundleEndIdx := bundleStartIdx + entriesPerBundle
		if bundleEndIdx > logSize {
			bundleEndIdx = logSize
		}

		entriesInBundle := bundleEndIdx - bundleStartIdx
		isFullBundle := entriesInBundle == entriesPerBundle

		var bundle []byte
		if isFullBundle {
			// Full bundle: p=0
			bundle, err = reader.ReadEntryBundle(ctx, bundleIdx, 0)
		} else {
			// Partial bundle: p = number of entries in the bundle
			bundle, err = reader.ReadEntryBundle(ctx, bundleIdx, uint8(entriesInBundle))
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read bundle %d: %w", bundleIdx, err)
		}

		entries, err := parseBundleEntries(bundle)
		if err != nil {
			return nil, fmt.Errorf("failed to parse bundle %d: %w", bundleIdx, err)
		}

		allEntries = append(allEntries, entries...)
	}

	// Trim to exactly upToIndex entries
	if uint64(len(allEntries)) > upToIndex {
		allEntries = allEntries[:upToIndex]
	}

	return allEntries, nil
}

// parseBundleEntries parses a Tessera bundle and extracts individual entries
func parseBundleEntries(bundle []byte) ([][]byte, error) {
	var entries [][]byte
	offset := 0

	for offset < len(bundle) {
		if offset+2 > len(bundle) {
			break // Not enough bytes for length prefix
		}

		// Read 2-byte big-endian length
		entryLen := int(bundle[offset])<<8 | int(bundle[offset+1])
		offset += 2

		if offset+entryLen > len(bundle) {
			break // Not enough bytes for entry data
		}

		// Extract entry data
		entry := make([]byte, entryLen)
		copy(entry, bundle[offset:offset+entryLen])
		entries = append(entries, entry)

		offset += entryLen
	}

	return entries, nil
}

// dummy signer for minimal Tessera setup
type dummySigner struct{}

func (d *dummySigner) Name() string {
	return "dummy"
}

func (d *dummySigner) Sign([]byte) ([]byte, error) {
	return []byte("dummy"), nil
}

func (d *dummySigner) KeyHash() uint32 {
	return 0
}
