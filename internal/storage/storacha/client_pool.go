package storacha

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal"
)

// ClientPool manages a pool of DelegatedClients for different logs/spaces.
// This avoids recreating clients on every request while allowing delegation updates.
type ClientPool struct {
	mu            sync.RWMutex
	clients       map[string]*DelegatedClient // keyed by logID
	serviceSigner principal.Signer
	serviceURL    string
	serviceDID    string
	gatewayURL    string
	logger        *slog.Logger
}

// ClientPoolConfig configures the client pool.
type ClientPoolConfig struct {
	// ServiceSigner is the service's identity for signing invocations.
	ServiceSigner principal.Signer

	// ServiceURL is the Storacha upload service URL.
	// Default: https://up.storacha.network
	ServiceURL string

	// ServiceDID is the Storacha service DID.
	// Default: did:web:up.storacha.network
	ServiceDID string

	// GatewayURL is the IPFS gateway URL.
	// Default: https://w3s.link
	GatewayURL string

	// Logger for structured logging.
	// Default: slog.Default()
	Logger *slog.Logger
}

// NewClientPool creates a new client pool.
func NewClientPool(cfg ClientPoolConfig) (*ClientPool, error) {
	if cfg.ServiceSigner == nil {
		return nil, fmt.Errorf("ServiceSigner is required")
	}

	if cfg.ServiceURL == "" {
		cfg.ServiceURL = "https://up.storacha.network"
	}
	if cfg.ServiceDID == "" {
		cfg.ServiceDID = "did:web:up.storacha.network"
	}
	if cfg.GatewayURL == "" {
		cfg.GatewayURL = "https://w3s.link"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &ClientPool{
		clients:       make(map[string]*DelegatedClient),
		serviceSigner: cfg.ServiceSigner,
		serviceURL:    cfg.ServiceURL,
		serviceDID:    cfg.ServiceDID,
		gatewayURL:    cfg.GatewayURL,
		logger:        cfg.Logger,
	}, nil
}

// GetClient retrieves or creates a client for a log.
// The returned client is configured for the space but does NOT cache delegations.
// Delegations must be passed per-request to upload methods.
func (p *ClientPool) GetClient(logID string, spaceDID string, dlg delegation.Delegation) (*DelegatedClient, error) {
	p.mu.RLock()
	client, exists := p.clients[logID]
	p.mu.RUnlock()

	if exists {
		// Client exists - return it (delegation is NOT cached or updated)
		return client, nil
	}

	// Create new client
	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	if client, exists = p.clients[logID]; exists {
		return client, nil
	}

	if dlg == nil {
		return nil, fmt.Errorf("delegation required for new client")
	}

	newClient, err := NewDelegatedClient(DelegatedClientConfig{
		ServiceSigner: p.serviceSigner,
		Delegation:    dlg,
		SpaceDID:      spaceDID,
		ServiceDID:    p.serviceDID,
		ServiceURL:    p.serviceURL,
		GatewayURL:    p.gatewayURL,
		Logger:        p.logger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create delegated client: %w", err)
	}

	p.clients[logID] = newClient
	return newClient, nil
}

// GetOrCreateClient retrieves an existing client or creates one with the provided delegation.
// Unlike GetClient, this requires a delegation for new clients but doesn't update existing ones.
func (p *ClientPool) GetOrCreateClient(logID string, spaceDID string, dlg delegation.Delegation) (*DelegatedClient, error) {
	p.mu.RLock()
	client, exists := p.clients[logID]
	p.mu.RUnlock()

	if exists {
		return client, nil
	}

	return p.GetClient(logID, spaceDID, dlg)
}

// InvalidateClient removes a client from the pool.
// Use this when a delegation is revoked or expired.
func (p *ClientPool) InvalidateClient(logID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.clients, logID)
}

// HasClient checks if a client exists for a log.
func (p *ClientPool) HasClient(logID string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, exists := p.clients[logID]
	return exists
}

// ClientCount returns the number of clients in the pool.
func (p *ClientPool) ClientCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.clients)
}

// Clear removes all clients from the pool.
func (p *ClientPool) Clear() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.clients = make(map[string]*DelegatedClient)
}
