package storacha

import (
	"fmt"
	"log/slog"
	"net/url"

	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	ucantohttp "github.com/storacha/go-ucanto/transport/http"
)

// ClientPool manages a shared connection to the Storacha service.
// It creates a fresh GuppyClient per request so that each request uses its own
// delegation, avoiding the security problem of baking one delegation into a
// cached client that is reused for all subsequent requests.
type ClientPool struct {
	conn          uclient.Connection // cached — no I/O at construction time
	serviceSigner principal.Signer
	gatewayURL    string
	replicaCount  uint
	logger        *slog.Logger

	// Retained for test inspection only.
	serviceURL string
	serviceDID string
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

	// ReplicaCount is the number of replicas for blob replication.
	// Default: 3 (some accounts may have a max of 2)
	ReplicaCount uint

	// Logger for structured logging.
	// Default: slog.Default()
	Logger *slog.Logger
}

// NewClientPool creates a new client pool.
// It builds the Storacha service connection once; GetClient reuses it.
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

	serviceDID, err := did.Parse(cfg.ServiceDID)
	if err != nil {
		return nil, fmt.Errorf("invalid service DID: %w", err)
	}

	serviceURL, err := url.Parse(cfg.ServiceURL)
	if err != nil {
		return nil, fmt.Errorf("invalid service URL: %w", err)
	}

	channel := ucantohttp.NewChannel(serviceURL)
	conn, err := uclient.NewConnection(serviceDID, channel)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	return &ClientPool{
		conn:          conn,
		serviceSigner: cfg.ServiceSigner,
		serviceURL:    cfg.ServiceURL,
		serviceDID:    cfg.ServiceDID,
		gatewayURL:    cfg.GatewayURL,
		replicaCount:  cfg.ReplicaCount,
		logger:        cfg.Logger,
	}, nil
}

// GetClient creates a fresh GuppyClient for the given space and delegation.
// A new client is built on every call so that each request uses its own delegation.
// The shared connection is reused across calls (no I/O overhead).
func (p *ClientPool) GetClient(spaceDID string, dlg delegation.Delegation) (StorachaClient, error) {
	if dlg == nil {
		return nil, fmt.Errorf("delegation required")
	}

	return NewGuppyClientWithConnection(GuppyClientConfig{
		ServiceSigner: p.serviceSigner,
		Delegation:    dlg,
		SpaceDID:      spaceDID,
		GatewayURL:    p.gatewayURL,
		ReplicaCount:  p.replicaCount,
		Logger:        p.logger,
	}, p.conn)
}
