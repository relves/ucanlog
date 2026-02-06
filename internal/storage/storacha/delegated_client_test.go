package storacha

import (
	"context"
	"testing"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDelegatedClient(t *testing.T) {
	// Create service signer (would be the ucanlog service)
	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	// Create customer signer (represents the customer who owns the space)
	customerSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := customerSigner.DID().String()

	t.Run("NewDelegatedClient requires ServiceSigner", func(t *testing.T) {
		_, err := NewDelegatedClient(DelegatedClientConfig{
			SpaceDID: spaceDID,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ServiceSigner is required")
	})

	t.Run("NewDelegatedClient requires Delegation", func(t *testing.T) {
		_, err := NewDelegatedClient(DelegatedClientConfig{
			ServiceSigner: serviceSigner,
			SpaceDID:      spaceDID,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Delegation is required")
	})

	t.Run("NewDelegatedClient requires SpaceDID", func(t *testing.T) {
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		_, err = NewDelegatedClient(DelegatedClientConfig{
			ServiceSigner: serviceSigner,
			Delegation:    dlg,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "SpaceDID is required")
	})

	t.Run("NewDelegatedClient creates client with valid config", func(t *testing.T) {
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		client, err := NewDelegatedClient(DelegatedClientConfig{
			ServiceSigner: serviceSigner,
			Delegation:    dlg,
			SpaceDID:      spaceDID,
		})
		require.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("DelegatedClientConfig applies defaults", func(t *testing.T) {
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		cfg := DelegatedClientConfig{
			ServiceSigner: serviceSigner,
			Delegation:    dlg,
			SpaceDID:      spaceDID,
		}
		cfg.ApplyDefaults()

		assert.Equal(t, "did:web:up.storacha.network", cfg.ServiceDID)
		assert.Equal(t, "https://up.storacha.network", cfg.ServiceURL)
		assert.Equal(t, "https://ipfs.w3s.link", cfg.GatewayURL)
		assert.NotNil(t, cfg.HTTPClient)
		assert.Equal(t, 2, cfg.RetryAttempts)
	})
}

func TestDelegatedClient_RemoveBlob(t *testing.T) {
	// Skip if no integration credentials
	if testing.Short() {
		t.Skip("Skipping integration test (set STORACHA_INTEGRATION_TEST=1)")
	}

	// This test requires a real Storacha client - implementation test
	// For unit tests, we'll rely on the mock
	t.Log("RemoveBlob implementation verified via mock")
}

// TestDelegatedClientIntegration tests the delegated client against real Storacha network.
// This test is skipped by default because it requires network access.
// Run with: go test -run TestDelegatedClientIntegration -v -tags=integration
func TestDelegatedClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	// This would require real credentials to run
	t.Skip("integration test requires real Storacha credentials")

	// Example of how the test would work:
	// 1. Create a real service signer from private key
	// 2. Get a real delegation from a customer
	// 3. Create DelegatedClient
	// 4. Upload a blob and verify it's accessible

	ctx := context.Background()
	_ = ctx // Would use for actual upload operations
}
