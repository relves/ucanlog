package storacha

import (
	"testing"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientPool(t *testing.T) {
	// Create service signer
	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	// Create customer signer for delegation
	customerSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := customerSigner.DID().String()

	// Create pool
	pool, err := NewClientPool(ClientPoolConfig{
		ServiceSigner: serviceSigner,
	})
	require.NoError(t, err)

	t.Run("create new client with delegation", func(t *testing.T) {
		// Create delegation
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		client, err := pool.GetClient("log1", spaceDID, dlg)
		require.NoError(t, err)
		assert.NotNil(t, client)
		assert.Equal(t, 1, pool.ClientCount())
	})

	t.Run("get existing client", func(t *testing.T) {
		// Create another delegation for same log
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		// Should return existing client
		client, err := pool.GetClient("log1", spaceDID, dlg)
		require.NoError(t, err)
		assert.NotNil(t, client)
		assert.Equal(t, 1, pool.ClientCount()) // Still only 1 client
	})

	t.Run("create client without delegation fails for new log", func(t *testing.T) {
		_, err := pool.GetClient("log-new", spaceDID, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delegation required")
	})

	t.Run("has client", func(t *testing.T) {
		assert.True(t, pool.HasClient("log1"))
		assert.False(t, pool.HasClient("non-existent"))
	})

	t.Run("invalidate client", func(t *testing.T) {
		// Create delegation
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		_, err = pool.GetClient("log-to-invalidate", spaceDID, dlg)
		require.NoError(t, err)
		assert.True(t, pool.HasClient("log-to-invalidate"))

		pool.InvalidateClient("log-to-invalidate")
		assert.False(t, pool.HasClient("log-to-invalidate"))
	})

	t.Run("clear pool", func(t *testing.T) {
		assert.Greater(t, pool.ClientCount(), 0)
		pool.Clear()
		assert.Equal(t, 0, pool.ClientCount())
	})

	t.Run("multiple logs with different spaces", func(t *testing.T) {
		pool.Clear()

		// Create another customer signer
		customer2Signer, err := signer.Generate()
		require.NoError(t, err)
		space2DID := customer2Signer.DID().String()

		dlg1, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		dlg2, err := delegation.Delegate(
			customer2Signer,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", space2DID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		_, err = pool.GetClient("logA", spaceDID, dlg1)
		require.NoError(t, err)

		_, err = pool.GetClient("logB", space2DID, dlg2)
		require.NoError(t, err)

		assert.Equal(t, 2, pool.ClientCount())
	})
}

func TestClientPoolConfig(t *testing.T) {
	t.Run("requires service signer", func(t *testing.T) {
		_, err := NewClientPool(ClientPoolConfig{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "ServiceSigner is required")
	})

	t.Run("applies defaults", func(t *testing.T) {
		serviceSigner, err := signer.Generate()
		require.NoError(t, err)

		pool, err := NewClientPool(ClientPoolConfig{
			ServiceSigner: serviceSigner,
		})
		require.NoError(t, err)

		// Check defaults are applied
		assert.Equal(t, "https://up.storacha.network", pool.serviceURL)
		assert.Equal(t, "did:web:up.storacha.network", pool.serviceDID)
		assert.Equal(t, "https://w3s.link", pool.gatewayURL)
	})
}
