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
	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	customerSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := customerSigner.DID().String()

	pool, err := NewClientPool(ClientPoolConfig{
		ServiceSigner: serviceSigner,
	})
	require.NoError(t, err)

	makeDlg := func(t *testing.T) delegation.Delegation {
		t.Helper()
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)
		return dlg
	}

	t.Run("creates client with delegation", func(t *testing.T) {
		client, err := pool.GetClient(spaceDID, makeDlg(t))
		require.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("returns fresh client on every call", func(t *testing.T) {
		c1, err := pool.GetClient(spaceDID, makeDlg(t))
		require.NoError(t, err)
		c2, err := pool.GetClient(spaceDID, makeDlg(t))
		require.NoError(t, err)
		// Each call returns a distinct instance.
		assert.NotSame(t, c1, c2)
	})

	t.Run("nil delegation returns error", func(t *testing.T) {
		_, err := pool.GetClient(spaceDID, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "delegation required")
	})

	t.Run("different spaces each get a client", func(t *testing.T) {
		customer2Signer, err := signer.Generate()
		require.NoError(t, err)
		space2DID := customer2Signer.DID().String()

		dlg2, err := delegation.Delegate(
			customer2Signer,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", space2DID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		c1, err := pool.GetClient(spaceDID, makeDlg(t))
		require.NoError(t, err)
		c2, err := pool.GetClient(space2DID, dlg2)
		require.NoError(t, err)

		assert.NotNil(t, c1)
		assert.NotNil(t, c2)
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

		assert.Equal(t, "https://up.storacha.network", pool.serviceURL)
		assert.Equal(t, "did:web:up.storacha.network", pool.serviceDID)
		assert.Equal(t, "https://w3s.link", pool.gatewayURL)
	})
}
