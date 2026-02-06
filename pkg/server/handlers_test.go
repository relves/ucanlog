package server

import (
	"testing"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ucanPkg "github.com/relves/ucanlog/pkg/ucan"
)

// TestDelegationRequirements tests the delegation-based authorization model.
// These tests verify the API contract without requiring full server setup.
func TestDelegationRequirements(t *testing.T) {
	// Create test signers
	spaceOwnerSigner, err := signer.Generate()
	require.NoError(t, err)

	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := spaceOwnerSigner.DID().String()
	serviceDID := serviceSigner.DID().String()

	t.Run("CreateCaveats only requires delegation field", func(t *testing.T) {
		// Create a valid delegation
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		encoded, err := delegation.Format(dlg)
		require.NoError(t, err)

		// Parse and validate the delegation
		parsed, err := ucanPkg.ParseDelegation(encoded)
		require.NoError(t, err)

		// Extract space DID from delegation
		extractedSpaceDID, err := ucanPkg.ExtractSpaceDID(parsed)
		require.NoError(t, err)
		assert.Equal(t, spaceDID, extractedSpaceDID)

		// Validate delegation
		err = ucanPkg.ValidateDelegation(parsed, serviceDID, extractedSpaceDID)
		assert.NoError(t, err)
	})

	t.Run("AppendCaveats requires delegation field", func(t *testing.T) {
		// This test verifies that the delegation field is required
		// and that we can extract space DID from it

		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		encoded, err := delegation.Format(dlg)
		require.NoError(t, err)

		// Simulate what the handler does: parse delegation
		parsed, err := ucanPkg.ParseDelegation(encoded)
		require.NoError(t, err)

		// Extract space DID to use as log identity
		logIdentity, err := ucanPkg.ExtractSpaceDID(parsed)
		require.NoError(t, err)

		// Space DID should be the log identity
		assert.Equal(t, spaceDID, logIdentity)
	})

	t.Run("SpaceDID is used as log identity", func(t *testing.T) {
		// Create delegation
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		// The log identity should be the space DID
		extractedSpaceDID, err := ucanPkg.ExtractSpaceDID(dlg)
		require.NoError(t, err)

		// Verify the space DID matches what we expect
		assert.Equal(t, spaceDID, extractedSpaceDID)

		// The space DID starts with "did:key:"
		assert.Contains(t, extractedSpaceDID, "did:key:")
	})
}

// TestRevokeCaveatsRequiresCIDAndDelegation tests that revoke requires both
// a CID of the delegation to revoke and a storage delegation.
func TestRevokeCaveatsRequiresCIDAndDelegation(t *testing.T) {
	spaceOwnerSigner, err := signer.Generate()
	require.NoError(t, err)

	userBSigner, err := signer.Generate()
	require.NoError(t, err)

	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := spaceOwnerSigner.DID().String()
	serviceDID := serviceSigner.DID().String()

	// Create a delegation chain: SpaceOwner -> UserB
	ownerToUserB, err := delegation.Delegate(
		spaceOwnerSigner,
		userBSigner.DID(),
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
		},
	)
	require.NoError(t, err)

	// Create storage delegation for accessing the space
	storageDelegation, err := delegation.Delegate(
		spaceOwnerSigner,
		serviceSigner.DID(),
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
			ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
		},
	)
	require.NoError(t, err)

	t.Run("revoke uses CID to identify delegation", func(t *testing.T) {
		// The CID of the delegation to revoke
		cidToRevoke := ownerToUserB.Link().String()
		assert.NotEmpty(t, cidToRevoke)
		assert.Contains(t, cidToRevoke, "bafy") // CIDs start with "bafy"

		// Storage delegation is encoded as base64
		storageDlgStr, err := delegation.Format(storageDelegation)
		require.NoError(t, err)
		assert.NotEmpty(t, storageDlgStr)

		// Parse and validate storage delegation
		storageDlg, err := ucanPkg.ParseDelegation(storageDlgStr)
		require.NoError(t, err)

		extractedSpaceDID, err := ucanPkg.ExtractSpaceDID(storageDlg)
		require.NoError(t, err)

		err = ucanPkg.ValidateDelegation(storageDlg, serviceDID, extractedSpaceDID)
		assert.NoError(t, err)
	})

	t.Run("only issuer or upstream can revoke", func(t *testing.T) {
		// UserB cannot revoke owner's delegation to them
		err := ucanPkg.ValidateRevocationAuthority(userBSigner.DID().String(), ownerToUserB)
		assert.Error(t, err)

		// But spaceOwner can revoke their own delegation
		err = ucanPkg.ValidateRevocationAuthority(spaceOwnerSigner.DID().String(), ownerToUserB)
		assert.NoError(t, err)
	})
}

// TestEmptyDelegationRejected verifies empty delegations are rejected.
func TestEmptyDelegationRejected(t *testing.T) {
	t.Run("empty delegation string is rejected", func(t *testing.T) {
		_, err := ucanPkg.ParseDelegation("")
		assert.Error(t, err)
	})

	t.Run("invalid delegation string is rejected", func(t *testing.T) {
		_, err := ucanPkg.ParseDelegation("not-a-valid-delegation")
		assert.Error(t, err)
	})
}

// TestDelegationMustHaveRequiredCapabilities verifies capability requirements.
func TestDelegationMustHaveRequiredCapabilities(t *testing.T) {
	spaceOwnerSigner, err := signer.Generate()
	require.NoError(t, err)

	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := spaceOwnerSigner.DID().String()
	serviceDID := serviceSigner.DID().String()

	t.Run("delegation with all capabilities is valid", func(t *testing.T) {
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		err = ucanPkg.ValidateDelegation(dlg, serviceDID, spaceDID)
		assert.NoError(t, err)
	})

	t.Run("delegation missing capability is invalid", func(t *testing.T) {
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				// Missing space/index/add and upload/add
			},
		)
		require.NoError(t, err)

		err = ucanPkg.ValidateDelegation(dlg, serviceDID, spaceDID)
		assert.Error(t, err)
	})

	t.Run("delegation to wrong audience is invalid", func(t *testing.T) {
		otherSigner, err := signer.Generate()
		require.NoError(t, err)

		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			otherSigner.DID(), // Wrong audience
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		err = ucanPkg.ValidateDelegation(dlg, serviceDID, spaceDID)
		assert.Error(t, err)
	})
}

// TestInvocationAuthorization tests the "invocation issuer == delegation issuer" rule.
func TestInvocationAuthorization(t *testing.T) {
	// Setup: create signers
	serviceSigner, _ := signer.Generate()

	spaceOwnerSigner, _ := signer.Generate()
	spaceDID := spaceOwnerSigner.DID().String()

	friendBSigner, _ := signer.Generate()
	friendBDID := friendBSigner.DID().String()

	eveSigner, _ := signer.Generate()
	eveDID := eveSigner.DID().String()

	t.Run("space owner can invoke with their own delegation", func(t *testing.T) {
		// Alice creates delegation: Alice → Service
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		// Alice invokes (inv.Issuer = Alice, dlg.Issuer = Alice)
		invocationIssuerDID := spaceOwnerSigner.DID().String()
		err = ucanPkg.ValidateInvocationAuthority(invocationIssuerDID, dlg)
		assert.NoError(t, err)
	})

	t.Run("friend can invoke with their delegation in chain", func(t *testing.T) {
		// Alice creates delegation: Alice → FriendB
		aliceToFriend, err := delegation.Delegate(
			spaceOwnerSigner,
			friendBSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		// FriendB creates delegation: FriendB → Service (with proof)
		friendToService, err := delegation.Delegate(
			friendBSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(aliceToFriend)),
		)
		require.NoError(t, err)

		// FriendB invokes (inv.Issuer = FriendB, dlg.Issuer = FriendB)
		invocationIssuerDID := friendBDID
		err = ucanPkg.ValidateInvocationAuthority(invocationIssuerDID, friendToService)
		assert.NoError(t, err)
	})

	t.Run("eve cannot use friend's delegation", func(t *testing.T) {
		// Alice → FriendB → Service delegation chain
		aliceToFriend, _ := delegation.Delegate(
			spaceOwnerSigner,
			friendBSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		friendToService, _ := delegation.Delegate(
			friendBSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(aliceToFriend)),
		)

		// Eve invokes with FriendB's delegation
		// inv.Issuer = Eve, dlg.Issuer = FriendB
		invocationIssuerDID := eveDID
		err := ucanPkg.ValidateInvocationAuthority(invocationIssuerDID, friendToService)

		assert.Error(t, err)
		var dlgErr *ucanPkg.DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ucanPkg.ErrCodeInvocationNotAuthorized, dlgErr.Code)
	})

	t.Run("eve cannot use space owner's delegation", func(t *testing.T) {
		// Alice creates delegation: Alice → Service
		dlg, _ := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)

		// Eve invokes with Alice's delegation
		// inv.Issuer = Eve, dlg.Issuer = Alice
		invocationIssuerDID := eveDID
		err := ucanPkg.ValidateInvocationAuthority(invocationIssuerDID, dlg)

		assert.Error(t, err)
		var dlgErr *ucanPkg.DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ucanPkg.ErrCodeInvocationNotAuthorized, dlgErr.Code)
	})
}

// TestProofChainValidation tests that delegations must have valid proof chains.
func TestProofChainValidation(t *testing.T) {
	// Setup: create signers
	serviceSigner, _ := signer.Generate()

	spaceOwnerSigner, _ := signer.Generate()
	spaceDID := spaceOwnerSigner.DID().String()

	agentSigner, _ := signer.Generate()
	eveSigner, _ := signer.Generate()

	t.Run("direct delegation from space owner is valid", func(t *testing.T) {
		dlg, _ := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)

		err := ucanPkg.ValidateProofChain(dlg, spaceDID)
		assert.NoError(t, err)
	})

	t.Run("delegation chain through agent is valid", func(t *testing.T) {
		// Space owner → Agent
		ownerToAgent, _ := delegation.Delegate(
			spaceOwnerSigner,
			agentSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)

		// Agent → Service (with proof)
		agentToService, _ := delegation.Delegate(
			agentSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(ownerToAgent)),
		)

		err := ucanPkg.ValidateProofChain(agentToService, spaceDID)
		assert.NoError(t, err)
	})

	t.Run("eve's delegation without proof chain is rejected", func(t *testing.T) {
		// Eve creates delegation claiming access to Alice's space
		eveDlg, _ := delegation.Delegate(
			eveSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			// No proofs!
		)

		err := ucanPkg.ValidateProofChain(eveDlg, spaceDID)
		assert.Error(t, err)

		var dlgErr *ucanPkg.DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ucanPkg.ErrCodeDelegationNoAuthority, dlgErr.Code)
	})
}
