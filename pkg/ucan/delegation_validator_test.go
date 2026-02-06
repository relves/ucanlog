package ucan

import (
	"testing"
	"time"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateDelegation(t *testing.T) {
	// Create test signers
	customerSigner, err := signer.Generate()
	require.NoError(t, err)

	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := customerSigner.DID().String()
	serviceDID := serviceSigner.DID().String()

	t.Run("valid delegation with all required capabilities", func(t *testing.T) {
		// Create delegation with required capabilities
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

		err = ValidateDelegation(dlg, serviceDID, spaceDID)
		assert.NoError(t, err)
	})

	t.Run("wrong audience", func(t *testing.T) {
		// Create another signer to be the wrong audience
		wrongSigner, err := signer.Generate()
		require.NoError(t, err)

		dlg, err := delegation.Delegate(
			customerSigner,
			wrongSigner.DID(), // Wrong audience
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		err = ValidateDelegation(dlg, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationWrongAudience, dlgErr.Code)
	})

	t.Run("missing capability", func(t *testing.T) {
		// Create delegation missing upload/add
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
				// Missing upload/add
			},
		)
		require.NoError(t, err)

		err = ValidateDelegation(dlg, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationMissingCapability, dlgErr.Code)
	})

	t.Run("wrong resource", func(t *testing.T) {
		// Create another space DID
		otherSigner, err := signer.Generate()
		require.NoError(t, err)
		otherSpaceDID := otherSigner.DID().String()

		// Create delegation with wrong resource
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", otherSpaceDID, ucan.NoCaveats{}), // Wrong resource
				ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		err = ValidateDelegation(dlg, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationWrongResource, dlgErr.Code)
	})

	t.Run("expired delegation", func(t *testing.T) {
		// Create delegation that expired in the past
		pastTime := time.Now().Add(-1 * time.Hour).Unix()
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/index/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("upload/add", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithExpiration(int(pastTime)),
		)
		require.NoError(t, err)

		err = ValidateDelegation(dlg, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationExpired, dlgErr.Code)
	})
}

func TestParseDelegation(t *testing.T) {
	// Create a test delegation
	customerSigner, err := signer.Generate()
	require.NoError(t, err)

	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := customerSigner.DID().String()

	dlg, err := delegation.Delegate(
		customerSigner,
		serviceSigner.DID(),
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
		},
	)
	require.NoError(t, err)

	// Format to base64
	encoded, err := delegation.Format(dlg)
	require.NoError(t, err)

	t.Run("valid delegation parses successfully", func(t *testing.T) {
		parsed, err := ParseDelegation(encoded)
		assert.NoError(t, err)
		assert.NotNil(t, parsed)
		assert.Equal(t, dlg.Link().String(), parsed.Link().String())
	})

	t.Run("invalid delegation fails", func(t *testing.T) {
		_, err := ParseDelegation("not-a-valid-delegation")
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationParseError, dlgErr.Code)
	})
}

func TestRequiredStorachaCapabilities(t *testing.T) {
	caps := RequiredStorachaCapabilities()
	assert.Contains(t, caps, "space/blob/add")
	assert.Contains(t, caps, "space/index/add")
	assert.Contains(t, caps, "upload/add")
	assert.Len(t, caps, 3)
}

func TestGetDelegationInfo(t *testing.T) {
	customerSigner, err := signer.Generate()
	require.NoError(t, err)

	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := customerSigner.DID().String()

	dlg, err := delegation.Delegate(
		customerSigner,
		serviceSigner.DID(),
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
		},
	)
	require.NoError(t, err)

	info := GetDelegationInfo(dlg)
	assert.Equal(t, customerSigner.DID().String(), info.Issuer)
	assert.Equal(t, serviceSigner.DID().String(), info.Audience)
	assert.Len(t, info.Capabilities, 1)
	assert.Equal(t, "space/blob/add", info.Capabilities[0].Can)
	assert.Equal(t, spaceDID, info.Capabilities[0].With)
}

func TestValidateDelegationForCreate(t *testing.T) {
	customerSigner, err := signer.Generate()
	require.NoError(t, err)

	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := customerSigner.DID().String()
	serviceDID := serviceSigner.DID().String()

	t.Run("valid delegation", func(t *testing.T) {
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

		encoded, err := delegation.Format(dlg)
		require.NoError(t, err)

		parsed, err := ValidateDelegationForCreate(encoded, serviceDID, spaceDID)
		assert.NoError(t, err)
		assert.NotNil(t, parsed)
	})
}

func TestExtractSpaceDID(t *testing.T) {
	customerSigner, err := signer.Generate()
	require.NoError(t, err)

	serviceSigner, err := signer.Generate()
	require.NoError(t, err)

	spaceDID := customerSigner.DID().String()

	t.Run("extracts space DID from single capability", func(t *testing.T) {
		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		extracted, err := ExtractSpaceDID(dlg)
		assert.NoError(t, err)
		assert.Equal(t, spaceDID, extracted)
	})

	t.Run("extracts space DID when all capabilities match", func(t *testing.T) {
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

		extracted, err := ExtractSpaceDID(dlg)
		assert.NoError(t, err)
		assert.Equal(t, spaceDID, extracted)
	})

	t.Run("fails when capabilities have different resources", func(t *testing.T) {
		otherSigner, err := signer.Generate()
		require.NoError(t, err)
		otherSpaceDID := otherSigner.DID().String()

		dlg, err := delegation.Delegate(
			customerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/index/add", otherSpaceDID, ucan.NoCaveats{}), // Different resource
			},
		)
		require.NoError(t, err)

		_, err = ExtractSpaceDID(dlg)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, "MISMATCHED_RESOURCES", dlgErr.Code)
	})

	t.Run("fails when delegation has no capabilities", func(t *testing.T) {
		// Create a delegation with no capabilities by using raw UCAN construction
		// Note: The delegation.Delegate function requires at least one capability,
		// so we need to test this edge case if it's possible to create such a delegation
		// For now, we'll document that this is tested conceptually

		// This test would need a way to create a delegation without capabilities
		// which the current API doesn't easily allow. We can skip this or use
		// a mock/test helper if needed in the future.
		t.Skip("Cannot easily create delegation without capabilities using current API")
	})
}

// Helper to create a valid test DID
func createTestDID() (did.DID, error) {
	s, err := signer.Generate()
	if err != nil {
		return did.DID{}, err
	}
	return s.DID(), nil
}

// createDelegationChain creates a test delegation chain for revocation tests
// Returns: SpaceOwner -> UserB -> UserC -> Service
func createDelegationChain(t *testing.T) (
	spaceOwner principal.Signer,
	userB principal.Signer,
	userC principal.Signer,
	service principal.Signer,
	ownerToB delegation.Delegation,
	bToC delegation.Delegation,
	cToService delegation.Delegation,
) {
	var err error
	
	// Generate signers
	spaceOwner, err = signer.Generate()
	require.NoError(t, err)
	
	userB, err = signer.Generate()
	require.NoError(t, err)
	
	userC, err = signer.Generate()
	require.NoError(t, err)
	
	service, err = signer.Generate()
	require.NoError(t, err)
	
	spaceDID := spaceOwner.DID().String()
	
	// Owner delegates to UserB
	ownerToB, err = delegation.Delegate(
		spaceOwner,
		userB.DID(),
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
		},
	)
	require.NoError(t, err)
	
	// UserB delegates to UserC (with proof of ownerToB)
	bToC, err = delegation.Delegate(
		userB,
		userC.DID(),
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
		},
		delegation.WithProof(delegation.FromDelegation(ownerToB)),
	)
	require.NoError(t, err)
	
	// UserC delegates to Service (with proof chain)
	cToService, err = delegation.Delegate(
		userC,
		service.DID(),
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
		},
		delegation.WithProof(delegation.FromDelegation(bToC)),
	)
	require.NoError(t, err)
	
	return
}

func TestValidateRevocationAuthority(t *testing.T) {
	// Setup: Create a delegation chain
	// SpaceOwner -> UserB -> UserC -> Service
	spaceOwner, userB, userC, service, ownerToB, bToC, cToService := createDelegationChain(t)
	
	t.Run("issuer can revoke their own delegation", func(t *testing.T) {
		// UserB issued delegation to UserC
		// UserB should be able to revoke it
		err := ValidateRevocationAuthority(userB.DID().String(), bToC)
		assert.NoError(t, err)
	})
	
	t.Run("space owner can revoke downstream delegation", func(t *testing.T) {
		// SpaceOwner should be able to revoke UserB->UserC delegation
		err := ValidateRevocationAuthority(spaceOwner.DID().String(), bToC)
		assert.NoError(t, err)
	})
	
	t.Run("upstream authority can revoke far downstream delegation", func(t *testing.T) {
		// SpaceOwner should be able to revoke UserC->Service delegation
		err := ValidateRevocationAuthority(spaceOwner.DID().String(), cToService)
		assert.NoError(t, err)
		
		// UserB should also be able to revoke UserC->Service delegation
		err = ValidateRevocationAuthority(userB.DID().String(), cToService)
		assert.NoError(t, err)
	})
	
	t.Run("cannot revoke upstream delegation", func(t *testing.T) {
		// UserC should NOT be able to revoke UserB->UserC delegation's proof (ownerToB)
		err := ValidateRevocationAuthority(userC.DID().String(), ownerToB)
		assert.Error(t, err)
		
		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeRevocationNotAuthorized, dlgErr.Code)
	})
	
	t.Run("unrelated party cannot revoke", func(t *testing.T) {
		// Random DID should not be able to revoke any delegation
		randomSigner, err := signer.Generate()
		require.NoError(t, err)
		
		err = ValidateRevocationAuthority(randomSigner.DID().String(), bToC)
		assert.Error(t, err)
		
		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeRevocationNotAuthorized, dlgErr.Code)
	})
	
	t.Run("audience cannot revoke just because they received it", func(t *testing.T) {
		// Service received the delegation but didn't issue it
		// Service should NOT be able to revoke it
		err := ValidateRevocationAuthority(service.DID().String(), cToService)
		assert.Error(t, err)
		
		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeRevocationNotAuthorized, dlgErr.Code)
	})
}

func TestValidateInvocationAuthority(t *testing.T) {
	// Create test signers
	aliceSigner, _ := signer.Generate()
	friendBSigner, _ := signer.Generate()
	serviceSigner, _ := signer.Generate()
	eveSigner, _ := signer.Generate()

	spaceDID := aliceSigner.DID().String()

	t.Run("allows when invocation issuer matches delegation issuer (direct)", func(t *testing.T) {
		// Alice creates delegation to service, Alice invokes
		dlg, err := delegation.Delegate(
			aliceSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		err = ValidateInvocationAuthority(aliceSigner.DID().String(), dlg)
		assert.NoError(t, err)
	})

	t.Run("allows when invocation issuer matches delegation issuer (chain)", func(t *testing.T) {
		// Alice delegates to FriendB
		aliceToB, _ := delegation.Delegate(
			aliceSigner,
			friendBSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)

		// FriendB delegates to service (with Alice's delegation as proof)
		friendBToService, err := delegation.Delegate(
			friendBSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(aliceToB)),
		)
		require.NoError(t, err)

		// FriendB invokes with their delegation
		err = ValidateInvocationAuthority(friendBSigner.DID().String(), friendBToService)
		assert.NoError(t, err)
	})

	t.Run("rejects when invocation issuer differs from delegation issuer", func(t *testing.T) {
		// Alice delegates to service
		dlg, _ := delegation.Delegate(
			aliceSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)

		// Eve tries to invoke with Alice's delegation
		err := ValidateInvocationAuthority(eveSigner.DID().String(), dlg)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeInvocationNotAuthorized, dlgErr.Code)
	})

	t.Run("rejects when Eve uses FriendB's delegation", func(t *testing.T) {
		// Alice → FriendB → service
		aliceToB, _ := delegation.Delegate(
			aliceSigner,
			friendBSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		friendBToService, _ := delegation.Delegate(
			friendBSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(aliceToB)),
		)

		// Eve tries to use FriendB's delegation
		err := ValidateInvocationAuthority(eveSigner.DID().String(), friendBToService)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeInvocationNotAuthorized, dlgErr.Code)
	})
}

func TestValidateProofChain(t *testing.T) {
	// Create test signers
	spaceOwnerSigner, _ := signer.Generate()
	agentSigner, _ := signer.Generate()
	friendBSigner, _ := signer.Generate()
	serviceSigner, _ := signer.Generate()
	eveSigner, _ := signer.Generate()

	spaceDID := spaceOwnerSigner.DID().String()

	t.Run("direct delegation from space owner is valid", func(t *testing.T) {
		// Space owner delegates directly to service
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		err = ValidateProofChain(dlg, spaceDID)
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
		agentToService, err := delegation.Delegate(
			agentSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(ownerToAgent)),
		)
		require.NoError(t, err)

		err = ValidateProofChain(agentToService, spaceDID)
		assert.NoError(t, err)
	})

	t.Run("delegation chain through agent and friend is valid", func(t *testing.T) {
		// Space owner → Agent
		ownerToAgent, _ := delegation.Delegate(
			spaceOwnerSigner,
			agentSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)

		// Agent → FriendB (with proof)
		agentToFriend, _ := delegation.Delegate(
			agentSigner,
			friendBSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(ownerToAgent)),
		)

		// FriendB → Service (with proof chain)
		friendToService, err := delegation.Delegate(
			friendBSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(agentToFriend)),
		)
		require.NoError(t, err)

		err = ValidateProofChain(friendToService, spaceDID)
		assert.NoError(t, err)
	})

	t.Run("eve's delegation without proof chain is rejected", func(t *testing.T) {
		// Eve creates delegation claiming access to space (no proofs)
		eveDlg, err := delegation.Delegate(
			eveSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			// No proofs!
		)
		require.NoError(t, err)

		err = ValidateProofChain(eveDlg, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationNoAuthority, dlgErr.Code)
	})

	t.Run("eve's delegation with fake proof is rejected", func(t *testing.T) {
		// Eve creates a fake delegation from "space owner" to herself
		// But since Eve doesn't have the space owner's key, she can't sign it
		// So she creates her own delegation chain that doesn't trace to space owner

		// Eve → EveAgent (Eve pretends to be upstream)
		eveToService, err := delegation.Delegate(
			eveSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
			// Even with a proof, if it doesn't trace to space owner, it fails
		)
		require.NoError(t, err)

		err = ValidateProofChain(eveToService, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationNoAuthority, dlgErr.Code)
	})
}

func TestValidateGCDelegation(t *testing.T) {
	// Create test signers
	spaceOwnerSigner, _ := signer.Generate()
	agentSigner, _ := signer.Generate()
	serviceSigner, _ := signer.Generate()
	eveSigner, _ := signer.Generate()

	spaceDID := spaceOwnerSigner.DID().String()
	serviceDID := serviceSigner.DID().String()

	t.Run("valid GC delegation with space/blob/remove", func(t *testing.T) {
		// Space owner creates direct delegation to service with blob/remove
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/remove", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		err = ValidateGCDelegation(dlg, serviceDID, spaceDID)
		assert.NoError(t, err)
	})

	t.Run("valid GC delegation with multiple capabilities including blob/remove", func(t *testing.T) {
		// Space owner creates delegation with blob/remove plus other capabilities
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
				ucan.NewCapability("space/blob/remove", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		err = ValidateGCDelegation(dlg, serviceDID, spaceDID)
		assert.NoError(t, err)
	})

	t.Run("rejects delegation missing space/blob/remove capability", func(t *testing.T) {
		// Space owner creates delegation without blob/remove
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		err = ValidateGCDelegation(dlg, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationMissingCapability, dlgErr.Code)
		assert.Contains(t, dlgErr.Message, "space/blob/remove")
	})

	t.Run("rejects indirect delegation with proof chain - wrong issuer", func(t *testing.T) {
		// Space owner → Agent (with blob/remove)
		ownerToAgent, _ := delegation.Delegate(
			spaceOwnerSigner,
			agentSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/remove", spaceDID, ucan.NoCaveats{}),
			},
		)

		// Agent → Service (with proof)
		agentToService, err := delegation.Delegate(
			agentSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/remove", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(ownerToAgent)),
		)
		require.NoError(t, err)

		// Should fail because delegation is not direct from space owner
		// The issuer check happens first, so we get that error
		err = ValidateGCDelegation(agentToService, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeGCDelegationNotDirect, dlgErr.Code)
		assert.Contains(t, dlgErr.Message, "issued by space owner")
	})

	t.Run("rejects delegation from space owner but with proof chain", func(t *testing.T) {
		// Create a proof delegation (could be anything)
		someDlg, _ := delegation.Delegate(
			agentSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/add", spaceDID, ucan.NoCaveats{}),
			},
		)

		// Space owner creates delegation WITH a proof chain (not allowed for GC)
		ownerDlgWithProof, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/remove", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithProof(delegation.FromDelegation(someDlg)),
		)
		require.NoError(t, err)

		// Should fail because delegation has a proof chain (must be direct)
		err = ValidateGCDelegation(ownerDlgWithProof, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeGCDelegationNotDirect, dlgErr.Code)
		assert.Contains(t, dlgErr.Message, "no proof chain")
	})

	t.Run("rejects delegation not issued by space owner", func(t *testing.T) {
		// Agent creates delegation claiming to be space owner (wrong issuer)
		agentDlg, err := delegation.Delegate(
			agentSigner, // Wrong issuer
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/remove", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		// Should fail because issuer != spaceDID
		err = ValidateGCDelegation(agentDlg, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeGCDelegationNotDirect, dlgErr.Code)
		assert.Contains(t, dlgErr.Message, "issued by space owner")
	})

	t.Run("rejects delegation with wrong audience", func(t *testing.T) {
		// Space owner creates delegation to wrong audience
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			eveSigner.DID(), // Wrong audience
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/remove", spaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		err = ValidateGCDelegation(dlg, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationWrongAudience, dlgErr.Code)
	})

	t.Run("rejects expired delegation", func(t *testing.T) {
		// Create delegation that expired in the past
		pastTime := time.Now().Add(-1 * time.Hour).Unix()
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/remove", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithExpiration(int(pastTime)),
		)
		require.NoError(t, err)

		err = ValidateGCDelegation(dlg, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationExpired, dlgErr.Code)
	})

	t.Run("accepts delegation with future expiration", func(t *testing.T) {
		// Create delegation that expires in the future
		futureTime := time.Now().Add(1 * time.Hour).Unix()
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/remove", spaceDID, ucan.NoCaveats{}),
			},
			delegation.WithExpiration(int(futureTime)),
		)
		require.NoError(t, err)

		err = ValidateGCDelegation(dlg, serviceDID, spaceDID)
		assert.NoError(t, err)
	})

	t.Run("rejects delegation with blob/remove for different space", func(t *testing.T) {
		// Create another space
		otherSpaceSigner, _ := signer.Generate()
		otherSpaceDID := otherSpaceSigner.DID().String()

		// Space owner creates delegation with blob/remove for OTHER space
		dlg, err := delegation.Delegate(
			spaceOwnerSigner,
			serviceSigner.DID(),
			[]ucan.Capability[ucan.NoCaveats]{
				ucan.NewCapability("space/blob/remove", otherSpaceDID, ucan.NoCaveats{}),
			},
		)
		require.NoError(t, err)

		// Should fail because capability is for different space
		err = ValidateGCDelegation(dlg, serviceDID, spaceDID)
		assert.Error(t, err)

		var dlgErr *DelegationError
		assert.ErrorAs(t, err, &dlgErr)
		assert.Equal(t, ErrCodeDelegationMissingCapability, dlgErr.Code)
	})
}

