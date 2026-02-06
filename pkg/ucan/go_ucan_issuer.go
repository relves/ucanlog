package ucan

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"time"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"

	"github.com/relves/ucanlog/pkg/types"
)

// GoUCANIssuer creates and signs UCANs using go-ucanto.
type GoUCANIssuer struct {
	signer ucan.Signer
	did    string
}

// NewGoUCANIssuer creates a new UCAN issuer using go-ucanto.
func NewGoUCANIssuer(privateKey ed25519.PrivateKey) (*GoUCANIssuer, error) {
	// Convert our Ed25519 private key to go-ucanto signer
	edSigner, err := signer.FromRaw(privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create ed25519 signer: %w", err)
	}

	return &GoUCANIssuer{
		signer: edSigner,
		did:    edSigner.DID().String(),
	}, nil
}

// IssueRootUCAN creates a root UCAN granting full control to an audience using go-ucanto.
func (i *GoUCANIssuer) IssueRootUCAN(
	ctx context.Context,
	audienceDID string,
	groupID types.GroupID,
	ttl time.Duration,
) (delegation.Delegation, error) {
	// Parse audience DID
	audience, err := did.Parse(audienceDID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse audience DID: %w", err)
	}

	// Define capabilities
	capabilities := []ucan.Capability[ucan.NoCaveats]{
		ucan.NewCapability(
			ucan.Ability(types.CapabilityAll),
			ucan.Resource(types.ResourceURI(string(groupID))),
			ucan.NoCaveats{},
		),
	}

	// Create delegation
	exp := ucan.UTCUnixTimestamp(time.Now().Add(ttl).Unix())
	dlg, err := delegation.Delegate(
		i.signer,
		audience,
		capabilities,
		delegation.WithExpiration(int(exp)),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create delegation: %w", err)
	}

	return dlg, nil
}

// IssueDelegatedUCAN creates a delegated UCAN with specific capabilities using go-ucanto.
func (i *GoUCANIssuer) IssueDelegatedUCAN(
	ctx context.Context,
	audienceDID string,
	capabilities []CapabilityInfo,
	parentProofs []delegation.Delegation,
	ttl time.Duration,
) (delegation.Delegation, error) {
	// Parse audience DID
	audience, err := did.Parse(audienceDID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse audience DID: %w", err)
	}

	// Convert our Attenuation to go-ucanto capabilities
	goCapabilities := make([]ucan.Capability[ucan.NoCaveats], len(capabilities))
	for j, cap := range capabilities {
		goCapabilities[j] = ucan.NewCapability(
			ucan.Ability(cap.Can),
			ucan.Resource(cap.With),
			ucan.NoCaveats{},
		)
	}

	// Convert parent proofs to go-ucanto Proofs
	proofs := make([]delegation.Proof, len(parentProofs))
	for j, proof := range parentProofs {
		proofs[j] = delegation.FromDelegation(proof)
	}

	// Create delegation
	exp := ucan.UTCUnixTimestamp(time.Now().Add(ttl).Unix())
	dlg, err := delegation.Delegate(
		i.signer,
		audience,
		goCapabilities,
		delegation.WithExpiration(int(exp)),
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create delegated delegation: %w", err)
	}

	return dlg, nil
}

// DID returns the issuer's DID.
func (i *GoUCANIssuer) DID() string {
	return i.did
}
