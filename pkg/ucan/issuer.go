// internal/ucan/issuer.go
package ucan

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/verifier"

	"github.com/relves/ucanlog/pkg/types"
)

// Issuer creates and signs UCANs.
type Issuer struct {
	goIssuer *GoUCANIssuer
}

// NewIssuer creates a new UCAN issuer.
func NewIssuer(privateKey ed25519.PrivateKey, publicKey ed25519.PublicKey) *Issuer {
	goIssuer, err := NewGoUCANIssuer(privateKey)
	if err != nil {
		// This should not happen in normal operation
		panic(fmt.Sprintf("failed to create go-ucanto issuer: %v", err))
	}

	return &Issuer{
		goIssuer: goIssuer,
	}
}

// Deprecated: IssueRootUCAN is no longer used in the simplified delegation model.
// Authorization is now handled via Storacha space delegations.
// This method is kept for backward compatibility with tests.
func (i *Issuer) IssueRootUCAN(
	audience ed25519.PublicKey,
	groupID types.GroupID,
	ttl time.Duration,
) (delegation.Delegation, error) {
	// Create audience verifier from public key
	audienceVerifier, err := verifier.FromRaw(audience)
	if err != nil {
		return nil, fmt.Errorf("failed to create audience verifier: %w", err)
	}
	audienceDID := audienceVerifier.DID().String()

	// Use go-ucanto internally
	return i.goIssuer.IssueRootUCAN(context.Background(), audienceDID, groupID, ttl)
}

// IssueDelegatedUCAN creates a delegated UCAN with specific capabilities.
func (i *Issuer) IssueDelegatedUCAN(
	audience ed25519.PublicKey,
	capabilities []CapabilityInfo,
	proofs []delegation.Delegation,
	ttl time.Duration,
) (delegation.Delegation, error) {
	// Create audience verifier from public key
	audienceVerifier, err := verifier.FromRaw(audience)
	if err != nil {
		return nil, fmt.Errorf("failed to create audience verifier: %w", err)
	}
	audienceDID := audienceVerifier.DID().String()

	// Use go-ucanto with proper delegation proofs
	dlg, err := i.goIssuer.IssueDelegatedUCAN(context.Background(), audienceDID, capabilities, proofs, ttl)
	if err != nil {
		return nil, err
	}

	return dlg, nil
}

// DID returns the issuer's DID.
func (i *Issuer) DID() string {
	return i.goIssuer.DID()
}

// DIDToPublicKey extracts the public key from a DID.
func DIDToPublicKey(did string) (ed25519.PublicKey, error) {
	prefix := "did:key:z"
	if len(did) <= len(prefix) {
		return nil, fmt.Errorf("invalid DID format")
	}
	keyBytes, err := base64.RawURLEncoding.DecodeString(did[len(prefix):])
	if err != nil {
		return nil, fmt.Errorf("failed to decode DID: %w", err)
	}
	return ed25519.PublicKey(keyBytes), nil
}
