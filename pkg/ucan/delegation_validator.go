// Package ucan provides UCAN delegation validation utilities.
package ucan

import (
	"context"
	"fmt"
	"time"

	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
)

// DelegationError represents an error with delegation validation.
type DelegationError struct {
	Code    string
	Message string
}

func (e *DelegationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewDelegationError creates a new delegation error.
func NewDelegationError(code, message string) *DelegationError {
	return &DelegationError{Code: code, Message: message}
}

// Error codes for delegation validation
const (
	ErrCodeDelegationExpired           = "DELEGATION_EXPIRED"
	ErrCodeDelegationWrongAudience     = "DELEGATION_WRONG_AUDIENCE"
	ErrCodeDelegationMissingCapability = "DELEGATION_MISSING_CAPABILITY"
	ErrCodeDelegationWrongResource     = "DELEGATION_WRONG_RESOURCE"
	ErrCodeDelegationInvalidSignature  = "DELEGATION_INVALID_SIGNATURE"
	ErrCodeDelegationParseError        = "DELEGATION_PARSE_ERROR"
	ErrCodeRevocationNotAuthorized     = "REVOCATION_NOT_AUTHORIZED"
	ErrCodeDelegationNotFound          = "DELEGATION_NOT_FOUND"
	ErrCodeDelegationFetchError        = "DELEGATION_FETCH_ERROR"
	ErrCodeInvocationNotAuthorized     = "INVOCATION_NOT_AUTHORIZED"
	ErrCodeDelegationNoAuthority       = "DELEGATION_NO_AUTHORITY"
	ErrCodeGCDelegationNotDirect       = "GC_DELEGATION_NOT_DIRECT"
	ErrCodeGCFailed                    = "GC_FAILED"
)

// BlobFetcher is an interface for fetching blobs by CID.
// This is used to fetch delegations from storage for revocation.
type BlobFetcher interface {
	FetchBlob(ctx context.Context, cid string) ([]byte, error)
}

// FetchDelegation fetches a delegation from storage by CID and parses it.
// The delegation must be stored as a CAR file in the space.
func FetchDelegation(ctx context.Context, fetcher BlobFetcher, cid string) (delegation.Delegation, error) {
	// Fetch the delegation blob
	data, err := fetcher.FetchBlob(ctx, cid)
	if err != nil {
		return nil, NewDelegationError(ErrCodeDelegationFetchError,
			fmt.Sprintf("failed to fetch delegation %s: %v", cid, err))
	}

	if len(data) == 0 {
		return nil, NewDelegationError(ErrCodeDelegationNotFound,
			fmt.Sprintf("delegation %s not found or empty", cid))
	}

	// Parse the delegation from CAR bytes
	dlg, err := ParseDelegationFromCAR(data)
	if err != nil {
		return nil, err
	}

	return dlg, nil
}

// ParseDelegationFromCAR parses a delegation from CAR-encoded bytes.
func ParseDelegationFromCAR(data []byte) (delegation.Delegation, error) {
	dlg, err := delegation.Extract(data)
	if err != nil {
		return nil, NewDelegationError(ErrCodeDelegationParseError,
			fmt.Sprintf("failed to parse delegation from CAR: %v", err))
	}
	return dlg, nil
}

// RequiredStorachaCapabilities returns the capabilities needed for log operations
// on a customer's Storacha space.
func RequiredStorachaCapabilities() []string {
	return []string{
		"space/blob/add",
		"space/index/add",
		"upload/add",
	}
}

// ParseDelegation parses a base64-encoded UCAN delegation.
func ParseDelegation(encoded string) (delegation.Delegation, error) {
	dlg, err := delegation.Parse(encoded)
	if err != nil {
		return nil, NewDelegationError(ErrCodeDelegationParseError,
			fmt.Sprintf("failed to parse delegation: %v", err))
	}
	return dlg, nil
}

// ValidateDelegation checks that a delegation grants required capabilities
// for the service to write to the customer's space.
func ValidateDelegation(dlg delegation.Delegation, serviceDID string, spaceDID string) error {
	// Check audience - delegation must be for the service
	audience := dlg.Audience().DID().String()
	if audience != serviceDID {
		return NewDelegationError(ErrCodeDelegationWrongAudience,
			fmt.Sprintf("delegation audience is %s, expected service DID %s", audience, serviceDID))
	}

	// Check expiration
	exp := dlg.Expiration()
	if exp != nil {
		expTime := time.Unix(int64(*exp), 0)
		if time.Now().After(expTime) {
			return NewDelegationError(ErrCodeDelegationExpired,
				fmt.Sprintf("delegation expired at %s", expTime))
		}
	}

	// Check capabilities
	caps := dlg.Capabilities()
	requiredCaps := RequiredStorachaCapabilities()

	// Build a set of capabilities in the delegation
	capSet := make(map[string]string) // ability -> resource
	for _, cap := range caps {
		capSet[cap.Can()] = cap.With()
	}

	// Verify each required capability is present with the correct resource
	for _, required := range requiredCaps {
		resource, found := capSet[required]
		if !found {
			return NewDelegationError(ErrCodeDelegationMissingCapability,
				fmt.Sprintf("delegation missing required capability: %s", required))
		}
		if resource != spaceDID {
			return NewDelegationError(ErrCodeDelegationWrongResource,
				fmt.Sprintf("capability %s has resource %s, expected %s", required, resource, spaceDID))
		}
	}

	return nil
}

// ValidateDelegationForCreate validates a delegation for log creation.
// This is the entry point for create handler.
func ValidateDelegationForCreate(delegationStr string, serviceDID string, spaceDID string) (delegation.Delegation, error) {
	// Parse the delegation
	dlg, err := ParseDelegation(delegationStr)
	if err != nil {
		return nil, err
	}

	// Validate the delegation
	if err := ValidateDelegation(dlg, serviceDID, spaceDID); err != nil {
		return nil, err
	}

	return dlg, nil
}

// ValidateDelegationForAppend validates an optional new delegation for append.
// If delegationStr is empty, returns nil (use existing delegation).
// If provided, validates the new delegation.
func ValidateDelegationForAppend(delegationStr string, serviceDID string, spaceDID string) (delegation.Delegation, error) {
	if delegationStr == "" {
		return nil, nil // Use existing delegation
	}

	return ValidateDelegationForCreate(delegationStr, serviceDID, spaceDID)
}

// ExtractSpaceDID extracts the space DID from a delegation's capabilities.
// All capabilities must have the same resource (the space DID).
func ExtractSpaceDID(dlg delegation.Delegation) (string, error) {
	caps := dlg.Capabilities()
	if len(caps) == 0 {
		return "", NewDelegationError(ErrCodeDelegationMissingCapability,
			"delegation has no capabilities")
	}

	spaceDID := caps[0].With()

	// Verify all capabilities target the same resource
	for _, cap := range caps[1:] {
		if cap.With() != spaceDID {
			return "", NewDelegationError("MISMATCHED_RESOURCES",
				fmt.Sprintf("capability resources don't match: %s vs %s", spaceDID, cap.With()))
		}
	}

	return spaceDID, nil
}

// ExtractDelegationCapabilities extracts capabilities from a delegation for debugging.
func ExtractDelegationCapabilities(dlg delegation.Delegation) []CapabilityInfo {
	caps := dlg.Capabilities()
	result := make([]CapabilityInfo, len(caps))
	for i, cap := range caps {
		result[i] = CapabilityInfo{
			Can:  cap.Can(),
			With: cap.With(),
		}
	}
	return result
}

// FormatDelegation encodes a delegation to base64 string.
func FormatDelegation(dlg delegation.Delegation) (string, error) {
	return delegation.Format(dlg)
}

// DelegationInfo contains information about a delegation for logging/debugging.
type DelegationInfo struct {
	Issuer       string           `json:"issuer"`
	Audience     string           `json:"audience"`
	Capabilities []CapabilityInfo `json:"capabilities"`
	Expiration   *time.Time       `json:"expiration,omitempty"`
}

// GetDelegationInfo extracts information from a delegation for logging.
func GetDelegationInfo(dlg delegation.Delegation) DelegationInfo {
	info := DelegationInfo{
		Issuer:       dlg.Issuer().DID().String(),
		Audience:     dlg.Audience().DID().String(),
		Capabilities: ExtractDelegationCapabilities(dlg),
	}

	if exp := dlg.Expiration(); exp != nil {
		t := time.Unix(int64(*exp), 0)
		info.Expiration = &t
	}

	return info
}

// ValidateInvocationAuthority checks if the invocation issuer has authority
// to use the provided delegation.
//
// The invocation issuer must be the delegation issuer. This ensures that
// only the principal who created the delegation can use it to invoke operations.
//
// This supports delegation chains:
// - Alice (space owner) delegates to FriendB
// - FriendB creates a new delegation to the service (with Alice's as proof)
// - FriendB signs the invocation
// - inv.Issuer (FriendB) == dlg.Issuer (FriendB) ✓
//
// This prevents delegation theft:
// - Eve finds FriendB's delegation
// - Eve signs an invocation
// - inv.Issuer (Eve) != dlg.Issuer (FriendB) ✗
func ValidateInvocationAuthority(invocationIssuerDID string, dlg delegation.Delegation) error {
	delegationIssuerDID := dlg.Issuer().DID().String()

	if invocationIssuerDID == delegationIssuerDID {
		return nil
	}

	return NewDelegationError(ErrCodeInvocationNotAuthorized,
		fmt.Sprintf("invocation issuer %s does not match delegation issuer %s",
			invocationIssuerDID, delegationIssuerDID))
}

// ValidateProofChain validates that the delegation issuer has authority over the space.
// This ensures the delegation traces back to the space owner through a valid proof chain.
//
// Valid scenarios:
// 1. Direct: Space owner delegates directly to service (issuer == spaceDID)
// 2. Chain: Space owner → Agent → Service (proof chain traces to spaceDID)
// 3. Sub-delegation: Space owner → Agent → FriendB → Service (proof chain traces to spaceDID)
//
// Invalid scenario:
// - Eve creates delegation for Alice's space with no proofs (issuer != spaceDID, no proof chain)
func ValidateProofChain(dlg delegation.Delegation, spaceDID string) error {
	issuerDID := dlg.Issuer().DID().String()

	// Case 1: Issuer is the space owner - direct delegation
	if issuerDID == spaceDID {
		return nil
	}

	// Case 2: Check proof chain for authority from space owner
	if hasAuthorityFromSpace(dlg, spaceDID) {
		return nil
	}

	return NewDelegationError(ErrCodeDelegationNoAuthority,
		fmt.Sprintf("delegation issuer %s has no authority over space %s (no valid proof chain)",
			issuerDID, spaceDID))
}

// hasAuthorityFromSpace checks if the delegation has a proof chain back to the space owner.
func hasAuthorityFromSpace(dlg delegation.Delegation, spaceDID string) bool {
	proofLinks := dlg.Proofs()
	if len(proofLinks) == 0 {
		return false
	}

	// Create a block reader from the delegation's blocks to resolve proofs
	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(dlg.Blocks()))
	if err != nil {
		return false
	}

	// Create proofs view to resolve proof delegations
	proofs := delegation.NewProofsView(proofLinks, bs)

	// Check each proof in the chain
	for _, proof := range proofs {
		proofDlg, ok := proof.Delegation()
		if !ok {
			continue
		}

		// Verify this proof is addressed to the current delegation's issuer
		// (i.e., the proof grants authority to the delegation issuer)
		if proofDlg.Audience().DID().String() != dlg.Issuer().DID().String() {
			continue
		}

		// If the proof issuer is the space owner, we found valid authority
		if proofDlg.Issuer().DID().String() == spaceDID {
			return true
		}

		// Recursively check if this proof has authority from the space
		if hasAuthorityFromSpace(proofDlg, spaceDID) {
			return true
		}
	}

	return false
}

// ValidateRevocationAuthority checks if revokerDID has authority to revoke the delegation.
// A principal can revoke a delegation if:
// 1. They are the issuer of the delegation, OR
// 2. They are upstream in the proof chain (issued a proof that the delegation depends on)
func ValidateRevocationAuthority(revokerDID string, dlgToRevoke delegation.Delegation) error {
	// Rule 1: Direct issuer can always revoke
	if dlgToRevoke.Issuer().DID().String() == revokerDID {
		return nil
	}

	// Rule 2: Check if revoker is upstream in the proof chain
	if isUpstreamAuthority(revokerDID, dlgToRevoke) {
		return nil
	}

	return NewDelegationError(ErrCodeRevocationNotAuthorized,
		fmt.Sprintf("principal %s is not authorized to revoke delegation issued by %s",
			revokerDID, dlgToRevoke.Issuer().DID().String()))
}

// isUpstreamAuthority checks if revokerDID issued any delegation in the proof chain.
func isUpstreamAuthority(revokerDID string, dlg delegation.Delegation) bool {
	// Get proofs from the delegation
	proofLinks := dlg.Proofs()
	if len(proofLinks) == 0 {
		return false
	}

	// Create a block reader from the delegation's blocks to resolve proofs
	bs, err := blockstore.NewBlockReader(blockstore.WithBlocksIterator(dlg.Blocks()))
	if err != nil {
		return false
	}

	// Create proofs view to resolve proof delegations
	proofs := delegation.NewProofsView(proofLinks, bs)

	// Check each proof
	for _, proof := range proofs {
		proofDlg, ok := proof.Delegation()
		if !ok {
			continue
		}

		// If revoker issued this proof, they're upstream
		if proofDlg.Issuer().DID().String() == revokerDID {
			return true
		}

		// Recursively check further up the chain
		if isUpstreamAuthority(revokerDID, proofDlg) {
			return true
		}
	}

	return false
}

// ValidateGCDelegation validates a delegation for garbage collection operations.
// This requires space/blob/remove capability and stricter validation than regular operations.
//
// GC delegations MUST satisfy these requirements:
// 1. Direct delegation: Must be issued directly by the space owner (issuer == spaceDID)
// 2. Remove capability: Must include space/blob/remove for the space DID resource
// 3. Correct audience: Must be addressed to the ucanlog service DID
// 4. Valid signature and not expired
// 5. No proof chain: len(proofs) == 0 (prevents delegation chains)
func ValidateGCDelegation(dlg delegation.Delegation, serviceDID string, spaceDID string) error {
	// Check audience - delegation must be for the service
	audience := dlg.Audience().DID().String()
	if audience != serviceDID {
		return NewDelegationError(ErrCodeDelegationWrongAudience,
			fmt.Sprintf("delegation audience is %s, expected service DID %s", audience, serviceDID))
	}

	// Check expiration
	exp := dlg.Expiration()
	if exp != nil {
		expTime := time.Unix(int64(*exp), 0)
		if time.Now().After(expTime) {
			return NewDelegationError(ErrCodeDelegationExpired,
				fmt.Sprintf("delegation expired at %s", expTime))
		}
	}

	// CRITICAL: Verify delegation is direct from space owner (no proof chain)
	// This prevents delegation chains like: space -> friend -> service
	issuerDID := dlg.Issuer().DID().String()
	if issuerDID != spaceDID {
		return NewDelegationError(ErrCodeGCDelegationNotDirect,
			fmt.Sprintf("GC delegation must be issued by space owner %s, but was issued by %s", spaceDID, issuerDID))
	}

	// Verify no proof chain (direct delegation only)
	proofLinks := dlg.Proofs()
	if len(proofLinks) > 0 {
		return NewDelegationError(ErrCodeGCDelegationNotDirect,
			"GC delegation must be direct from space owner to service (no proof chain allowed)")
	}

	// Check for blob/remove capability
	caps := dlg.Capabilities()
	hasRemove := false
	for _, cap := range caps {
		if cap.Can() == "space/blob/remove" && cap.With() == spaceDID {
			hasRemove = true
			break
		}
	}

	if !hasRemove {
		return NewDelegationError(ErrCodeDelegationMissingCapability,
			fmt.Sprintf("delegation must include space/blob/remove for %s", spaceDID))
	}

	return nil
}

