// Package storachatest provides test helpers for Storacha storage
package storachatest

import (
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	"github.com/storacha/go-ucanto/ucan"
)

// MockDelegation creates a mock delegation for testing purposes.
// This is useful for tests that need a valid delegation but don't care about its content.
func MockDelegation() delegation.Delegation {
	// Create a test signer
	signer, err := signer.Generate()
	if err != nil {
		panic(err)
	}

	// Create a simple delegation
	dlg, err := delegation.Delegate(
		signer,
		signer.DID(),
		[]ucan.Capability[ucan.NoCaveats]{
			ucan.NewCapability("space/blob/add", signer.DID().String(), ucan.NoCaveats{}),
		},
	)
	if err != nil {
		panic(err)
	}

	return dlg
}
