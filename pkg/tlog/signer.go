package tlog

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
)

// Ed25519Signer implements tessera.Signer using Ed25519 keys
type Ed25519Signer struct {
	privateKey ed25519.PrivateKey
	publicKey  ed25519.PublicKey
	name       string
}

// NewEd25519Signer creates a new Ed25519 signer for Tessera checkpoints
func NewEd25519Signer(privateKey ed25519.PrivateKey, name string) (*Ed25519Signer, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key size: got %d, want %d", len(privateKey), ed25519.PrivateKeySize)
	}

	// Derive public key from private key
	publicKey := privateKey.Public().(ed25519.PublicKey)

	if name == "" {
		// Default name format: log-<first-8-hex-chars-of-pubkey>
		name = fmt.Sprintf("log-%x", publicKey[:4])
	}

	return &Ed25519Signer{
		privateKey: privateKey,
		publicKey:  publicKey,
		name:       name,
	}, nil
}

// Name returns the signer's name (used in checkpoint format)
func (s *Ed25519Signer) Name() string {
	return s.name
}

// Sign creates an Ed25519 signature over the given data
func (s *Ed25519Signer) Sign(data []byte) ([]byte, error) {
	signature := ed25519.Sign(s.privateKey, data)
	return signature, nil
}

// KeyHash returns the key ID per the signed note format (c2sp.org/signed-note).
// The key ID is SHA256(name + "\n" + encoded_key)[:4] where encoded_key is
// the type byte (0x01 for Ed25519) followed by the public key bytes.
func (s *Ed25519Signer) KeyHash() uint32 {
	// Build encoded key: type byte (0x01 = Ed25519) + public key
	encoded := append([]byte{0x01}, s.publicKey...)
	// Hash: name + "\n" + encoded_key
	h := sha256.Sum256([]byte(s.name + "\n" + string(encoded)))
	return uint32(h[0])<<24 | uint32(h[1])<<16 | uint32(h[2])<<8 | uint32(h[3])
}

// PublicKey returns the Ed25519 public key
func (s *Ed25519Signer) PublicKey() ed25519.PublicKey {
	return s.publicKey
}
