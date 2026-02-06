package storacha

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"strings"
	"sync"

	"github.com/storacha/go-ucanto/core/delegation"
)

// StorachaClient defines the interface for Storacha blob operations.
type StorachaClient interface {
	// UploadBlob uploads data to the given space and returns its CID.
	// The delegation parameter provides authorization for this specific write.
	UploadBlob(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error)

	// UploadCAR uploads CAR data to Storacha using dag/add capability.
	// This properly indexes all blocks in the CAR, making the DAG browsable.
	// The delegation parameter provides authorization for this specific write.
	UploadCAR(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error)

	// FetchBlob retrieves data by CID (no delegation needed for reads).
	FetchBlob(ctx context.Context, cid string) ([]byte, error)

	// RemoveBlob removes a blob from the space by its multihash.
	// The delegation parameter provides authorization for this specific delete.
	RemoveBlob(ctx context.Context, spaceDID string, digest []byte, dlg delegation.Delegation) error
}

// MockClient is a mock implementation for testing.
// It stores blobs in memory and generates deterministic CIDs.
type MockClient struct {
	blobs map[string][]byte
	mu    sync.RWMutex
}

// NewMockClient creates a new mock Storacha client.
func NewMockClient() *MockClient {
	return &MockClient{
		blobs: make(map[string][]byte),
	}
}

// UploadBlob stores data and returns a deterministic CID.
// The delegation parameter is ignored for mock purposes.
func (c *MockClient) UploadBlob(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Generate deterministic CID from content hash
	cid := generateMockCID(data)
	c.blobs[cid] = append([]byte(nil), data...) // Copy data

	return cid, nil
}

// UploadCAR stores CAR data and returns the CAR root CID.
// For mock purposes, it stores the CAR data and returns a mock CID for the root.
// The delegation parameter is ignored for mock purposes.
func (c *MockClient) UploadCAR(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Generate a mock root CID - in real implementation this would be extracted from the CAR
	rootCID := generateMockCID(data)
	c.blobs[rootCID] = append([]byte(nil), data...) // Copy data

	return rootCID, nil
}

// FetchBlob retrieves data by CID.
func (c *MockClient) FetchBlob(ctx context.Context, cid string) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	data, ok := c.blobs[cid]
	if !ok {
		return nil, fmt.Errorf("blob not found: %s", cid)
	}

	return append([]byte(nil), data...), nil // Return copy
}

// RemoveBlob removes a blob from mock storage.
// The delegation parameter is ignored for mock purposes.
func (c *MockClient) RemoveBlob(ctx context.Context, spaceDID string, digest []byte, dlg delegation.Delegation) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// In mock, we can't easily reverse the hash to find the CID
	// Just return success - the actual implementation will use the digest
	return nil
}

// generateMockCID creates a deterministic CID-like string from data.
// Uses "bafymock" prefix + base32-encoded SHA-256 hash.
func generateMockCID(data []byte) string {
	hash := sha256.Sum256(data)
	encoded := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(hash[:])
	return "bafymock" + strings.ToLower(encoded[:32])
}

// Ensure MockClient implements StorachaClient.
var _ StorachaClient = (*MockClient)(nil)
