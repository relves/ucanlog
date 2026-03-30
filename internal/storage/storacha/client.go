package storacha

import (
	"context"
	"crypto/sha256"
	"encoding/base32"
	"fmt"
	"strings"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-libstoracha/blobindex"
)

// StorachaClient defines the interface for Storacha blob operations.
type StorachaClient interface {
	// FetchBlob retrieves data by CID (no delegation needed for reads).
	FetchBlob(ctx context.Context, cid string) ([]byte, error)

	// UploadFinalizedBlob uploads a single immutable data blob through the full
	// Storacha pipeline including IPNI indexing, so the gateway can resolve it by CID.
	// Pipeline: SpaceBlobAdd → Replicate → build trivial ShardedDagIndex →
	// SpaceBlobAdd(index) → Replicate(index) → SpaceIndexAdd → UploadAdd.
	// Returns the CID of the uploaded blob.
	UploadFinalizedBlob(ctx context.Context, data []byte) (string, error)

	// UploadFullStateCAR uploads a full-state CAR and registers it as the log head.
	UploadFullStateCAR(ctx context.Context, carData []byte, rootCID cid.Cid, positions map[cid.Cid]blobindex.Position) (string, error)
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
func (c *MockClient) UploadBlob(ctx context.Context, spaceDID string, data []byte) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Generate deterministic CID from content hash
	cid := generateMockCID(data)
	c.blobs[cid] = append([]byte(nil), data...) // Copy data

	return cid, nil
}

// UploadCAR stores CAR data and returns the CAR root CID and raw blob CID.
// For mock purposes, it stores the CAR data and returns mock CIDs.
func (c *MockClient) UploadCAR(ctx context.Context, spaceDID string, data []byte) (string, string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Generate mock CIDs - in real implementation these would be extracted from the CAR
	rootCID := generateMockCID(data)
	rawCID := "bafkrei" + generateMockCID(data)[8:] // different prefix for raw CID
	c.blobs[rootCID] = append([]byte(nil), data...) // Copy data

	return rootCID, rawCID, nil
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
func (c *MockClient) RemoveBlob(ctx context.Context, spaceDID string, digest []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// In mock, we can't easily reverse the hash to find the CID
	// Just return success - the actual implementation will use the digest
	return nil
}

// UploadFinalizedBlob stores blob data in mock and returns a deterministic CID.
func (c *MockClient) UploadFinalizedBlob(ctx context.Context, data []byte) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cidStr := generateMockCID(data)
	c.blobs[cidStr] = append([]byte(nil), data...)
	return cidStr, nil
}

// UploadFullStateCAR stores CAR data and returns the root CID as head CID.
func (c *MockClient) UploadFullStateCAR(ctx context.Context, carData []byte, rootCID cid.Cid, positions map[cid.Cid]blobindex.Position) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	head := rootCID.String()
	c.blobs[head] = append([]byte(nil), carData...)
	return head, nil
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
