package storacha

import (
	"context"
	"testing"

	"github.com/relves/ucanlog/internal/storage/storacha/storachatest"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/stretchr/testify/require"
)

// TestUploadCAR verifies that UploadCAR delegates to the client's UploadCAR method.
func TestUploadCAR(t *testing.T) {
	// Create mock client that tracks uploads
	mockClient := &trackingClient{
		uploadedBlobs: make(map[string][]byte),
		expectedCID:   "bafkreigw5mgxo2zalz7j5rnmeswpz6xo3wtqfvzmgm4exy5nvykwdnhfxu",
		uploadedData:  nil,
	}

	uploader := NewStorachaUploader(newClientRef(mockClient), "did:key:test")

	// Create context with mock delegation
	ctx := context.Background()
	ctx = WithDelegation(ctx, storachatest.MockDelegation())

	// Upload test data
	testData := []byte("test CAR data")
	cid, err := uploader.UploadCAR(ctx, testData)
	require.NoError(t, err)

	// Verify the CID matches what the client returned
	require.Equal(t, mockClient.expectedCID, cid)

	// Verify the data was passed to the client
	require.NotNil(t, mockClient.uploadedData)
	require.Equal(t, testData, mockClient.uploadedData)
}

// TestUploadCAR_NoClient verifies error when no client is configured
func TestUploadCAR_NoClient(t *testing.T) {
	uploader := NewStorachaUploader(newClientRef(nil), "did:key:test")
	_, err := uploader.UploadCAR(context.Background(), []byte{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no client configured")
}

// trackingClient tracks blob uploads for testing
type trackingClient struct {
	uploadedBlobs map[string][]byte
	expectedCID   string
	uploadedData  []byte
}

func (c *trackingClient) UploadBlob(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	// Compute CID from data
	cidStr, _, err := ComputeCID(data)
	if err != nil {
		return "", err
	}
	c.uploadedBlobs[cidStr] = data
	return cidStr, nil
}

func (c *trackingClient) UploadCAR(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	// Store the data for verification
	c.uploadedData = data

	// For mock purposes, compute CID and return
	cidStr, _, err := ComputeCID(data)
	if err != nil {
		return "", err
	}
	c.uploadedBlobs[cidStr] = data
	return c.expectedCID, nil
}

func (c *trackingClient) FetchBlob(ctx context.Context, cid string) ([]byte, error) {
	data, ok := c.uploadedBlobs[cid]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (c *trackingClient) RemoveBlob(ctx context.Context, spaceDID string, digest []byte, dlg delegation.Delegation) error {
	return nil
}
