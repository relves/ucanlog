package storacha

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/storacha/go-ucanto/core/delegation"
)

// GatewayClient is a read-only StorachaClient that fetches blobs via IPFS gateway.
// It does not support uploads - use DelegatedClient for write operations.
type GatewayClient struct {
	gatewayURL string
	httpClient *http.Client
}

// NewGatewayClient creates a new read-only gateway client.
func NewGatewayClient(gatewayURL string) *GatewayClient {
	return &GatewayClient{
		gatewayURL: gatewayURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// FetchBlob retrieves data by CID via the IPFS gateway.
func (c *GatewayClient) FetchBlob(ctx context.Context, cidStr string) ([]byte, error) {
	url := fmt.Sprintf("%s/ipfs/%s", c.gatewayURL, cidStr)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gateway request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gateway returned status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return data, nil
}

// UploadBlob returns an error - this client is read-only.
// The delegation parameter is accepted but ignored since this is a read-only client.
func (c *GatewayClient) UploadBlob(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	return "", fmt.Errorf("GatewayClient is read-only: cannot upload blob")
}

// UploadCAR returns an error - this client is read-only.
// The delegation parameter is accepted but ignored since this is a read-only client.
func (c *GatewayClient) UploadCAR(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	return "", fmt.Errorf("GatewayClient is read-only: cannot upload CAR")
}

// RemoveBlob returns an error - this client is read-only.
// The delegation parameter is accepted but ignored since this is a read-only client.
func (c *GatewayClient) RemoveBlob(ctx context.Context, spaceDID string, digest []byte, dlg delegation.Delegation) error {
	return fmt.Errorf("GatewayClient is read-only: cannot remove blob")
}

// Ensure GatewayClient implements StorachaClient.
var _ StorachaClient = (*GatewayClient)(nil)
