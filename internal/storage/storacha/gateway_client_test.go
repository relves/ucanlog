package storacha

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatewayClient_FetchBlob(t *testing.T) {
	// Mock IPFS gateway server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/ipfs/bafytest123", r.URL.Path)
		w.Write([]byte("test data"))
	}))
	defer server.Close()

	client := NewGatewayClient(server.URL)

	data, err := client.FetchBlob(context.Background(), "bafytest123")
	require.NoError(t, err)
	assert.Equal(t, []byte("test data"), data)
}

func TestGatewayClient_UploadBlob_ReturnsError(t *testing.T) {
	client := NewGatewayClient("https://example.com")

	// GatewayClient ignores delegation parameter but requires it for interface compliance
	_, err := client.UploadBlob(context.Background(), "did:key:test", []byte("data"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")
}

func TestGatewayClient_UploadCAR_ReturnsError(t *testing.T) {
	client := NewGatewayClient("https://example.com")

	// GatewayClient ignores delegation parameter but requires it for interface compliance
	_, err := client.UploadCAR(context.Background(), "did:key:test", []byte("data"), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read-only")
}
