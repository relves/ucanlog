package storacha

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGatewayClient_FetchBlob(t *testing.T) {
	client := NewGatewayClient("https://example.com")
	client.httpClient = &http.Client{
		Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			assert.Equal(t, "/ipfs/bafytest123", r.URL.Path)
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("test data")),
				Header:     make(http.Header),
			}, nil
		}),
	}

	data, err := client.FetchBlob(context.Background(), "bafytest123")
	require.NoError(t, err)
	assert.Equal(t, []byte("test data"), data)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

