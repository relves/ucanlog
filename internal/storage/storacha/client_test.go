package storacha

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMockClient_UploadAndFetch(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	data := []byte("test blob data for storacha")
	// MockClient ignores delegation parameter
	cid, err := client.UploadBlob(ctx, "did:key:test", data, nil)
	require.NoError(t, err)
	require.NotEmpty(t, cid)

	// Fetch should return same data
	fetched, err := client.FetchBlob(ctx, cid)
	require.NoError(t, err)
	require.Equal(t, data, fetched)
}

func TestMockClient_FetchNotFound(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	_, err := client.FetchBlob(ctx, "bafynotexist")
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestMockClient_DeterministicCID(t *testing.T) {
	client := NewMockClient()
	ctx := context.Background()

	data := []byte("same data")

	// MockClient ignores delegation parameter
	cid1, _ := client.UploadBlob(ctx, "did:key:test", data, nil)
	cid2, _ := client.UploadBlob(ctx, "did:key:test", data, nil)

	// Same data should produce same CID
	require.Equal(t, cid1, cid2)
}
