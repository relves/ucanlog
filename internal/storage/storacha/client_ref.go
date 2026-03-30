package storacha

import (
	"context"
	"fmt"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/storacha/go-libstoracha/blobindex"
)

// clientRef is a shared, concurrency-safe reference to a StorachaClient.
// All components that need the client hold a pointer to this instead of
// a bare StorachaClient, so that SetClient updates propagate automatically.
type clientRef struct {
	mu     sync.RWMutex
	client StorachaClient
}

func newClientRef(c StorachaClient) *clientRef {
	return &clientRef{client: c}
}

func (r *clientRef) Get() StorachaClient {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.client
}

func (r *clientRef) Set(c StorachaClient) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.client = c
}

// UploadFinalizedBlob implements uploadqueue.StorachaUploader by forwarding to the current client.
func (r *clientRef) UploadFinalizedBlob(ctx context.Context, data []byte) (string, error) {
	c := r.Get()
	if c == nil {
		return "", fmt.Errorf("no Storacha client configured")
	}
	return c.UploadFinalizedBlob(ctx, data)
}

// UploadFullStateCAR implements uploadqueue.StorachaUploader by forwarding to the current client.
func (r *clientRef) UploadFullStateCAR(ctx context.Context, carData []byte, rootCID cid.Cid, positions map[cid.Cid]blobindex.Position) (string, error) {
	c := r.Get()
	if c == nil {
		return "", fmt.Errorf("no Storacha client configured")
	}
	return c.UploadFullStateCAR(ctx, carData, rootCID, positions)
}
