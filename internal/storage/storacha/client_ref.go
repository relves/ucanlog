package storacha

import "sync"

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
