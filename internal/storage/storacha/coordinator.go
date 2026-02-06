package storacha

import (
	"context"
	"sync"

	"github.com/relves/ucanlog/internal/storage"
)

// coordinator manages sequencing and tree state for the Storacha driver.
// Uses StateStore (SQLite) for coordination and state persistence.
// SQLite's transaction isolation handles concurrency.
type coordinator struct {
	stateStore storage.StateStore
	logDID     string
	mu         sync.Mutex
}

// newCoordinator creates a new coordinator backed by StateStore.
func newCoordinator(stateStore storage.StateStore, logDID string) (*coordinator, error) {
	return &coordinator{
		stateStore: stateStore,
		logDID:     logDID,
	}, nil
}

// lock acquires an in-process lock for the given operation.
// SQLite handles database-level concurrency via transactions.
// Returns an unlock function that must be called when done.
func (c *coordinator) lock(ctx context.Context, operation string) (func(), error) {
	c.mu.Lock()
	return func() {
		c.mu.Unlock()
	}, nil
}

// readTreeState reads the current tree state from StateStore.
func (c *coordinator) readTreeState(ctx context.Context) (uint64, []byte, error) {
	return c.stateStore.GetTreeState(ctx, c.logDID)
}

// writeTreeState persists the tree state to StateStore.
func (c *coordinator) writeTreeState(ctx context.Context, size uint64, root []byte) error {
	return c.stateStore.SetTreeState(ctx, c.logDID, size, root)
}

// readNextIndex returns the next available sequence number.
func (c *coordinator) readNextIndex(ctx context.Context) (uint64, error) {
	size, _, err := c.readTreeState(ctx)
	return size, err
}
