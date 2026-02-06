package storacha

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoordinator_TreeState(t *testing.T) {
	stateStore := newMockStateStore()
	coord, err := newCoordinator(stateStore, "did:key:test")
	require.NoError(t, err)

	ctx := context.Background()

	// Initial state should be empty
	size, root, err := coord.readTreeState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(0), size)
	require.Nil(t, root)

	// Write state
	newRoot := []byte("merkle root hash here")
	err = coord.writeTreeState(ctx, 100, newRoot)
	require.NoError(t, err)

	// Read back
	size, root, err = coord.readTreeState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(100), size)
	require.Equal(t, newRoot, root)
}

func TestCoordinator_Locking(t *testing.T) {
	stateStore := newMockStateStore()
	coord, err := newCoordinator(stateStore, "did:key:test")
	require.NoError(t, err)

	ctx := context.Background()

	// Acquire lock
	unlock, err := coord.lock(ctx, "test-operation")
	require.NoError(t, err)
	require.NotNil(t, unlock)

	// Release lock
	unlock()
}
