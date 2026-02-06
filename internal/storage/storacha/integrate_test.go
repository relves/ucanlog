package storacha

import (
	"context"
	"log/slog"
	"testing"

	"github.com/relves/ucanlog/internal/storage/storacha/storachatest"
	"github.com/stretchr/testify/require"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
)

func TestIntegrate_EmptyTree(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	leafHashes := [][]byte{
		rfc6962.DefaultHasher.HashLeaf([]byte("entry 0")),
		rfc6962.DefaultHasher.HashLeaf([]byte("entry 1")),
		rfc6962.DefaultHasher.HashLeaf([]byte("entry 2")),
	}

	newRoot, err := integrate(ctx, 0, leafHashes, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, newRoot)
	require.Len(t, newRoot, 32)

	tPath := layout.TilePath(0, 0, 3)
	_, ok := index.Get(tPath)
	require.True(t, ok, "tile should be stored at %s", tPath)
}

func TestIntegrate_ExtendTree(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	leafHashes1 := [][]byte{
		rfc6962.DefaultHasher.HashLeaf([]byte("entry 0")),
		rfc6962.DefaultHasher.HashLeaf([]byte("entry 1")),
	}
	root1, err := integrate(ctx, 0, leafHashes1, lrs, slog.Default())
	require.NoError(t, err)

	leafHashes2 := [][]byte{
		rfc6962.DefaultHasher.HashLeaf([]byte("entry 2")),
		rfc6962.DefaultHasher.HashLeaf([]byte("entry 3")),
	}
	root2, err := integrate(ctx, 2, leafHashes2, lrs, slog.Default())
	require.NoError(t, err)

	require.NotEqual(t, root1, root2)

	tPath := layout.TilePath(0, 0, 4)
	_, ok := index.Get(tPath)
	require.True(t, ok, "tile should be stored at %s", tPath)
}

func TestIntegrate_FullTile(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	leafHashes := make([][]byte, 256)
	for i := 0; i < 256; i++ {
		leafHashes[i] = rfc6962.DefaultHasher.HashLeaf([]byte{byte(i)})
	}

	root, err := integrate(ctx, 0, leafHashes, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root)

	tPath := layout.TilePath(0, 0, 0)
	_, ok := index.Get(tPath)
	require.True(t, ok, "full tile should be stored at %s", tPath)
}

func TestIntegrate_MultipleTiles(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	leafHashes := make([][]byte, 260)
	for i := 0; i < 260; i++ {
		leafHashes[i] = rfc6962.DefaultHasher.HashLeaf([]byte{byte(i % 256), byte(i / 256)})
	}

	root, err := integrate(ctx, 0, leafHashes, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root)

	_, ok := index.Get(layout.TilePath(0, 0, 0))
	require.True(t, ok, "first tile should be full")

	_, ok = index.Get(layout.TilePath(0, 1, 4))
	require.True(t, ok, "second tile should be partial with 4 entries")

	_, ok = index.Get(layout.TilePath(1, 0, 1))
	require.True(t, ok, "level 1 tile should exist")
}

func TestIntegrate_NoEntries(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	root, err := integrate(ctx, 0, [][]byte{}, lrs, slog.Default())
	require.NoError(t, err)

	require.Equal(t, rfc6962.DefaultHasher.EmptyRoot(), root)
}
