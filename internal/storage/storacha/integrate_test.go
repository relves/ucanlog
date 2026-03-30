package storacha

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api/layout"
)

func TestIntegrate_EmptyTree(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

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
	_, err = objStore.getObject(ctx, tPath)
	require.NoError(t, err, "tile should be stored at %s", tPath)
}

func TestIntegrate_ExtendTree(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

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
	_, err = objStore.getObject(ctx, tPath)
	require.NoError(t, err, "tile should be stored at %s", tPath)
}

func TestIntegrate_FullTile(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	leafHashes := make([][]byte, 256)
	for i := 0; i < 256; i++ {
		leafHashes[i] = rfc6962.DefaultHasher.HashLeaf([]byte{byte(i)})
	}

	root, err := integrate(ctx, 0, leafHashes, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root)

	tPath := layout.TilePath(0, 0, 0)
	_, err = objStore.getObject(ctx, tPath)
	require.NoError(t, err, "full tile should be stored at %s", tPath)
}

func TestIntegrate_MultipleTiles(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	leafHashes := make([][]byte, 260)
	for i := 0; i < 260; i++ {
		leafHashes[i] = rfc6962.DefaultHasher.HashLeaf([]byte{byte(i % 256), byte(i / 256)})
	}

	root, err := integrate(ctx, 0, leafHashes, lrs, slog.Default())
	require.NoError(t, err)
	require.NotEmpty(t, root)

	_, err = objStore.getObject(ctx, layout.TilePath(0, 0, 0))
	require.NoError(t, err, "first tile should be full")

	_, err = objStore.getObject(ctx, layout.TilePath(0, 1, 4))
	require.NoError(t, err, "second tile should be partial with 4 entries")

	_, err = objStore.getObject(ctx, layout.TilePath(1, 0, 1))
	require.NoError(t, err, "level 1 tile should exist")
}

func TestIntegrate_NoEntries(t *testing.T) {
	objStore := newObjStore(slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := context.Background()

	root, err := integrate(ctx, 0, [][]byte{}, lrs, slog.Default())
	require.NoError(t, err)

	require.Equal(t, rfc6962.DefaultHasher.EmptyRoot(), root)
}
