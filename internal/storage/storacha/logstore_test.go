package storacha

import (
	"context"
	"log/slog"
	"testing"

	"github.com/relves/ucanlog/internal/storage/storacha/storachatest"
	"github.com/stretchr/testify/require"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
)

func TestLogResourceStore_SetAndGetTile(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	tile := &api.HashTile{
		Nodes: [][]byte{
			make([]byte, 32),
			make([]byte, 32),
			make([]byte, 32),
		},
	}
	copy(tile.Nodes[0], []byte("hash0000000000000000000000000001"))
	copy(tile.Nodes[1], []byte("hash0000000000000000000000000002"))
	copy(tile.Nodes[2], []byte("hash0000000000000000000000000003"))

	err := lrs.setTile(ctx, 0, 0, 3, tile)
	require.NoError(t, err)

	tPath := layout.TilePath(0, 0, 3)
	_, ok := index.Get(tPath)
	require.True(t, ok, "tile path should be in index")
}

func TestLogResourceStore_GetTiles(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	treeSize := uint64(3)

	tile0 := &api.HashTile{Nodes: [][]byte{make([]byte, 32)}}
	copy(tile0.Nodes[0], []byte("hash0000000000000000000000000001"))
	partial0 := layout.PartialTileSize(0, 0, treeSize)
	lrs.setTile(ctx, 0, 0, uint8(partial0), tile0)

	tile1 := &api.HashTile{Nodes: [][]byte{make([]byte, 32)}}
	copy(tile1.Nodes[0], []byte("hash0000000000000000000000000002"))
	partial1 := layout.PartialTileSize(0, 1, treeSize)
	lrs.setTile(ctx, 0, 1, uint8(partial1), tile1)

	tileIDs := []TileID{{Level: 0, Index: 0}, {Level: 0, Index: 1}, {Level: 0, Index: 999}}
	tiles, err := lrs.getTiles(ctx, tileIDs, treeSize)
	require.NoError(t, err)
	require.Len(t, tiles, 3)

	require.NotNil(t, tiles[0])
	require.NotNil(t, tiles[1])
	require.Nil(t, tiles[2])
}

func TestLogResourceStore_SetAndGetEntryBundle(t *testing.T) {
	client := NewMockClient()
	index := NewCIDIndex()

	objStore := newObjStore(newClientRef(client), index, "did:key:test", "https://w3s.link", slog.Default())
	lrs := newLogResourceStore(objStore, layout.EntriesPath)

	ctx := WithDelegation(context.Background(), storachatest.MockDelegation())

	bundleData := make([]byte, 0)
	entry1 := []byte("entry data 1")
	entry2 := []byte("entry data 2")

	bundleData = append(bundleData, byte(len(entry1)>>8), byte(len(entry1)))
	bundleData = append(bundleData, entry1...)
	bundleData = append(bundleData, byte(len(entry2)>>8), byte(len(entry2)))
	bundleData = append(bundleData, entry2...)

	err := lrs.setEntryBundle(ctx, 0, 2, bundleData)
	require.NoError(t, err)

	retrieved, err := lrs.getEntryBundle(ctx, 0, 2)
	require.NoError(t, err)
	require.Equal(t, bundleData, retrieved)
}
