package storacha

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/transparency-dev/merkle/compact"
	"github.com/transparency-dev/merkle/rfc6962"
	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"golang.org/x/sync/errgroup"
)

func integrate(ctx context.Context, fromSeq uint64, leafHashes [][]byte, lrs *logResourceStore, logger *slog.Logger) ([]byte, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if len(leafHashes) == 0 {
		if fromSeq == 0 {
			return rfc6962.DefaultHasher.EmptyRoot(), nil
		}
		return nil, nil
	}

	getTiles := func(ctx context.Context, tileIDs []TileID, treeSize uint64) ([]*api.HashTile, error) {
		return lrs.getTiles(ctx, tileIDs, treeSize)
	}

	newSize, newRoot, dirtyTiles, err := integrateImpl(ctx, getTiles, fromSeq, leafHashes, logger)
	if err != nil {
		return nil, fmt.Errorf("integrate: %w", err)
	}

	logger.Debug("integrate writing dirty tiles", "count", len(dirtyTiles))
	writeStart := time.Now()
	errG := errgroup.Group{}
	for tileID, tile := range dirtyTiles {
		tileID, tile := tileID, tile
		errG.Go(func() error {
			partial := layout.PartialTileSize(tileID.Level, tileID.Index, newSize)
			return lrs.setTile(ctx, tileID.Level, tileID.Index, uint8(partial), tile)
		})
	}

	if err := errG.Wait(); err != nil {
		return nil, fmt.Errorf("failed to write tiles: %w", err)
	}
	logger.Debug("integrate writing tiles took", "duration", time.Since(writeStart))

	return newRoot, nil
}

func integrateImpl(ctx context.Context, getTiles func(ctx context.Context, tileIDs []TileID, treeSize uint64) ([]*api.HashTile, error), fromSize uint64, leafHashes [][]byte, logger *slog.Logger) (newSize uint64, rootHash []byte, tiles map[TileID]*api.HashTile, err error) {
	rf := &compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}

	logger.Debug("integrate starting", "fromSize", fromSize, "numLeaves", len(leafHashes))
	compactStart := time.Now()
	baseRange, err := newCompactRange(ctx, rf, fromSize, getTiles, logger)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("failed to create range covering existing log: %w", err)
	}
	logger.Debug("newCompactRange took", "duration", time.Since(compactStart))

	_, err = baseRange.GetRootHash(nil)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("invalid log state, unable to recalculate root: %w", err)
	}

	newRange := rf.NewEmptyRange(fromSize)
	tc := newTileWriteCache(fromSize, getTiles)
	visitor := tc.Visitor(ctx)
	for _, e := range leafHashes {
		if err := newRange.Append(e, visitor); err != nil {
			return 0, nil, nil, fmt.Errorf("newRange.Append(): %v", err)
		}
	}

	if err := tc.Err(); err != nil {
		return 0, nil, nil, err
	}

	if err := baseRange.AppendRange(newRange, nil); err != nil {
		return 0, nil, nil, fmt.Errorf("failed to merge new range onto existing log: %w", err)
	}

	if err := tc.Err(); err != nil {
		return 0, nil, nil, err
	}

	newRoot, err := baseRange.GetRootHash(nil)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("failed to calculate new root hash: %w", err)
	}

	return baseRange.End(), newRoot, tc.Tiles(), nil
}

func newCompactRange(ctx context.Context, rf *compact.RangeFactory, treeSize uint64, getTiles func(ctx context.Context, tileIDs []TileID, treeSize uint64) ([]*api.HashTile, error), logger *slog.Logger) (*compact.Range, error) {
	if treeSize == 0 {
		return rf.NewEmptyRange(0), nil
	}

	rangeNodes := compact.RangeNodes(0, treeSize, nil)
	toFetch := make(map[TileID]struct{})
	for _, id := range rangeNodes {
		tLevel, tIndex, _, _ := layout.NodeCoordsToTileAddress(uint64(id.Level), id.Index)
		toFetch[TileID{Level: tLevel, Index: tIndex}] = struct{}{}
	}

	tileIDs := make([]TileID, 0, len(toFetch))
	for id := range toFetch {
		tileIDs = append(tileIDs, id)
	}

	logger.Debug("newCompactRange fetching tiles", "count", len(tileIDs), "treeSize", treeSize)
	fetchStart := time.Now()
	tiles, err := getTiles(ctx, tileIDs, treeSize)
	if err != nil {
		return nil, err
	}
	logger.Debug("newCompactRange getTiles took", "duration", time.Since(fetchStart))

	tileMap := make(map[TileID]*api.HashTile)
	for i, id := range tileIDs {
		tileMap[id] = tiles[i]
	}

	hashes := make([][]byte, 0, len(rangeNodes))
	for _, id := range rangeNodes {
		tLevel, tIndex, nodeLevel, nodeIndex := layout.NodeCoordsToTileAddress(uint64(id.Level), id.Index)
		tileID := TileID{Level: tLevel, Index: tIndex}
		tile := tileMap[tileID]

		if tile == nil {
			return nil, fmt.Errorf("missing tile for node [%d/%d@%d]", id.Level, id.Index, treeSize)
		}

		var hash []byte
		if nodeLevel == 0 {
			// Leaf node - directly stored in tile
			if nodeIndex >= uint64(len(tile.Nodes)) {
				return nil, fmt.Errorf("missing leaf node [%d/%d@%d] in tile (tile has %d nodes)", id.Level, id.Index, treeSize, len(tile.Nodes))
			}
			hash = tile.Nodes[nodeIndex]
		} else {
			// Internal node - must be computed from leaves
			// This internal node covers leaves [firstLeaf, lastLeaf)
			numLeaves := 1 << nodeLevel
			firstLeaf := int(nodeIndex) * numLeaves
			lastLeaf := firstLeaf + numLeaves

			if lastLeaf > len(tile.Nodes) {
				return nil, fmt.Errorf("require leaf nodes [%d, %d) for internal node [%d/%d@%d] but tile only has %d leaves",
					firstLeaf, lastLeaf, id.Level, id.Index, treeSize, len(tile.Nodes))
			}

			// Compute the internal node hash from the leaf nodes
			r := rf.NewEmptyRange(0)
			for _, leaf := range tile.Nodes[firstLeaf:lastLeaf] {
				if err := r.Append(leaf, nil); err != nil {
					return nil, fmt.Errorf("failed to compute internal node [%d/%d]: %w", id.Level, id.Index, err)
				}
			}
			hash, err = r.GetRootHash(nil)
			if err != nil {
				return nil, fmt.Errorf("failed to get root for internal node [%d/%d]: %w", id.Level, id.Index, err)
			}
		}

		hashes = append(hashes, hash)
	}

	return rf.NewRange(0, treeSize, hashes)
}

type tileWriteCache struct {
	m        map[TileID]*populatedTile
	err      []error
	treeSize uint64
	getTile  func(ctx context.Context, tileID TileID, treeSize uint64) (*api.HashTile, error)
}

func newTileWriteCache(treeSize uint64, getTile func(ctx context.Context, tileIDs []TileID, treeSize uint64) ([]*api.HashTile, error)) *tileWriteCache {
	return &tileWriteCache{
		m:        make(map[TileID]*populatedTile),
		treeSize: treeSize,
		getTile: func(ctx context.Context, tileID TileID, treeSize uint64) (*api.HashTile, error) {
			tiles, err := getTile(ctx, []TileID{tileID}, treeSize)
			if err != nil {
				return nil, err
			}
			if len(tiles) == 0 {
				return nil, nil
			}
			return tiles[0], nil
		},
	}
}

func (tc *tileWriteCache) Err() error {
	if len(tc.err) == 0 {
		return nil
	}
	result := tc.err[0]
	for _, e := range tc.err[1:] {
		if e != nil {
			result = fmt.Errorf("%v; %w", result, e)
		}
	}
	return result
}

func minImpliedTreeSize(id TileID) uint64 {
	return (id.Index * layout.TileWidth) << (id.Level * 8)
}

func (tc *tileWriteCache) Visitor(ctx context.Context) compact.VisitFn {
	return func(id compact.NodeID, hash []byte) {
		tileLevel, tileIndex, nodeLevel, nodeIndex := layout.NodeCoordsToTileAddress(uint64(id.Level), uint64(id.Index))
		tileID := TileID{Level: tileLevel, Index: tileIndex}
		tile := tc.m[tileID]
		if tile == nil {
			if iSize := minImpliedTreeSize(tileID); iSize <= tc.treeSize {
				existingTile, err := tc.getTile(ctx, tileID, tc.treeSize)
				if err != nil {
					tc.err = append(tc.err, err)
					return
				}
				if existingTile != nil {
					tile = newPopulatedTile(existingTile)
				}
			}
			if tile == nil {
				tile = newPopulatedTile(nil)
			}
		}
		tc.m[tileID] = tile
		idx := compact.NodeID{Level: nodeLevel, Index: nodeIndex}
		tile.Set(idx, hash)
	}
}

func (tc *tileWriteCache) Tiles() map[TileID]*api.HashTile {
	newTiles := make(map[TileID]*api.HashTile)
	for k, t := range tc.m {
		newTiles[k] = &api.HashTile{Nodes: t.leaves}
	}
	return newTiles
}

type populatedTile struct {
	inner  map[compact.NodeID][]byte
	leaves [][]byte
}

func newPopulatedTile(h *api.HashTile) *populatedTile {
	ft := &populatedTile{
		inner:  make(map[compact.NodeID][]byte),
		leaves: make([][]byte, 0, layout.TileWidth),
	}

	if h != nil {
		r := (&compact.RangeFactory{Hash: rfc6962.DefaultHasher.HashChildren}).NewEmptyRange(0)
		for _, h := range h.Nodes {
			if err := r.Append(h, ft.Set); err != nil {
				continue
			}
		}
	}
	return ft
}

func (f *populatedTile) Set(id compact.NodeID, hash []byte) {
	if id.Level == 0 {
		if id.Index > 255 {
			panic(fmt.Sprintf("Weird node ID: %v", id))
		}
		if l, idx := uint64(len(f.leaves)), id.Index; idx >= l {
			f.leaves = append(f.leaves, make([][]byte, idx-l+1)...)
		}
		f.leaves[id.Index] = hash
	} else {
		f.inner[id] = hash
	}
}

func (f *populatedTile) Get(id compact.NodeID) []byte {
	if id.Level == 0 {
		if l := uint64(len(f.leaves)); id.Index >= l {
			return nil
		}
		return f.leaves[id.Index]
	}
	return f.inner[id]
}
