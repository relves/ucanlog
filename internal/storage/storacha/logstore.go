package storacha

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/transparency-dev/tessera/api"
	"github.com/transparency-dev/tessera/api/layout"
	"golang.org/x/sync/errgroup"
)

type logResourceStore struct {
	objStore    *objStore
	entriesPath func(uint64, uint8) string
}

func newLogResourceStore(objStore *objStore, entriesPath func(uint64, uint8) string) *logResourceStore {
	if entriesPath == nil {
		entriesPath = layout.EntriesPath
	}
	return &logResourceStore{
		objStore:    objStore,
		entriesPath: entriesPath,
	}
}

func (lrs *logResourceStore) setTile(ctx context.Context, level, index uint64, partial uint8, tile *api.HashTile) error {
	data, err := tile.MarshalText()
	if err != nil {
		return fmt.Errorf("failed to marshal tile: %w", err)
	}

	tPath := layout.TilePath(level, index, partial)
	if err := lrs.objStore.setObject(ctx, tPath, data); err != nil {
		return fmt.Errorf("failed to store tile at %s: %w", tPath, err)
	}

	return nil
}

func (lrs *logResourceStore) getTile(ctx context.Context, level, index uint64, partial uint8) (*api.HashTile, error) {
	tPath := layout.TilePath(level, index, partial)
	data, err := lrs.objStore.getObject(ctx, tPath)
	if err != nil {
		return nil, err
	}

	tile := &api.HashTile{}
	if err := tile.UnmarshalText(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tile: %w", err)
	}

	return tile, nil
}

func (lrs *logResourceStore) getTiles(ctx context.Context, tileIDs []TileID, treeSize uint64) ([]*api.HashTile, error) {
	result := make([]*api.HashTile, len(tileIDs))
	errG := errgroup.Group{}

	for i, id := range tileIDs {
		i, id := i, id
		errG.Go(func() error {
			partial := layout.PartialTileSize(id.Level, id.Index, treeSize)
			tPath := layout.TilePath(id.Level, id.Index, partial)

			data, err := lrs.objStore.getObject(ctx, tPath)
			if err != nil {
				if isNotFoundError(err) {
					return nil
				}
				return err
			}

			tile := &api.HashTile{}
			if err := tile.UnmarshalText(data); err != nil {
				return fmt.Errorf("failed to unmarshal tile %s: %w", tPath, err)
			}
			result[i] = tile
			return nil
		})
	}

	if err := errG.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

func (lrs *logResourceStore) setEntryBundle(ctx context.Context, bundleIndex uint64, partial uint8, bundleData []byte) error {
	objPath := lrs.entriesPath(bundleIndex, partial)

	_, err := lrs.objStore.setObjectIfNoneMatch(ctx, objPath, bundleData)
	if err != nil {
		return fmt.Errorf("failed to store entry bundle at %s: %w", objPath, err)
	}

	return nil
}

func (lrs *logResourceStore) getEntryBundle(ctx context.Context, bundleIndex uint64, partial uint8) ([]byte, error) {
	objPath := lrs.entriesPath(bundleIndex, partial)

	data, err := lrs.objStore.getObject(ctx, objPath)
	if err != nil {
		if isNotFoundError(err) {
			return nil, fmt.Errorf("%s: %w", objPath, os.ErrNotExist)
		}
		return nil, err
	}

	return data, nil
}

func (lrs *logResourceStore) setCheckpoint(ctx context.Context, cpRaw []byte) error {
	return lrs.objStore.setObject(ctx, layout.CheckpointPath, cpRaw)
}

func (lrs *logResourceStore) getCheckpoint(ctx context.Context) ([]byte, error) {
	data, err := lrs.objStore.getObject(ctx, layout.CheckpointPath)
	if err != nil {
		if isNotFoundError(err) {
			return nil, os.ErrNotExist
		}
		return nil, err
	}
	return data, nil
}

func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "path not found in index") ||
		os.IsNotExist(err)
}
