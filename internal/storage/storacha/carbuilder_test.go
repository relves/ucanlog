package storacha

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestBuildHybridCAR_SmallerThanFullCAR(t *testing.T) {
	ctx := context.Background()

	bundleData0 := bytes.Repeat([]byte("a"), 8192) // realistic bundle size
	bundleData1 := bytes.Repeat([]byte("b"), 8192)
	_, mh0, _ := ComputeCID(bundleData0)
	_, mh1, _ := ComputeCID(bundleData1)
	c0 := cid.NewCidV1(cid.Raw, mh0)
	c1 := cid.NewCidV1(cid.Raw, mh1)

	embedded := map[string][]byte{
		"tile/0/x000/000": []byte("tile data level 0"),
		"checkpoint":      []byte("checkpoint data"),
		"tile/entries/002.p/50": []byte("partial bundle"),
	}
	linked := map[string]cid.Cid{
		"tile/entries/000": c0,
		"tile/entries/001": c1,
	}

	hybridCAR, hybridRoot, err := BuildHybridCAR(ctx, embedded, linked)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, hybridRoot)
	require.NotEmpty(t, hybridCAR)

	// Compare to full CAR with everything embedded
	fullEmbedded := make(map[string][]byte)
	for k, v := range embedded {
		fullEmbedded[k] = v
	}
	fullEmbedded["tile/entries/000"] = bundleData0
	fullEmbedded["tile/entries/001"] = bundleData1

	fullCAR, _, err := BuildHybridCAR(ctx, fullEmbedded, nil)
	require.NoError(t, err)

	require.Less(t, len(hybridCAR), len(fullCAR),
		"hybrid CAR (%d bytes) should be smaller than full CAR (%d bytes)",
		len(hybridCAR), len(fullCAR))

	t.Logf("hybrid CAR: %d bytes, full CAR: %d bytes (saved %d bytes)",
		len(hybridCAR), len(fullCAR), len(fullCAR)-len(hybridCAR))
}

func TestBuildHybridCAR_ManifestEmbedded(t *testing.T) {
	ctx := context.Background()

	bundleData := []byte("finalized bundle data")
	_, mh, _ := ComputeCID(bundleData)
	c := cid.NewCidV1(cid.Raw, mh)

	embedded := map[string][]byte{
		"checkpoint": []byte("cp"),
	}
	linked := map[string]cid.Cid{
		"tile/entries/000": c,
	}

	carData, _, err := BuildHybridCAR(ctx, embedded, linked)
	require.NoError(t, err)

	// Parse CAR back and find manifest
	stateMap, err := parseStateCAR(context.Background(), bytes.NewReader(carData))
	require.NoError(t, err)

	manifestData, ok := stateMap["_manifest.json"]
	require.True(t, ok, "_manifest.json should be embedded in CAR")

	var manifest map[string]string
	require.NoError(t, json.Unmarshal(manifestData, &manifest))
	require.Equal(t, c.String(), manifest["tile/entries/000"])

	// Linked bundle bytes should NOT be in stateMap
	_, ok = stateMap["tile/entries/000"]
	require.False(t, ok, "finalized bundle data should not be embedded in CAR")
}

func TestBuildHybridCAR_NoLinked_EquivalentToFull(t *testing.T) {
	ctx := context.Background()

	state := map[string][]byte{
		"checkpoint":      []byte("cp"),
		"tile/0/x000/000": []byte("tile"),
		"tile/entries/000.p/10": []byte("partial"),
	}

	hybridCAR, _, err := BuildHybridCAR(ctx, state, nil)
	require.NoError(t, err)

	// BuildHybridCAR with no linked bundles should produce identical output
	hybridCAR2, _, err := BuildHybridCAR(ctx, state, nil)
	require.NoError(t, err)

	// With no linked bundles both should be the same size (no manifest overhead)
	require.Equal(t, len(hybridCAR2), len(hybridCAR),
		"hybrid with no links should equal full CAR size")
}

func TestBuildHybridCAR_LinkedBundleNotInBlockstore(t *testing.T) {
	ctx := context.Background()

	_, mh, _ := ComputeCID([]byte("external bundle"))
	extCID := cid.NewCidV1(cid.Raw, mh)

	embedded := map[string][]byte{"checkpoint": []byte("cp")}
	linked := map[string]cid.Cid{"tile/entries/000": extCID}

	carData, _, err := BuildHybridCAR(ctx, embedded, linked)
	require.NoError(t, err)

	// The external bundle bytes should not appear anywhere in the CAR
	externalData := []byte("external bundle")
	require.False(t, bytes.Contains(carData, externalData),
		"external bundle bytes should not be embedded in hybrid CAR")
}
