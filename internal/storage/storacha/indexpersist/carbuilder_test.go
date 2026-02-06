// storage/storacha/indexpersist/carbuilder_test.go
package indexpersist

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestBuildIndexCAR_SimpleStructure(t *testing.T) {
	ctx := context.Background()

	// Create a simple index with a few paths
	index := map[string]string{
		"checkpoint":           "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		"tile/0/000/000":       "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre",
		"tile/entries/000/000": "bafkreifl4sayvhqhqjgst32ebsqjuqbzdmnyky2k7igwgneiav7ni3r6ei",
	}

	carData, rootCID, err := BuildIndexCAR(ctx, index)
	require.NoError(t, err)
	require.NotEmpty(t, rootCID)

	// Verify the root CID is valid
	_, err = cid.Decode(rootCID)
	require.NoError(t, err)

	// Verify CAR was created
	require.Greater(t, len(carData), 0)
}

func TestBuildIndexCAR_EmptyIndex(t *testing.T) {
	ctx := context.Background()
	index := map[string]string{}

	carData, rootCID, err := BuildIndexCAR(ctx, index)
	require.NoError(t, err)
	require.NotEmpty(t, rootCID)
	require.Greater(t, len(carData), 0)
}

func TestBuildIndexCAR_DeepNesting(t *testing.T) {
	ctx := context.Background()

	// Test deep directory nesting like tile/entries/000/001
	index := map[string]string{
		"tile/entries/000/001": "bafkreifh7wqlmhrai7ypcbl5d5pqmtbhf7k5jeewdrjr6tpwjmg5gvdihi",
		"tile/entries/000/002": "bafkreihzeahasnhtzgfau235z66fou5kbvxeuqa2rhnxftgnqtriixifpi",
		"tile/entries/001/000": "bafkreigt3ua3ocscw3qjd2ys4wrnkwisy46yvowdvol4b7httkrv7q2ze4",
		"tile/0/000/000":       "bafkreihi7i4vqvcl2e45yt5oe33slxt73a4u32z3adeigfcrba5o4l3bae",
		"tile/0/000/001":       "bafkreieh2uz4y4hn2omtc4dsvhbjbjonhl5ycm5vmm5hyo5dreuyr5mxxy",
	}

	carData, rootCID, err := BuildIndexCAR(ctx, index)
	require.NoError(t, err)
	require.NotEmpty(t, rootCID)
	require.Greater(t, len(carData), 0)
}

func TestBuildIndexCAR_AfterGCCleanup(t *testing.T) {
	ctx := context.Background()

	// Use valid CIDs from the other tests
	cidCheckpoint := "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54"
	cidBundle := "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre"
	cidTile := "bafkreifl4sayvhqhqjgst32ebsqjuqbzdmnyky2k7igwgneiav7ni3r6ei"
	cidPartial1 := "bafkreihdwdcefgh4dqkjv67uzcmw7ojee6xedzdetojuzjevtenxquvyku"
	cidPartial2 := "bafkreifjjcie6lypi6ny7n7jh26dn7cxsnqvakxr6zzwxnqpzpxqq5qury"
	cidPartial3 := "bafkreig7h2mwcaasqxhspnpxqfuqhxqmhdseyeti3ksxjnckp5qbg5rhsy"

	// Simulate: index has complete bundle + partials, then GC removes partials
	indexBefore := map[string]string{
		"checkpoint":                 cidCheckpoint,
		"tile/entries/000/000":       cidBundle,   // complete bundle
		"tile/entries/000/000.p/128": cidPartial1, // partial (to be GC'd)
		"tile/entries/000/000.p/255": cidPartial2, // partial (to be GC'd)
		"tile/0/000/000":             cidTile,     // complete tile
		"tile/0/000/000.p/128":       cidPartial3, // partial (to be GC'd)
	}

	// Build CAR before GC - should include partial directories
	carBefore, rootBefore, err := BuildIndexCAR(ctx, indexBefore)
	require.NoError(t, err)
	require.NotEmpty(t, rootBefore)

	// Simulate GC cleanup - remove partial paths
	indexAfter := map[string]string{
		"checkpoint":           cidCheckpoint,
		"tile/entries/000/000": cidBundle,
		"tile/0/000/000":       cidTile,
	}

	// Build CAR after GC - should NOT include partial directories
	carAfter, rootAfter, err := BuildIndexCAR(ctx, indexAfter)
	require.NoError(t, err)
	require.NotEmpty(t, rootAfter)

	// The root CID should be different (fewer entries = different tree)
	require.NotEqual(t, rootBefore, rootAfter, "CAR root should differ after GC removes paths")

	// The CAR after should be smaller (fewer directory entries)
	require.Less(t, len(carAfter), len(carBefore), "CAR after GC should be smaller")
}
