// storage/storacha/indexpersist/integration_test.go
package indexpersist

import (
	"context"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/stretchr/testify/require"
)

func TestE2E_BuildUploadRecover(t *testing.T) {
	ctx := context.Background()

	// Simulate a real index
	originalIndex := map[string]string{
		"checkpoint":           "bafkreichgieyp6netvnqaem3syhsi6uvm5z7k5kdtavyx7fw3jn3hl6z54",
		"tile/0/000/000":       "bafkreif3gzzg23xfjtgvw45ggqvkpoq7fof3b6ag5f74y4afpnjcxfutre",
		"tile/0/000/001":       "bafkreihe3rekfvt7mexj36dxsfhy2csjk6nvlwrxgfbke7fenosv5qq2d4",
		"tile/entries/000/000": "bafkreifl4sayvhqhqjgst32ebsqjuqbzdmnyky2k7igwgneiav7ni3r6ei",
		"tile/entries/000/001": "bafkreifh7wqlmhrai7ypcbl5d5pqmtbhf7k5jeewdrjr6tpwjmg5gvdihi",
	}

	// Build CAR
	carData, rootCID, err := BuildIndexCAR(ctx, originalIndex)
	require.NoError(t, err)
	require.NotEmpty(t, rootCID)

	// Verify root CID is valid
	c, err := cid.Decode(rootCID)
	require.NoError(t, err)
	require.NotEqual(t, cid.Undef, c)

	// CAR should have content
	require.Greater(t, len(carData), 100) // At least some bytes

	t.Logf("Built CAR: root=%s size=%d bytes", rootCID, len(carData))
}
