package storacha

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeCID(t *testing.T) {
	data := []byte("hello world")

	cid, mhash, err := ComputeCID(data)
	require.NoError(t, err)
	require.NotEmpty(t, cid)
	require.NotEmpty(t, mhash)

	// CID should start with "bafy" (CIDv1 with dag-pb or raw)
	require.True(t, len(cid) > 4, "CID should be non-trivial length")

	// Same data should produce same CID
	cid2, _, err := ComputeCID(data)
	require.NoError(t, err)
	require.Equal(t, cid, cid2)

	// Different data should produce different CID
	cid3, _, err := ComputeCID([]byte("different data"))
	require.NoError(t, err)
	require.NotEqual(t, cid, cid3)
}

func TestComputeCID_EmptyData(t *testing.T) {
	cid, mhash, err := ComputeCID([]byte{})
	require.NoError(t, err)
	require.NotEmpty(t, cid)
	require.NotEmpty(t, mhash)
}

func TestComputeCID_LargeData(t *testing.T) {
	// 1MB of data
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	cid, _, err := ComputeCID(data)
	require.NoError(t, err)
	require.NotEmpty(t, cid)
}
