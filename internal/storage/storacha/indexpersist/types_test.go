// storage/storacha/indexpersist/types_test.go
package indexpersist

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestIndexMeta_JSON(t *testing.T) {
	meta := IndexMeta{
		RootCID:      "bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi",
		Version:      42,
		LastUploaded: time.Date(2026, 1, 21, 10, 30, 0, 0, time.UTC),
		EntryCount:   1500,
	}

	data, err := json.Marshal(meta)
	require.NoError(t, err)

	var decoded IndexMeta
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	require.Equal(t, meta.RootCID, decoded.RootCID)
	require.Equal(t, meta.Version, decoded.Version)
	require.Equal(t, meta.EntryCount, decoded.EntryCount)
	require.True(t, meta.LastUploaded.Equal(decoded.LastUploaded))
}

func TestConfig_Defaults(t *testing.T) {
	cfg := DefaultConfig()

	require.Equal(t, 30*time.Second, cfg.Interval)
	require.Equal(t, "index/", cfg.PathPrefix)
}
