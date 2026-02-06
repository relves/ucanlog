package storacha

import (
	"github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
)

// ComputeCID computes a CID for the given data using SHA2-256.
// Returns the CID string, the multihash bytes, and any error.
//
// Uses CIDv1 with raw codec (0x55) which is standard for blob storage.
func ComputeCID(data []byte) (string, mh.Multihash, error) {
	// Compute SHA2-256 multihash
	hash, err := mh.Sum(data, mh.SHA2_256, -1)
	if err != nil {
		return "", nil, err
	}

	// Create CIDv1 with raw codec
	c := cid.NewCidV1(cid.Raw, hash)

	return c.String(), hash, nil
}

// MultihashFromCID extracts the multihash from a CID string.
func MultihashFromCID(cidStr string) (mh.Multihash, error) {
	c, err := cid.Decode(cidStr)
	if err != nil {
		return nil, err
	}
	return c.Hash(), nil
}
