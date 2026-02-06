package storacha

import (
	"fmt"
	"io"
	"time"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-ucanto/core/ipld"
)

// UCAN capability definitions for Storacha
const BlobAddAbility = "space/blob/add"
const BlobAllocateAbility = "space/blob/allocate"
const BlobAcceptAbility = "blob/accept"
const W3SBlobAcceptAbility = "web3.storage/blob/accept"
const IndexAddAbility = "space/index/add"
const UploadAddAbility = "upload/add"
const ContentRetrieveAbility = "space/content/retrieve"
const BlobRemoveAbility = "space/blob/remove"

// ReceiptsEndpoint is the Storacha receipts API endpoint for polling task status
const ReceiptsEndpoint = "https://up.storacha.network/receipt"

// PollInterval is the time to wait between receipt poll requests
const PollInterval = time.Second

// PollRetries is the maximum number of poll attempts
const PollRetries = 10

// Blob represents a blob with digest and size
type Blob struct {
	Digest mh.Multihash
	Size   uint64
}

// AddCaveats represents the caveats for space/blob/add
type AddCaveats struct {
	Blob Blob
}

// ToIPLD implements the CaveatBuilder interface
func (c AddCaveats) ToIPLD() (datamodel.Node, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, _ := nb.BeginMap(1)
	ma.AssembleKey().AssignString("blob")

	// Create blob map
	bb := basicnode.Prototype.Map.NewBuilder()
	ba, _ := bb.BeginMap(2)
	ba.AssembleKey().AssignString("digest")
	ba.AssembleValue().AssignBytes([]byte(c.Blob.Digest))
	ba.AssembleKey().AssignString("size")
	ba.AssembleValue().AssignInt(int64(c.Blob.Size))
	ba.Finish()
	blobNode := bb.Build()

	ma.AssembleValue().AssignNode(blobNode)
	ma.Finish()
	return nb.Build(), nil
}

// RemoveCaveats represents the caveats for space/blob/remove
type RemoveCaveats struct {
	Digest mh.Multihash
}

// ToIPLD implements the CaveatBuilder interface
func (c RemoveCaveats) ToIPLD() (datamodel.Node, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, _ := nb.BeginMap(1)
	ma.AssembleKey().AssignString("digest")
	ma.AssembleValue().AssignBytes([]byte(c.Digest))
	ma.Finish()
	return nb.Build(), nil
}

// AllocateAddress contains the presigned URL and headers for uploading
type AllocateAddress struct {
	URL     string
	Headers map[string]string
	Expires int64
}

// parseAllocateAddress extracts the address from a blob/allocate receipt
func parseAllocateAddress(addressNode ipld.Node) (*AllocateAddress, error) {
	if addressNode.Kind() != datamodel.Kind_Map {
		return nil, fmt.Errorf("address is not a map")
	}

	addr := &AllocateAddress{
		Headers: make(map[string]string),
	}

	// Get URL
	urlNode, err := addressNode.LookupByString("url")
	if err != nil {
		return nil, fmt.Errorf("address missing url: %w", err)
	}
	addr.URL, err = urlNode.AsString()
	if err != nil {
		return nil, fmt.Errorf("url is not a string: %w", err)
	}

	// Get headers (optional)
	headersNode, err := addressNode.LookupByString("headers")
	if err == nil && headersNode.Kind() == datamodel.Kind_Map {
		iter := headersNode.MapIterator()
		for !iter.Done() {
			k, v, err := iter.Next()
			if err != nil {
				break
			}
			kStr, err := k.AsString()
			if err != nil {
				continue
			}
			vStr, err := v.AsString()
			if err != nil {
				continue
			}
			addr.Headers[kStr] = vStr
		}
	}

	// Get expires (optional)
	expiresNode, err := addressNode.LookupByString("expires")
	if err == nil {
		addr.Expires, _ = expiresNode.AsInt()
	}

	return addr, nil
}

// OkBuilder is a simple success result builder
type OkBuilder struct{}

func (o OkBuilder) ToIPLD() (datamodel.Node, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, _ := nb.BeginMap(0)
	ma.Finish()
	return nb.Build(), nil
}

// ErrBuilder is a simple error result builder
type ErrBuilder struct {
	Message string
}

func (e ErrBuilder) ToIPLD() (datamodel.Node, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, _ := nb.BeginMap(1)
	ma.AssembleKey().AssignString("message")
	ma.AssembleValue().AssignString(e.Message)
	ma.Finish()
	return nb.Build(), nil
}

// ConcludeCaveats represents the caveats for ucan/conclude
type ConcludeCaveats struct {
	Receipt ipld.Link
}

// ToIPLD implements the CaveatBuilder interface
func (c ConcludeCaveats) ToIPLD() (datamodel.Node, error) {
	nb := basicnode.Prototype.Map.NewBuilder()
	ma, _ := nb.BeginMap(1)
	ma.AssembleKey().AssignString("receipt")
	ma.AssembleValue().AssignLink(c.Receipt)
	ma.Finish()
	return nb.Build(), nil
}

// extractSignerArchive tries to extract a signer from an IPLD node
// The archive format is: { id: "did:key:...", keys: { "EdDSA": privateKeyBytes } }
func extractSignerArchive(node ipld.Node) []byte {
	if node.Kind() != datamodel.Kind_Map {
		return nil
	}

	// Look for the 'keys' field which contains { algorithm: keyBytes }
	keysNode, err := node.LookupByString("keys")
	if err != nil {
		return nil
	}

	if keysNode.Kind() != datamodel.Kind_Map {
		return nil
	}

	// Iterate to find the key bytes (usually under "EdDSA" or similar key)
	iter := keysNode.MapIterator()
	for !iter.Done() {
		_, v, err := iter.Next()
		if err != nil {
			break
		}

		if v.Kind() == datamodel.Kind_Bytes {
			keyBytes, _ := v.AsBytes()
			return keyBytes
		}
	}

	return nil
}

// readCARHeaderSize reads a CAR file and returns the header size.
func readCARHeaderSize(r io.Reader) (uint64, error) {
	// Read the varint header length
	headerLen, bytesRead, err := readVarint(r)
	if err != nil {
		return 0, fmt.Errorf("failed to read header length: %w", err)
	}

	// Skip the header data
	headerData := make([]byte, headerLen)
	if _, err := io.ReadFull(r, headerData); err != nil {
		return 0, fmt.Errorf("failed to read header data: %w", err)
	}

	return uint64(bytesRead) + uint64(headerLen), nil
}

// readVarint reads a varint from a reader.
func readVarint(r io.Reader) (uint64, int, error) {
	var x uint64
	var s uint
	bytesRead := 0
	buf := make([]byte, 1)

	for i := 0; i < 10; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, bytesRead, err
		}
		bytesRead++
		b := buf[0]
		if b < 0x80 {
			return x | uint64(b)<<s, bytesRead, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
	return 0, bytesRead, fmt.Errorf("varint too long")
}

// varintSize returns the number of bytes needed to encode n as a varint.
func varintSize(n int) int {
	size := 0
	for n >= 0x80 {
		size++
		n >>= 7
	}
	return size + 1
}

// TileID identifies a tile by level and index within a log.
// Copied from tessera/storage/internal since it's not exported.
type TileID struct {
	Level uint64
	Index uint64
}

// SequencedEntry represents an entry with its bundle data and leaf hash.
// Copied from tessera/storage/internal since it's not exported.
type SequencedEntry struct {
	BundleData []byte
	LeafHash   []byte
}
