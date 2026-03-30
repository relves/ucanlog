package storacha

import (
	"fmt"
	"io"
)

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
