package storacha

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"maps"
	"sort"
	"strings"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/ipld/block"
)



// hybridDirEntry is like dirEntry but supports external CID links for finalized bundles.
type hybridDirEntry struct {
	isDir    bool
	content  []byte
	linkCID  cid.Cid // non-zero for external CID link (not stored locally)
	children map[string]*hybridDirEntry
}

// virtualNode is a minimal format.Node that exists only to provide an external CID
// for a directory link. Its data is not stored in the local blockstore.
type virtualNode struct {
	c cid.Cid
}

func (v *virtualNode) Cid() cid.Cid                                              { return v.c }
func (v *virtualNode) RawData() []byte                                            { return nil }
func (v *virtualNode) Resolve(path []string) (interface{}, []string, error)       { return nil, nil, fmt.Errorf("virtual") }
func (v *virtualNode) ResolveLink(path []string) (*format.Link, []string, error) { return nil, nil, fmt.Errorf("virtual") }
func (v *virtualNode) Copy() format.Node                                          { return &virtualNode{c: v.c} }
func (v *virtualNode) Links() []*format.Link                                      { return nil }
func (v *virtualNode) Stat() (*format.NodeStat, error)                            { return &format.NodeStat{}, nil }
func (v *virtualNode) Size() (uint64, error)                                      { return 0, nil }
func (v *virtualNode) Loggable() map[string]interface{}                           { return map[string]interface{}{"cid": v.c.String()} }
func (v *virtualNode) String() string                                             { return v.c.String() }
func (v *virtualNode) Tree(path string, depth int) []string                       { return nil }

// BuildHybridCAR builds a UnixFS CAR that embeds mutable state as raw data and
// references finalized bundles as CID links (without embedding their bytes).
// A _manifest.json is embedded that maps finalized paths to their CIDs, enabling
// cold-start reconstruction without re-fetching each bundle.
func BuildHybridCAR(ctx context.Context, embedded map[string][]byte, linked map[string]cid.Cid) ([]byte, cid.Cid, error) {
	// Clone embedded so we can add the manifest without mutating the caller's map.
	embedded = maps.Clone(embedded)

	// Embed manifest of finalized CIDs so cold start can restore the map.
	if len(linked) > 0 {
		manifest := make(map[string]string, len(linked))
		for path, c := range linked {
			manifest[path] = c.String()
		}
		manifestJSON, err := json.Marshal(manifest)
		if err != nil {
			return nil, cid.Undef, fmt.Errorf("marshal manifest: %w", err)
		}
		embedded["_manifest.json"] = manifestJSON
	}

	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, offline.Exchange(bs))
	dagService := merkledag.NewDAGService(bserv)

	rootNode, err := buildHybridDirectoryTree(ctx, dagService, embedded, linked)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("build hybrid tree: %w", err)
	}

	// collectLocalBlocks skips any CID whose block isn't in the local store
	// (i.e. linked external CIDs) — this is intentional.
	nodes, err := collectLocalBlocks(ctx, dagService, rootNode.Cid())
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("collect blocks: %w", err)
	}

	rootLink := cidlink.Link{Cid: rootNode.Cid()}
	blocks := nodesToBlocks(nodes)

	reader := car.Encode([]ipld.Link{rootLink}, blocks)
	defer reader.Close()

	carData, err := io.ReadAll(reader)
	if err != nil {
		return nil, cid.Undef, fmt.Errorf("read CAR: %w", err)
	}

	rootCID := cid.NewCidV1(rootNode.Cid().Type(), rootNode.Cid().Hash())
	return carData, rootCID, nil
}

func buildHybridDirectoryTree(ctx context.Context, dagService format.DAGService, embedded map[string][]byte, linked map[string]cid.Cid) (format.Node, error) {
	allPaths := make([]string, 0, len(embedded)+len(linked))
	for p := range embedded {
		allPaths = append(allPaths, p)
	}
	for p := range linked {
		allPaths = append(allPaths, p)
	}
	sort.Strings(allPaths)

	root := &hybridDirEntry{isDir: true, children: make(map[string]*hybridDirEntry)}
	for _, path := range allPaths {
		parts := strings.Split(path, "/")
		current := root
		for i, part := range parts {
			if i == len(parts)-1 {
				if c, ok := linked[path]; ok {
					current.children[part] = &hybridDirEntry{linkCID: c}
				} else {
					current.children[part] = &hybridDirEntry{
						content: append([]byte(nil), embedded[path]...),
					}
				}
				continue
			}
			if _, exists := current.children[part]; !exists {
				current.children[part] = &hybridDirEntry{
					isDir:    true,
					children: make(map[string]*hybridDirEntry),
				}
			}
			current = current.children[part]
		}
	}

	return buildHybridDirNode(ctx, dagService, root)
}

func buildHybridDirNode(ctx context.Context, dagService format.DAGService, entry *hybridDirEntry) (format.Node, error) {
	dir, err := ufsio.NewDirectory(dagService)
	if err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	childNames := make([]string, 0, len(entry.children))
	for name := range entry.children {
		childNames = append(childNames, name)
	}
	sort.Strings(childNames)

	for _, name := range childNames {
		child := entry.children[name]

		var childNode format.Node
		if child.isDir {
			node, err := buildHybridDirNode(ctx, dagService, child)
			if err != nil {
				return nil, fmt.Errorf("build subdir %s: %w", name, err)
			}
			childNode = node
		} else if child.linkCID != cid.Undef {
			// External CID link — use virtualNode so AddChild can extract the CID
			// for the directory entry without storing the block locally.
			childNode = &virtualNode{c: child.linkCID}
		} else {
			node := merkledag.NewRawNode(child.content)
			if err := dagService.Add(ctx, node); err != nil {
				return nil, fmt.Errorf("add file %s: %w", name, err)
			}
			childNode = node
		}

		if err := dir.AddChild(ctx, name, childNode); err != nil {
			return nil, fmt.Errorf("add child %s: %w", name, err)
		}
	}

	node, err := dir.GetNode()
	if err != nil {
		return nil, fmt.Errorf("get directory node: %w", err)
	}
	if err := dagService.Add(ctx, node); err != nil {
		return nil, fmt.Errorf("add directory: %w", err)
	}
	return node, nil
}

// ExtractBlockPositions extracts content block offsets/sizes from CAR data.
func ExtractBlockPositions(carData []byte) (map[cid.Cid]blobindex.Position, error) {
	_, blocks, err := car.Decode(bytes.NewReader(carData))
	if err != nil {
		return nil, fmt.Errorf("decode CAR: %w", err)
	}

	carReader := bytes.NewReader(carData)
	headerSize, err := readCARHeaderSize(carReader)
	if err != nil {
		return nil, fmt.Errorf("read CAR header: %w", err)
	}

	positions := make(map[cid.Cid]blobindex.Position)
	currentOffset := headerSize

	for blk, err := range blocks {
		if err != nil {
			return nil, fmt.Errorf("read CAR block: %w", err)
		}

		blockCID := blk.Link().(cidlink.Link).Cid
		blockData := blk.Bytes()

		cidBytes := blockCID.Bytes()
		blockHeaderLength := varintSize(len(cidBytes)+len(blockData)) + len(cidBytes)
		blockEncodedSize := uint64(blockHeaderLength + len(blockData))

		positions[blockCID] = blobindex.Position{
			Offset: currentOffset + uint64(blockHeaderLength),
			Length: uint64(len(blockData)),
		}
		currentOffset += blockEncodedSize
	}

	return positions, nil
}



func collectLocalBlocks(ctx context.Context, dagService format.DAGService, root cid.Cid) ([]format.Node, error) {
	var blocks []format.Node
	seen := make(map[cid.Cid]bool)

	var collect func(c cid.Cid) error
	collect = func(c cid.Cid) error {
		if seen[c] {
			return nil
		}
		seen[c] = true

		node, err := dagService.Get(ctx, c)
		if err != nil {
			return nil
		}
		blocks = append(blocks, node)

		for _, link := range node.Links() {
			if err := collect(link.Cid); err != nil {
				return err
			}
		}
		return nil
	}

	if err := collect(root); err != nil {
		return nil, err
	}
	return blocks, nil
}

func nodesToBlocks(nodes []format.Node) iter.Seq2[ipld.Block, error] {
	return func(yield func(ipld.Block, error) bool) {
		for _, node := range nodes {
			link := cidlink.Link{Cid: node.Cid()}
			blk := block.NewBlock(link, node.RawData())
			if !yield(blk, nil) {
				return
			}
		}
	}
}
