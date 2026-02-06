// storage/storacha/indexpersist/carbuilder.go
package indexpersist

import (
	"context"
	"fmt"
	"io"
	"iter"
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
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/ipld/block"
)

// BuildIndexCAR builds a UnixFS CAR from a path->CID index map.
// The CAR contains directory nodes that link to the existing blob CIDs.
// Returns the CAR data and the root CID string.
func BuildIndexCAR(ctx context.Context, index map[string]string) ([]byte, string, error) {
	// Create in-memory blockstore
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bserv := blockservice.New(bs, offline.Exchange(bs))
	dagService := merkledag.NewDAGService(bserv)

	// Build directory tree
	rootNode, err := buildDirectoryTree(ctx, dagService, index)
	if err != nil {
		return nil, "", fmt.Errorf("build tree: %w", err)
	}

	// Collect all blocks for the CAR
	nodes, err := collectBlocks(ctx, dagService, rootNode.Cid())
	if err != nil {
		return nil, "", fmt.Errorf("collect blocks: %w", err)
	}

	// Convert to go-ucanto types and encode using car.Encode
	rootLink := cidlink.Link{Cid: rootNode.Cid()}
	blocks := nodesToBlocks(nodes)

	reader := car.Encode([]ipld.Link{rootLink}, blocks)
	defer reader.Close()

	carData, err := io.ReadAll(reader)
	if err != nil {
		return nil, "", fmt.Errorf("read CAR: %w", err)
	}

	return carData, rootNode.Cid().String(), nil
}

// nodesToBlocks converts format.Node slice to iter.Seq2[ipld.Block, error]
func nodesToBlocks(nodes []format.Node) iter.Seq2[ipld.Block, error] {
	return func(yield func(ipld.Block, error) bool) {
		for _, node := range nodes {
			// Skip proxy nodes - they reference external blobs that are already
			// stored elsewhere. The directory structure includes links to these
			// CIDs, but we don't need the actual blob data in this CAR.
			if _, ok := node.(*cidProxyNode); ok {
				continue
			}

			link := cidlink.Link{Cid: node.Cid()}
			blk := block.NewBlock(link, node.RawData())
			if !yield(blk, nil) {
				return
			}
		}
	}
}

// dirEntry represents a node in the tree structure
type dirEntry struct {
	isDir    bool
	blobCID  string               // if leaf (links to existing blob)
	content  []byte               // if leaf with actual content
	children map[string]*dirEntry // if directory
}

// buildDirectoryTree creates a UnixFS directory DAG from path->CID mappings.
func buildDirectoryTree(ctx context.Context, dagService format.DAGService, index map[string]string) (format.Node, error) {
	// Sort paths for deterministic output
	paths := make([]string, 0, len(index))
	for p := range index {
		paths = append(paths, p)
	}
	sort.Strings(paths)

	// Build tree structure
	root := &dirEntry{isDir: true, children: make(map[string]*dirEntry)}

	for _, path := range paths {
		blobCID := index[path]
		parts := strings.Split(path, "/")

		current := root
		for i, part := range parts {
			if i == len(parts)-1 {
				// Leaf node - links to existing blob CID
				current.children[part] = &dirEntry{
					isDir:   false,
					blobCID: blobCID,
				}
			} else {
				// Intermediate directory
				if _, exists := current.children[part]; !exists {
					current.children[part] = &dirEntry{
						isDir:    true,
						children: make(map[string]*dirEntry),
					}
				}
				current = current.children[part]
			}
		}
	}

	// Recursively build the DAG
	return buildDirNode(ctx, dagService, root)
}

// buildDirNode recursively builds a UnixFS directory node.
func buildDirNode(ctx context.Context, dagService format.DAGService, entry *dirEntry) (format.Node, error) {
	dir, err := ufsio.NewDirectory(dagService)
	if err != nil {
		return nil, fmt.Errorf("create directory: %w", err)
	}

	// Sort children for deterministic output
	childNames := make([]string, 0, len(entry.children))
	for name := range entry.children {
		childNames = append(childNames, name)
	}
	sort.Strings(childNames)

	for _, name := range childNames {
		child := entry.children[name]

		var childNode format.Node
		if child.isDir {
			node, err := buildDirNode(ctx, dagService, child)
			if err != nil {
				return nil, fmt.Errorf("build subdir %s: %w", name, err)
			}
			childNode = node
		} else if child.blobCID != "" {
			// Create a proxy node that links to the external blob CID.
			// This allows the CAR to reference external blobs while still
			// having the correct CID in directory entries.
			blobCid, err := cid.Decode(child.blobCID)
			if err != nil {
				return nil, fmt.Errorf("invalid CID for %s: %w", name, err)
			}
			childNode = &cidProxyNode{c: blobCid}
		} else {
			// Create a raw node with actual content (for testing)
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

// collectBlocks traverses the DAG and collects all blocks.
func collectBlocks(ctx context.Context, dagService format.DAGService, root cid.Cid) ([]format.Node, error) {
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
			// Assume external CID, skip
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

// cidProxyNode is a minimal node implementation that just holds a CID.
// Used to create directory links to existing blobs without their content.
// The CID returned by Cid() is the external blob CID, allowing the CAR
// to reference external content while maintaining correct CID links.
type cidProxyNode struct {
	c cid.Cid
}

func (n *cidProxyNode) Cid() cid.Cid                                         { return n.c }
func (n *cidProxyNode) RawData() []byte                                      { return nil }
func (n *cidProxyNode) String() string                                       { return n.c.String() }
func (n *cidProxyNode) Loggable() map[string]interface{}                     { return nil }
func (n *cidProxyNode) Resolve([]string) (interface{}, []string, error)      { return nil, nil, nil }
func (n *cidProxyNode) Tree(string, int) []string                            { return nil }
func (n *cidProxyNode) ResolveLink([]string) (*format.Link, []string, error) { return nil, nil, nil }
func (n *cidProxyNode) Copy() format.Node                                    { return &cidProxyNode{c: n.c} }
func (n *cidProxyNode) Links() []*format.Link                                { return nil }
func (n *cidProxyNode) Stat() (*format.NodeStat, error)                      { return &format.NodeStat{}, nil }
func (n *cidProxyNode) Size() (uint64, error)                                { return 0, nil }
