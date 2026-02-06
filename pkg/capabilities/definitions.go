// Package capabilities defines the public definitions for tlog UCAN capabilities.
package capabilities

import (
	ipldprime "github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	ipldschema "github.com/ipld/go-ipld-prime/schema"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/schema"
	"github.com/storacha/go-ucanto/validator"
)

// ToIPLD converts CreateCaveats to an IPLD node
func (c CreateCaveats) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(1)
	ma.AssembleKey().AssignString("delegation")
	ma.AssembleValue().AssignString(c.Delegation)
	ma.Finish()
	return nb.Build(), nil
}

func createCaveatsType() ipldschema.Type {
	ts, err := ipldprime.LoadSchemaBytes([]byte(`
		type CreateCaveats struct {
			delegation String
		}
	`))
	if err != nil {
		panic(err)
	}
	return ts.TypeByName("CreateCaveats")
}

// ToIPLD converts CreateSuccess to an IPLD node
func (s CreateSuccess) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(3)
	ma.AssembleKey().AssignString("logId")
	ma.AssembleValue().AssignString(s.LogID)
	ma.AssembleKey().AssignString("index_cid")
	ma.AssembleValue().AssignString(s.IndexCID)
	ma.AssembleKey().AssignString("tree_size")
	ma.AssembleValue().AssignInt(int64(s.TreeSize))
	ma.Finish()
	return nb.Build(), nil
}

func (f CreateFailure) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	ma.AssembleKey().AssignString("name")
	ma.AssembleValue().AssignString(f.name)
	ma.AssembleKey().AssignString("message")
	ma.AssembleValue().AssignString(f.message)
	ma.Finish()
	return nb.Build(), nil
}

// ToIPLD converts AppendCaveats to an IPLD node
func (c AppendCaveats) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	fieldCount := 2 // data and delegation are required
	if c.IndexCID != nil && *c.IndexCID != "" {
		fieldCount++
	}
	ma, _ := nb.BeginMap(int64(fieldCount))
	ma.AssembleKey().AssignString("data")
	ma.AssembleValue().AssignString(c.Data)
	if c.IndexCID != nil && *c.IndexCID != "" {
		ma.AssembleKey().AssignString("index_cid")
		ma.AssembleValue().AssignString(*c.IndexCID)
	}
	ma.AssembleKey().AssignString("delegation")
	ma.AssembleValue().AssignString(c.Delegation)
	ma.Finish()
	return nb.Build(), nil
}

func appendCaveatsType() ipldschema.Type {
	ts, err := ipldprime.LoadSchemaBytes([]byte(`
		type AppendCaveats struct {
			data String
			index_cid optional String
			delegation String
		}
	`))
	if err != nil {
		panic(err)
	}
	return ts.TypeByName("AppendCaveats")
}

// ToIPLD converts AppendSuccess to an IPLD node
func (s AppendSuccess) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(3)
	ma.AssembleKey().AssignString("index")
	ma.AssembleValue().AssignInt(s.Index)
	ma.AssembleKey().AssignString("new_index_cid")
	ma.AssembleValue().AssignString(s.NewIndexCID)
	ma.AssembleKey().AssignString("tree_size")
	ma.AssembleValue().AssignInt(int64(s.TreeSize))
	ma.Finish()
	return nb.Build(), nil
}

func (f AppendFailure) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	ma.AssembleKey().AssignString("name")
	ma.AssembleValue().AssignString(f.name)
	ma.AssembleKey().AssignString("message")
	ma.AssembleValue().AssignString(f.message)
	ma.Finish()
	return nb.Build(), nil
}

// ToIPLD converts ReadCaveats to an IPLD node
func (c ReadCaveats) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	fieldCount := 0
	if c.Offset != nil {
		fieldCount++
	}
	if c.Limit != nil {
		fieldCount++
	}
	ma, _ := nb.BeginMap(int64(fieldCount))
	if c.Offset != nil {
		ma.AssembleKey().AssignString("offset")
		ma.AssembleValue().AssignInt(*c.Offset)
	}
	if c.Limit != nil {
		ma.AssembleKey().AssignString("limit")
		ma.AssembleValue().AssignInt(*c.Limit)
	}
	ma.Finish()
	return nb.Build(), nil
}

func readCaveatsType() ipldschema.Type {
	ts, err := ipldprime.LoadSchemaBytes([]byte(`
		type ReadCaveats struct {
			offset optional Int
			limit optional Int
		}
	`))
	if err != nil {
		panic(err)
	}
	return ts.TypeByName("ReadCaveats")
}

// ToIPLD converts ReadSuccess to an IPLD node
func (s ReadSuccess) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)

	ma.AssembleKey().AssignString("entries")
	la, _ := ma.AssembleValue().BeginList(int64(len(s.Entries)))
	for _, entry := range s.Entries {
		la.AssembleValue().AssignString(entry)
	}
	la.Finish()

	ma.AssembleKey().AssignString("total")
	ma.AssembleValue().AssignInt(s.Total)

	ma.Finish()
	return nb.Build(), nil
}

func (f ReadFailure) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	ma.AssembleKey().AssignString("name")
	ma.AssembleValue().AssignString(f.name)
	ma.AssembleKey().AssignString("message")
	ma.AssembleValue().AssignString(f.message)
	ma.Finish()
	return nb.Build(), nil
}

// ToIPLD converts RevokeCaveats to an IPLD node
func (c RevokeCaveats) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	ma.AssembleKey().AssignString("cid")
	ma.AssembleValue().AssignString(c.Cid)
	ma.AssembleKey().AssignString("delegation")
	ma.AssembleValue().AssignString(c.Delegation)
	ma.Finish()
	return nb.Build(), nil
}

func revokeCaveatsType() ipldschema.Type {
	ts, err := ipldprime.LoadSchemaBytes([]byte(`
		type RevokeCaveats struct {
			cid String
			delegation String
		}
	`))
	if err != nil {
		panic(err)
	}
	return ts.TypeByName("RevokeCaveats")
}

// ToIPLD converts RevokeSuccess to an IPLD node
func (s RevokeSuccess) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(1)
	ma.AssembleKey().AssignString("revoked")
	ma.AssembleValue().AssignBool(s.Revoked)
	ma.Finish()
	return nb.Build(), nil
}

func (f RevokeFailure) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	ma.AssembleKey().AssignString("name")
	ma.AssembleValue().AssignString(f.name)
	ma.AssembleKey().AssignString("message")
	ma.AssembleValue().AssignString(f.message)
	ma.Finish()
	return nb.Build(), nil
}

// ToIPLD converts GarbageCaveats to an IPLD node
func (c GarbageCaveats) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	ma.AssembleKey().AssignString("logId")
	ma.AssembleValue().AssignString(c.LogID)
	ma.AssembleKey().AssignString("delegation")
	ma.AssembleValue().AssignString(c.Delegation)
	ma.Finish()
	return nb.Build(), nil
}

func garbageCaveatsType() ipldschema.Type {
	ts, err := ipldprime.LoadSchemaBytes([]byte(`
		type GarbageCaveats struct {
			logId String
			delegation String
		}
	`))
	if err != nil {
		panic(err)
	}
	return ts.TypeByName("GarbageCaveats")
}

// ToIPLD converts GarbageSuccess to an IPLD node
func (s GarbageSuccess) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(4)
	ma.AssembleKey().AssignString("bundlesProcessed")
	ma.AssembleValue().AssignInt(int64(s.BundlesProcessed))
	ma.AssembleKey().AssignString("blobsRemoved")
	ma.AssembleValue().AssignInt(int64(s.BlobsRemoved))
	ma.AssembleKey().AssignString("bytesFreed")
	ma.AssembleValue().AssignInt(int64(s.BytesFreed))
	ma.AssembleKey().AssignString("newGCPosition")
	ma.AssembleValue().AssignInt(int64(s.NewGCPosition))
	ma.Finish()
	return nb.Build(), nil
}

func (f GarbageFailure) ToIPLD() (ipld.Node, error) {
	np := basicnode.Prototype.Any
	nb := np.NewBuilder()
	ma, _ := nb.BeginMap(2)
	ma.AssembleKey().AssignString("name")
	ma.AssembleValue().AssignString(f.name)
	ma.AssembleKey().AssignString("message")
	ma.AssembleValue().AssignString(f.message)
	ma.Finish()
	return nb.Build(), nil
}

// Capability parsers
var (
	// TlogCreate is the capability parser for tlog/create
	TlogCreate = validator.NewCapability(
		AbilityCreate,
		schema.DIDString(),
		schema.Struct[CreateCaveats](createCaveatsType(), nil),
		nil,
	)

	// TlogAppend is the capability parser for tlog/append
	TlogAppend = validator.NewCapability(
		AbilityAppend,
		schema.DIDString(),
		schema.Struct[AppendCaveats](appendCaveatsType(), nil),
		nil,
	)

	// TlogRead is the capability parser for tlog/read
	TlogRead = validator.NewCapability(
		AbilityRead,
		schema.DIDString(),
		schema.Struct[ReadCaveats](readCaveatsType(), nil),
		nil,
	)

	// TlogRevoke is the capability parser for tlog/revoke
	TlogRevoke = validator.NewCapability(
		AbilityRevoke,
		schema.DIDString(),
		schema.Struct[RevokeCaveats](revokeCaveatsType(), nil),
		nil,
	)

	// TlogGarbage is the capability parser for tlog/gc
	TlogGarbage = validator.NewCapability(
		AbilityGarbage,
		schema.DIDString(),
		schema.Struct[GarbageCaveats](garbageCaveatsType(), nil),
		nil,
	)
)
