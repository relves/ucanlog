package storacha

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/multiformats/go-multicodec"
	"github.com/storacha/go-libstoracha/blobindex"
	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	captypes "github.com/storacha/go-libstoracha/capabilities/types"
	uclient "github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	failuredatamodel "github.com/storacha/go-ucanto/core/result/failure/datamodel"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	serverdatamodel "github.com/storacha/go-ucanto/server/datamodel"
	ucantohttp "github.com/storacha/go-ucanto/transport/http"
	guppyclient "github.com/storacha/guppy/pkg/client"
)

// GuppyClient implements StorachaClient using the guppy library.
// This replaces the custom DelegatedClient with the official Storacha client.
type GuppyClient struct {
	client       *guppyclient.Client
	space        did.DID
	gateway      string
	logger       *slog.Logger
	http         *http.Client
	replicaCount uint
}

// GuppyClientConfig configures a guppy-based Storacha client.
type GuppyClientConfig struct {
	// ServiceSigner is the service's identity for signing invocations.
	ServiceSigner principal.Signer

	// Delegation is the customer's UCAN delegation granting space access.
	Delegation delegation.Delegation

	// SpaceDID is the customer's Storacha space DID.
	SpaceDID string

	// ServiceDID is the Storacha service DID.
	// Default: did:web:up.storacha.network
	ServiceDID string

	// ServiceURL is the Storacha upload service URL.
	// Default: https://up.storacha.network
	ServiceURL string

	// GatewayURL is the IPFS gateway URL for fetching blobs.
	// Default: https://w3s.link
	GatewayURL string

	// HTTPClient for outgoing requests.
	// Default: client with 30s timeout
	HTTPClient *http.Client

	// Logger for structured logging.
	// Default: slog.Default()
	Logger *slog.Logger

	// ReplicaCount is the number of replicas for blob replication.
	// Default: 3 (some accounts may have a max of 2)
	ReplicaCount uint
}

// Validate checks that required fields are set.
func (c *GuppyClientConfig) Validate() error {
	if c.ServiceSigner == nil {
		return fmt.Errorf("ServiceSigner is required")
	}
	if c.Delegation == nil {
		return fmt.Errorf("Delegation is required")
	}
	if c.SpaceDID == "" {
		return fmt.Errorf("SpaceDID is required")
	}
	return nil
}

// ApplyDefaults sets default values for optional fields.
func (c *GuppyClientConfig) ApplyDefaults() {
	if c.ServiceDID == "" {
		c.ServiceDID = "did:web:up.storacha.network"
	}
	if c.ServiceURL == "" {
		c.ServiceURL = "https://up.storacha.network"
	}
	if c.GatewayURL == "" {
		c.GatewayURL = "https://w3s.link"
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{}
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	// Note: ReplicaCount of 0 means "skip replication" (useful for dev/test)
	// We don't apply a default here to allow explicit 0
}

// NewGuppyClient creates a new guppy-based Storacha client.
func NewGuppyClient(cfg GuppyClientConfig) (*GuppyClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()

	// Parse space DID
	spaceDID, err := did.Parse(cfg.SpaceDID)
	if err != nil {
		return nil, fmt.Errorf("invalid space DID: %w", err)
	}

	// Parse service DID
	serviceDID, err := did.Parse(cfg.ServiceDID)
	if err != nil {
		return nil, fmt.Errorf("invalid service DID: %w", err)
	}

	// Parse service URL
	serviceURL, err := url.Parse(cfg.ServiceURL)
	if err != nil {
		return nil, fmt.Errorf("invalid service URL: %w", err)
	}

	// Create HTTP channel for connection
	channel := ucantohttp.NewChannel(serviceURL)

	// Create connection to Storacha service
	conn, err := uclient.NewConnection(serviceDID, channel)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	// Create guppy client with our service identity and customer delegation
	guppyClient, err := guppyclient.NewClient(
		guppyclient.WithPrincipal(cfg.ServiceSigner),
		guppyclient.WithConnection(conn),
		guppyclient.WithAdditionalProofs(cfg.Delegation),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create guppy client: %w", err)
	}

	return &GuppyClient{
		client:       guppyClient,
		space:        spaceDID,
		gateway:      cfg.GatewayURL,
		logger:       cfg.Logger,
		http:         cfg.HTTPClient,
		replicaCount: cfg.ReplicaCount,
	}, nil
}

// NewGuppyClientWithConnection creates a new guppy-based Storacha client using a
// pre-built connection. This avoids rebuilding the HTTP channel and connection on
// every call, which is useful when creating many short-lived per-request clients.
func NewGuppyClientWithConnection(cfg GuppyClientConfig, conn uclient.Connection) (*GuppyClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()

	spaceDID, err := did.Parse(cfg.SpaceDID)
	if err != nil {
		return nil, fmt.Errorf("invalid space DID: %w", err)
	}

	guppyClient, err := guppyclient.NewClient(
		guppyclient.WithPrincipal(cfg.ServiceSigner),
		guppyclient.WithConnection(conn),
		guppyclient.WithAdditionalProofs(cfg.Delegation),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create guppy client: %w", err)
	}

	return &GuppyClient{
		client:       guppyClient,
		space:        spaceDID,
		gateway:      cfg.GatewayURL,
		logger:       cfg.Logger,
		http:         cfg.HTTPClient,
		replicaCount: cfg.ReplicaCount,
	}, nil
}

// UploadBlob uploads data to the space and returns its CID.
func (c *GuppyClient) UploadBlob(ctx context.Context, spaceDID string, data []byte) (string, error) {
	// Compute CID for logging
	cidStr, _, err := ComputeCID(data)
	if err != nil {
		return "", fmt.Errorf("failed to compute CID: %w", err)
	}

	c.logger.Debug("uploading blob", "cid", cidStr, "size", len(data))

	// Upload blob using guppy - this handles the full allocate→put→accept workflow
	addedBlob, err := c.client.SpaceBlobAdd(ctx, bytes.NewReader(data), c.space)
	if err != nil {
		return "", fmt.Errorf("failed to upload blob: %w", err)
	}

	// Replicate to multiple nodes for long-term content discoverability via IPNI
	// Skip replication if replicaCount is 0 (useful for dev/test accounts without replication)
	if c.replicaCount > 0 {
		_, _, err = c.client.SpaceBlobReplicate(ctx, c.space,
			captypes.Blob{Digest: addedBlob.Digest, Size: addedBlob.Size},
			c.replicaCount, addedBlob.Location)
		if err != nil {
			c.logReplicationResult(err, "REPLICATION FAILED - blob stored but not replicated, may disappear",
				"cid", cidStr)
		}
	}

	c.logger.Debug("blob uploaded successfully", "cid", cidStr)

	return cidStr, nil
}

// UploadCAR uploads CAR data to Storacha using blob/add and upload/add capabilities.
// Returns the root CID (DAG root) and the raw blob CID (directly resolvable).
// SpaceIndexAdd is disabled — it always fails with a server-side bug and is not needed for blob persistence.
func (c *GuppyClient) UploadCAR(ctx context.Context, spaceDID string, data []byte) (string, string, error) {
	// Decode CAR to get root
	roots, _, err := car.Decode(bytes.NewReader(data))
	if err != nil {
		return "", "", fmt.Errorf("failed to decode CAR: %w", err)
	}

	if len(roots) == 0 {
		return "", "", fmt.Errorf("CAR has no roots")
	}

	rootCID := roots[0]
	rootLink := cidlink.Link{Cid: rootCID.(cidlink.Link).Cid}

	c.logger.Debug("uploading CAR", "root", rootLink.Cid.String(), "size", len(data))

	// Compute raw blob CID and shard link
	rawCIDStr, shardMultihash, err := ComputeCID(data)
	if err != nil {
		return "", "", fmt.Errorf("failed to compute shard hash: %w", err)
	}
	shardCIDParsed := cid.NewCidV1(uint64(multicodec.Car), shardMultihash)
	shardLink := cidlink.Link{Cid: shardCIDParsed}

	// Upload CAR blob using guppy
	addedBlob, err := c.client.SpaceBlobAdd(ctx, bytes.NewReader(data), c.space)
	if err != nil {
		return "", "", fmt.Errorf("failed to upload CAR blob: %w", err)
	}

	// Replicate to multiple nodes for long-term content discoverability via IPNI
	// Skip replication if replicaCount is 0 (useful for dev/test accounts without replication)
	if c.replicaCount > 0 {
		_, _, err = c.client.SpaceBlobReplicate(ctx, c.space,
			captypes.Blob{Digest: addedBlob.Digest, Size: addedBlob.Size},
			c.replicaCount, addedBlob.Location)
		if err != nil {
			c.logReplicationResult(err, "REPLICATION FAILED for CAR blob",
				"shard", shardLink.Cid.String())
		}
	}

	c.logger.Debug("CAR blob uploaded", "shard", shardLink.Cid.String(), "rawCID", rawCIDStr)

	// Register upload (root + shard) — no SpaceIndexAdd needed for blob persistence
	_, err = c.client.UploadAdd(ctx, c.space, rootLink, []ipld.Link{shardLink})
	if err != nil {
		return "", "", fmt.Errorf("failed to register upload: %w", err)
	}

	c.logger.Debug("upload registered successfully", "root", rootLink.Cid.String(), "rawCID", rawCIDStr)

	return rootLink.Cid.String(), rawCIDStr, nil
}

// UploadFullStateCAR uploads a full-state CAR, publishes a sharded DAG index,
// registers content indexing, and finally registers the upload head.
func (c *GuppyClient) UploadFullStateCAR(ctx context.Context, carData []byte, rootCID cid.Cid, positions map[cid.Cid]blobindex.Position) (string, error) {
	rootLink := cidlink.Link{Cid: rootCID}

	_, shardMultihash, err := ComputeCID(carData)
	if err != nil {
		return "", fmt.Errorf("failed to compute shard hash: %w", err)
	}
	shardCID := cid.NewCidV1(uint64(multicodec.Car), shardMultihash)
	shardLink := cidlink.Link{Cid: shardCID}

	c.logger.Info("uploading full-state CAR", "root", rootCID.String(), "shard", shardCID.String(), "size", len(carData), "blocks", len(positions))

	addedShard, err := c.client.SpaceBlobAdd(ctx, bytes.NewReader(carData), c.space)
	if err != nil {
		return "", fmt.Errorf("failed to upload full-state CAR shard: %w", err)
	}
	c.logger.Info("SpaceBlobAdd succeeded for CAR shard", "shard", shardCID.String())

	if c.replicaCount > 0 {
		_, _, err = c.client.SpaceBlobReplicate(ctx, c.space,
			captypes.Blob{Digest: addedShard.Digest, Size: addedShard.Size},
			c.replicaCount, addedShard.Location)
		if err != nil {
			c.logReplicationResult(err, "REPLICATION FAILED for full-state shard", "shard", shardCID.String())
		} else {
			c.logger.Info("SpaceBlobReplicate succeeded for CAR shard", "shard", shardCID.String(), "replicas", c.replicaCount)
		}
	}

	indexView := blobindex.NewShardedDagIndexView(rootLink, 1)
	shardSlices := blobindex.NewMultihashMap[blobindex.Position](len(positions))
	for blockCID, pos := range positions {
		shardSlices.Set(blockCID.Hash(), pos)
	}
	indexView.Shards().Set(shardMultihash, shardSlices)

	indexReader, err := blobindex.Archive(indexView)
	if err != nil {
		return "", fmt.Errorf("failed to archive sharded DAG index: %w", err)
	}
	indexData, err := io.ReadAll(indexReader)
	if err != nil {
		return "", fmt.Errorf("failed to read index archive: %w", err)
	}

	addedIndex, err := c.client.SpaceBlobAdd(ctx, bytes.NewReader(indexData), c.space)
	if err != nil {
		return "", fmt.Errorf("failed to upload index blob: %w", err)
	}
	c.logger.Info("SpaceBlobAdd succeeded for index blob", "indexSize", len(indexData))

	if c.replicaCount > 0 {
		_, _, err = c.client.SpaceBlobReplicate(ctx, c.space,
			captypes.Blob{Digest: addedIndex.Digest, Size: addedIndex.Size},
			c.replicaCount, addedIndex.Location)
		if err != nil {
			c.logReplicationResult(err, "REPLICATION FAILED for index blob")
		} else {
			c.logger.Info("SpaceBlobReplicate succeeded for index blob", "replicas", c.replicaCount)
		}
	}

	_, indexMultihash, err := ComputeCID(indexData)
	if err != nil {
		return "", fmt.Errorf("failed to compute index hash: %w", err)
	}
	indexCID := cid.NewCidV1(uint64(multicodec.Car), indexMultihash)

	// Call SpaceIndexAdd WITHOUT the Content field to match the JS client behavior.
	// Guppy's SpaceIndexAdd always sets Content, which triggers a broken server-side
	// retrieval code path. Omitting Content makes the server fetch the index blob
	// itself via its internal blobRetriever, which works.
	if err := c.spaceIndexAddWithoutContent(ctx, indexCID); err != nil {
		c.logger.Warn("SpaceIndexAdd failed", "root", rootCID.String(), "indexCID", indexCID.String(), "error", err)
	} else {
		c.logger.Info("SpaceIndexAdd succeeded", "root", rootCID.String(), "indexCID", indexCID.String())
	}

	if _, err := c.client.UploadAdd(ctx, c.space, rootLink, []ipld.Link{shardLink}); err != nil {
		return "", fmt.Errorf("failed to register full-state upload: %w", err)
	}
	c.logger.Info("UploadAdd succeeded — full-state CAR upload complete", "root", rootCID.String(), "shard", shardCID.String())

	return rootCID.String(), nil
}

// FetchBlob retrieves data by CID via the IPFS gateway.
func (c *GuppyClient) FetchBlob(ctx context.Context, cidStr string) ([]byte, error) {
	// Validate CID
	_, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("invalid CID: %w", err)
	}

	// Fetch via gateway
	url := fmt.Sprintf("%s/ipfs/%s", c.gateway, cidStr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("gateway request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("gateway returned status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return data, nil
}

// RemoveBlob is a no-op as guppy doesn't implement blob/remove and this is deprioritized.
// Logs a warning and returns nil.
func (c *GuppyClient) RemoveBlob(ctx context.Context, spaceDID string, digest []byte) error {
	c.logger.Warn("RemoveBlob called but not implemented - guppy doesn't support blob/remove")
	return nil
}

// UploadFinalizedBlob uploads a single immutable raw data blob through the full
// Storacha pipeline including IPNI indexing, enabling gateway resolution by CID.
func (c *GuppyClient) UploadFinalizedBlob(ctx context.Context, data []byte) (string, error) {
	cidStr, blobMH, err := ComputeCID(data)
	if err != nil {
		return "", fmt.Errorf("compute CID: %w", err)
	}
	blobCID, err := cid.Decode(cidStr)
	if err != nil {
		return "", fmt.Errorf("decode CID: %w", err)
	}
	blobLink := cidlink.Link{Cid: blobCID}

	c.logger.Info("uploading finalized blob", "cid", cidStr, "size", len(data))

	// 1. Upload raw blob
	addedBlob, err := c.client.SpaceBlobAdd(ctx, bytes.NewReader(data), c.space)
	if err != nil {
		return "", fmt.Errorf("SpaceBlobAdd: %w", err)
	}

	// 2. Replicate
	if c.replicaCount > 0 {
		_, _, err = c.client.SpaceBlobReplicate(ctx, c.space,
			captypes.Blob{Digest: addedBlob.Digest, Size: addedBlob.Size},
			c.replicaCount, addedBlob.Location)
		if err != nil {
			c.logReplicationResult(err, "REPLICATION FAILED for finalized blob", "cid", cidStr)
		}
	}

	// 3. Build trivial ShardedDagIndex: one shard (the blob itself), one block at offset 0.
	// For a raw blob, the shard CID uses cid.Raw codec with the blob's own multihash.
	indexView := blobindex.NewShardedDagIndexView(blobLink, 1)
	shardSlices := blobindex.NewMultihashMap[blobindex.Position](1)
	shardSlices.Set(blobMH, blobindex.Position{Offset: 0, Length: uint64(len(data))})
	indexView.Shards().Set(blobMH, shardSlices)

	indexReader, err := blobindex.Archive(indexView)
	if err != nil {
		return "", fmt.Errorf("archive index: %w", err)
	}
	indexData, err := io.ReadAll(indexReader)
	if err != nil {
		return "", fmt.Errorf("read index archive: %w", err)
	}

	// 4. Upload index blob
	addedIndex, err := c.client.SpaceBlobAdd(ctx, bytes.NewReader(indexData), c.space)
	if err != nil {
		return "", fmt.Errorf("SpaceBlobAdd index: %w", err)
	}
	if c.replicaCount > 0 {
		_, _, err = c.client.SpaceBlobReplicate(ctx, c.space,
			captypes.Blob{Digest: addedIndex.Digest, Size: addedIndex.Size},
			c.replicaCount, addedIndex.Location)
		if err != nil {
			c.logReplicationResult(err, "REPLICATION FAILED for finalized blob index", "cid", cidStr)
		}
	}

	// 5. Register index with IPNI
	_, indexMH, err := ComputeCID(indexData)
	if err != nil {
		return "", fmt.Errorf("compute index CID: %w", err)
	}
	indexCID := cid.NewCidV1(uint64(multicodec.Car), indexMH)
	if err := c.spaceIndexAddWithoutContent(ctx, indexCID); err != nil {
		c.logger.Error("SpaceIndexAdd failed for finalized blob", "cid", cidStr, "error", err)
		// Non-fatal: gateway may still resolve via UploadAdd
	}

	// 6. Register upload
	shardLink := cidlink.Link{Cid: cid.NewCidV1(cid.Raw, blobMH)}
	if _, err := c.client.UploadAdd(ctx, c.space, blobLink, []ipld.Link{shardLink}); err != nil {
		return "", fmt.Errorf("UploadAdd: %w", err)
	}

	c.logger.Info("finalized blob uploaded with full pipeline", "cid", cidStr)
	return cidStr, nil
}

// spaceIndexAddWithoutContent invokes space/index/add with only the index CID,
// omitting the Content field. This matches the JS upload-client behavior and avoids
// triggering a broken server-side code path where the server tries to fetch the
// index via UCANTO HTTP retrieval (which fails with "missing X-Agent-Message header").
// isTransientReplicationError returns true for replication errors that are
// infrastructure-side and transient (e.g. no storage candidates available).
// These don't indicate data loss — the primary store succeeded — so they
// should be logged as warnings rather than errors.
func isTransientReplicationError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "CandidateUnavailable")
}

// logReplicationResult logs a replication outcome at WARN for transient
// infrastructure errors and ERROR for all other failures.
func (c *GuppyClient) logReplicationResult(err error, msg string, args ...any) {
	if err == nil {
		return
	}
	if isTransientReplicationError(err) {
		c.logger.Warn(msg, append(args, "error", err)...)
	} else {
		c.logger.Error(msg, append(args, "error", err)...)
	}
}

func (c *GuppyClient) spaceIndexAddWithoutContent(ctx context.Context, indexCID cid.Cid) error {
	indexLink := cidlink.Link{Cid: indexCID}

	proofs, err := c.client.Proofs()
	if err != nil {
		return fmt.Errorf("getting proofs: %w", err)
	}
	pfs := make([]delegation.Proof, 0, len(proofs))
	for _, del := range proofs {
		pfs = append(pfs, delegation.FromDelegation(del))
	}

	inv, err := spaceindexcap.Add.Invoke(
		c.client.Issuer(),
		c.client.Connection().ID(),
		c.space.String(),
		spaceindexcap.AddCaveats{
			Index: indexLink,
			// Content deliberately omitted (nil) — see method doc comment.
		},
		delegation.WithProof(pfs...),
	)
	if err != nil {
		return fmt.Errorf("invoking space/index/add: %w", err)
	}

	resp, err := uclient.Execute(ctx, []invocation.Invocation{inv}, c.client.Connection())
	if err != nil {
		return fmt.Errorf("executing space/index/add: %w", err)
	}

	rcptLink, ok := resp.Get(inv.Link())
	if !ok {
		return fmt.Errorf("receipt not found for space/index/add")
	}

	reader, err := receipt.NewReceiptReaderFromTypes[spaceindexcap.AddOk, serverdatamodel.HandlerExecutionErrorModel](
		spaceindexcap.AddOkType(), serverdatamodel.HandlerExecutionErrorType(), captypes.Converters...)
	if err != nil {
		return fmt.Errorf("creating receipt reader: %w", err)
	}

	rcpt, err := reader.Read(rcptLink, resp.Blocks())
	if err != nil {
		return fmt.Errorf("reading receipt: %w", err)
	}

	_, failErr := result.Unwrap(result.MapError(
		result.MapError(
			rcpt.Out(),
			func(errorModel serverdatamodel.HandlerExecutionErrorModel) failuredatamodel.FailureModel {
				return failuredatamodel.FailureModel(errorModel.Cause)
			},
		),
		failure.FromFailureModel,
	))
	if failErr != nil {
		return fmt.Errorf("space/index/add failed: %w", failErr)
	}

	return nil
}

// Ensure GuppyClient implements StorachaClient.
var _ StorachaClient = (*GuppyClient)(nil)
