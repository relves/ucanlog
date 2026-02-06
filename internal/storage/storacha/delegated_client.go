package storacha

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/multiformats/go-multicodec"
	mh "github.com/multiformats/go-multihash"
	"github.com/storacha/go-libstoracha/blobindex"
	contentcap "github.com/storacha/go-libstoracha/capabilities/space/content"
	spaceindexcap "github.com/storacha/go-libstoracha/capabilities/space/index"
	uploadcap "github.com/storacha/go-libstoracha/capabilities/upload"
	"github.com/storacha/go-ucanto/client"
	"github.com/storacha/go-ucanto/core/car"
	"github.com/storacha/go-ucanto/core/dag/blockstore"
	"github.com/storacha/go-ucanto/core/delegation"
	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/message"
	"github.com/storacha/go-ucanto/core/receipt"
	"github.com/storacha/go-ucanto/core/receipt/ran"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/did"
	"github.com/storacha/go-ucanto/principal"
	"github.com/storacha/go-ucanto/principal/ed25519/signer"
	ucantohttp "github.com/storacha/go-ucanto/transport/http"
	"github.com/storacha/go-ucanto/ucan"
)

// DelegatedClientConfig configures a delegated Storacha client.
type DelegatedClientConfig struct {
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

	// RetryAttempts is the number of retry attempts for failed requests.
	// Default: 3
	RetryAttempts int

	// RetryDelay is the initial delay between retries.
	// Default: 1s
	RetryDelay time.Duration

	// Logger for structured logging.
	// Default: slog.Default()
	Logger *slog.Logger
}

// Validate checks that required fields are set.
func (c *DelegatedClientConfig) Validate() error {
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
func (c *DelegatedClientConfig) ApplyDefaults() {
	if c.ServiceDID == "" {
		c.ServiceDID = "did:web:up.storacha.network"
	}
	if c.ServiceURL == "" {
		c.ServiceURL = "https://up.storacha.network"
	}
	if c.GatewayURL == "" {
		c.GatewayURL = "https://ipfs.w3s.link"
	}
	if c.HTTPClient == nil {
		c.HTTPClient = &http.Client{
			Timeout: 10 * time.Second, // Reduced from 30s for faster failure on slow gateways
		}
	}
	if c.RetryAttempts == 0 {
		c.RetryAttempts = 2 // Reduced from 3 for faster failure
	}
	if c.RetryDelay == 0 {
		c.RetryDelay = time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// DelegatedClient uploads to a customer's Storacha space using their delegation.
// This client uses customer-provided UCAN delegations to write to customer-owned spaces.
type DelegatedClient struct {
	cfg    DelegatedClientConfig
	space  did.DID
	conn   client.Connection
	logger *slog.Logger
}

// NewDelegatedClient creates a new client that uses customer delegations.
func NewDelegatedClient(cfg DelegatedClientConfig) (*DelegatedClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()

	// Parse the space DID
	space, err := did.Parse(cfg.SpaceDID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse space DID: %w", err)
	}

	// Parse service URL and DID
	serviceURL, err := url.Parse(cfg.ServiceURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse service URL: %w", err)
	}
	servicePrincipal, err := did.Parse(cfg.ServiceDID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse service DID: %w", err)
	}

	// Create transport channel and connection
	channel := ucantohttp.NewChannel(serviceURL)
	conn, err := client.NewConnection(servicePrincipal, channel)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	return &DelegatedClient{
		cfg:    cfg,
		space:  space,
		conn:   conn,
		logger: cfg.Logger,
	}, nil
}

// Ensure DelegatedClient implements StorachaClient.
var _ StorachaClient = (*DelegatedClient)(nil)

// UploadBlob uploads data to the customer's space using the provided delegation.
func (c *DelegatedClient) UploadBlob(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	// Compute CID and multihash
	cidStr, multihash, err := ComputeCID(data)
	if err != nil {
		return "", fmt.Errorf("failed to compute CID: %w", err)
	}

	// Create caveats
	caveats := AddCaveats{
		Blob: Blob{
			Digest: multihash,
			Size:   uint64(len(data)),
		},
	}

	// Create capability
	capability := ucan.NewCapability(
		BlobAddAbility,
		c.space.String(),
		caveats,
	)

	// Create invocation with the provided delegation as proof
	proofs := []delegation.Proof{delegation.FromDelegation(dlg)}
	inv, err := invocation.Invoke(
		c.cfg.ServiceSigner,
		c.conn.ID(),
		capability,
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return "", fmt.Errorf("failed to create invocation: %w", err)
	}

	// Execute the invocation
	resp, err := client.Execute(ctx, []invocation.Invocation{inv}, c.conn)
	if err != nil {
		return "", fmt.Errorf("failed to execute invocation: %w", err)
	}

	// Get the receipt
	rcptLink, found := resp.Get(inv.Link())
	if !found {
		return "", fmt.Errorf("no receipt found for invocation: %s", inv.Link())
	}

	// Create block store from response
	bs, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(resp.Blocks()))
	if err != nil {
		return "", fmt.Errorf("failed to create block store: %w", err)
	}

	// Read the receipt
	rcpt, err := receipt.NewAnyReceipt(rcptLink, bs)
	if err != nil {
		return "", fmt.Errorf("failed to read receipt: %w", err)
	}

	// Check result
	out := rcpt.Out()
	_, xerr := result.Unwrap(out)
	if xerr != nil {
		if errNode, isNode := xerr.(ipld.Node); isNode {
			if msgNode, err := errNode.LookupByString("message"); err == nil {
				if msg, err := msgNode.AsString(); err == nil {
					return "", fmt.Errorf("upload failed: %s", msg)
				}
			}
		}
		return "", fmt.Errorf("upload failed: %v", xerr)
	}

	// Process fork effects to upload data if needed
	if err := c.processUploadEffects(ctx, rcpt, data); err != nil {
		return "", err
	}

	return cidStr, nil
}

// processUploadEffects handles the blob/allocate and http/put workflow.
func (c *DelegatedClient) processUploadEffects(ctx context.Context, rcpt receipt.AnyReceipt, data []byte) error {
	forkEffects := rcpt.Fx().Fork()

	var allocateAddress *AllocateAddress
	var httpPutTask delegation.Delegation
	var acceptTask delegation.Delegation

	c.logger.Debug("processing fork effects", "count", len(forkEffects))

	for _, effect := range forkEffects {
		forkInv, ok := effect.Invocation()
		if !ok {
			continue
		}

		caps := forkInv.Capabilities()
		if len(caps) == 0 {
			continue
		}

		cap := caps[0]
		canAbility := cap.Can()
		c.logger.Debug("found capability", "capability", canAbility)

		if canAbility == "http/put" {
			httpPutTask = forkInv
			continue
		}

		if canAbility == BlobAcceptAbility || canAbility == W3SBlobAcceptAbility {
			acceptTask = forkInv
			continue
		}

		if canAbility != "ucan/conclude" {
			continue
		}

		// Extract embedded receipt from ucan/conclude
		nb := cap.Nb()
		nbNode, ok := nb.(ipld.Node)
		if !ok {
			continue
		}

		receiptLinkNode, err := nbNode.LookupByString("receipt")
		if err != nil {
			continue
		}

		receiptLink, err := receiptLinkNode.AsLink()
		if err != nil {
			continue
		}

		embeddedBs, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(forkInv.Blocks()))
		if err != nil {
			continue
		}

		embeddedRcpt, err := receipt.NewAnyReceipt(receiptLink, embeddedBs)
		if err != nil {
			continue
		}

		ran := embeddedRcpt.Ran()
		ranInv, ok := ran.Invocation()
		if !ok {
			continue
		}

		ranCaps := ranInv.Capabilities()
		if len(ranCaps) == 0 {
			continue
		}

		ranCan := ranCaps[0].Can()
		if ranCan != "blob/allocate" && ranCan != "web3.storage/blob/allocate" {
			continue
		}

		allocOut := embeddedRcpt.Out()
		allocOk, allocErr := result.Unwrap(allocOut)
		if allocErr != nil {
			continue
		}

		allocOkNode, ok := allocOk.(ipld.Node)
		if !ok {
			continue
		}

		addressNode, err := allocOkNode.LookupByString("address")
		if err != nil || addressNode.IsNull() {
			c.logger.Debug("blob allocate missing address", "error", err, "isNull", addressNode != nil && addressNode.IsNull())
			continue
		}

		allocateAddress, err = parseAllocateAddress(addressNode)
		if err != nil {
			c.logger.Debug("failed to parse allocate address", "error", err)
		} else {
			c.logger.Debug("got allocate address URL")//, "url", allocateAddress.URL)
		}
	}

	// Upload data if we got an address
	if allocateAddress != nil {
		c.logger.Debug("uploading to presigned URL", "bytes", len(data))
		if err := c.uploadToPresignedURL(ctx, allocateAddress.URL, data, allocateAddress.Headers); err != nil {
			return fmt.Errorf("failed to upload data: %w", err)
		}
		c.logger.Debug("upload successful")

		if httpPutTask != nil {
			_ = c.submitHttpPutReceipt(ctx, httpPutTask)
		}

		if acceptTask != nil {
			if err := c.pollAcceptReceipt(ctx, acceptTask.Link()); err != nil {
				return fmt.Errorf("blob accept failed (not confirmed stored): %w", err)
			}
			c.logger.Debug("blob accepted successfully")
		}
	} else {
		c.logger.Debug("no allocate address - blob may already exist")
	}

	return nil
}

// uploadToPresignedURL uploads data via HTTP PUT.
func (c *DelegatedClient) uploadToPresignedURL(ctx context.Context, uploadURL string, data []byte, headers map[string]string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, uploadURL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.ContentLength = int64(len(data))
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP PUT failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("upload returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// submitHttpPutReceipt confirms the HTTP PUT succeeded.
func (c *DelegatedClient) submitHttpPutReceipt(ctx context.Context, httpPutTask delegation.Delegation) error {
	// The http/put task has signing keys in its facts that we need to use
	// to sign the receipt. Extract the keys from facts[0]['keys']
	facts := httpPutTask.Facts()
	if len(facts) == 0 {
		return fmt.Errorf("http/put task has no facts")
	}

	// Get the 'keys' field from the first fact
	var keysData []byte
	for _, fact := range facts {
		if keysNode, ok := fact["keys"]; ok {
			// The keys should be a signer archive we can use
			if keysBytes, ok := keysNode.([]byte); ok {
				keysData = keysBytes
				break
			}
			// It might be an IPLD node containing the key data
			if keysIPLD, ok := keysNode.(ipld.Node); ok {
				if keysIPLD.Kind() == datamodel.Kind_Bytes {
					keysData, _ = keysIPLD.AsBytes()
					break
				}
				// It might be a map containing the signer archive
				if keysIPLD.Kind() == datamodel.Kind_Map {
					// Try to extract from the map - it's likely a signer archive
					keysData = extractSignerArchive(keysIPLD)
					if keysData != nil {
						break
					}
				}
			}
		}
	}

	if keysData == nil {
		return fmt.Errorf("no signing keys found in http/put task facts")
	}

	// Parse the derived signer from the keys
	derivedSigner, err := signer.Decode(keysData)
	if err != nil {
		return fmt.Errorf("failed to decode http/put signer: %w", err)
	}

	// Create a successful result for the http/put receipt
	// The result is just { ok: {} }
	okResult := result.Ok[OkBuilder, ErrBuilder](OkBuilder{})

	// Issue the receipt
	httpPutReceipt, err := receipt.Issue(
		derivedSigner,
		okResult,
		ran.FromLink(httpPutTask.Link()),
	)
	if err != nil {
		return fmt.Errorf("failed to issue http/put receipt: %w", err)
	}

	// Create ucan/conclude invocation to submit the receipt
	concludeCaveats := ConcludeCaveats{
		Receipt: httpPutReceipt.Root().Link(),
	}

	concludeCap := ucan.NewCapability(
		"ucan/conclude",
		c.cfg.ServiceSigner.DID().String(),
		concludeCaveats,
	)

	concludeInv, err := invocation.Invoke(
		c.cfg.ServiceSigner,
		c.conn.ID(),
		concludeCap,
		delegation.WithNoExpiration(),
	)
	if err != nil {
		return fmt.Errorf("failed to create conclude invocation: %w", err)
	}

	// Attach the receipt blocks to the invocation
	for block, err := range httpPutReceipt.Export() {
		if err != nil {
			return fmt.Errorf("failed to export receipt block: %w", err)
		}
		if err := concludeInv.Attach(block); err != nil {
			return fmt.Errorf("failed to attach receipt block: %w", err)
		}
	}

	// Execute the conclude invocation
	_, err = client.Execute(ctx, []invocation.Invocation{concludeInv}, c.conn)
	if err != nil {
		return fmt.Errorf("failed to execute conclude: %w", err)
	}

	return nil
}

// extractReceiptFromResponse extracts a receipt from the response body.
// The receipts endpoint can return two formats:
// 1. ucan/receipt@0.9.1 archive - a direct receipt archive
// 2. ucanto/message@7.0.0 - an agent message with receipts in the report field
func extractReceiptFromResponse(body []byte, taskLink ipld.Link) (receipt.AnyReceipt, error) {
	// First try to extract as a direct receipt archive (ucan/receipt@0.9.1)
	rcpt, err := receipt.Extract(body)
	if err == nil {
		return rcpt, nil
	}

	// If that fails, try to parse as an agent message (ucanto/message@7.0.0)
	roots, blocks, carErr := car.Decode(bytes.NewReader(body))
	if carErr != nil {
		return nil, fmt.Errorf("failed to decode as receipt (%v) or CAR (%v)", err, carErr)
	}

	if len(roots) == 0 {
		return nil, fmt.Errorf("no roots in CAR")
	}

	bs, bsErr := blockstore.NewBlockStore(blockstore.WithBlocksIterator(blocks))
	if bsErr != nil {
		return nil, fmt.Errorf("failed to create blockstore: %w", bsErr)
	}

	msg, msgErr := message.NewMessage(roots[0], bs)
	if msgErr != nil {
		return nil, fmt.Errorf("failed to decode as receipt (%v) or message (%v)", err, msgErr)
	}

	// Look up the receipt for our task in the message's report
	rcptLink, found := msg.Get(taskLink)
	if !found {
		// The message exists but doesn't contain a receipt for our task yet
		return nil, fmt.Errorf("message does not contain receipt for task %s", taskLink)
	}

	rcpt, found, rcptErr := msg.Receipt(rcptLink)
	if rcptErr != nil {
		return nil, fmt.Errorf("failed to get receipt from message: %w", rcptErr)
	}
	if !found {
		return nil, fmt.Errorf("receipt link %s not found in message blocks", rcptLink)
	}

	return rcpt, nil
}

// pollAcceptReceipt polls for blob acceptance and verifies the receipt indicates success.
func (c *DelegatedClient) pollAcceptReceipt(ctx context.Context, taskLink ipld.Link) error {
	endpoint := fmt.Sprintf("%s/%s", ReceiptsEndpoint, taskLink.String())
	c.logger.Debug("polling accept receipt", "endpoint", endpoint)

	for attempt := 0; attempt < PollRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(PollInterval):
			}
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
		if err != nil {
			return fmt.Errorf("creating receipt request: %w", err)
		}

		resp, err := c.cfg.HTTPClient.Do(req)
		if err != nil {
			c.logger.Debug("poll attempt failed", "attempt", attempt+1, "error", err)
			continue
		}

		c.logger.Debug("poll attempt status", "attempt", attempt+1, "status", resp.StatusCode)

		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			continue
		}

		// Read the response body
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			c.logger.Debug("poll attempt failed to read body", "attempt", attempt+1, "error", err)
			continue
		}

		// Check if response is "No receipt found" message (base64 encoded text)
		// The endpoint returns base64-encoded error messages for 404-like conditions
		if decoded, err := base64DecodeString(string(body)); err == nil {
			if strings.Contains(decoded, "No receipt") || strings.Contains(decoded, "not found") {
				c.logger.Debug("poll attempt receipt not ready", "attempt", attempt+1, "message", decoded)
				continue
			}
		}

		// Parse the response - it can be either:
		// 1. ucan/receipt@0.9.1 archive (direct receipt)
		// 2. ucanto/message@7.0.0 (agent message containing receipts)
		rcpt, err := extractReceiptFromResponse(body, taskLink)
		if err != nil {
			c.logger.Debug("poll attempt failed to extract receipt", "attempt", attempt+1, "error", err)
			continue
		}

		// Successfully parsed receipt - check the out field
		out := rcpt.Out()
		_, xerr := result.Unwrap(out)
		if xerr != nil {
			// Extract error message if possible
			if errNode, isNode := xerr.(ipld.Node); isNode {
				if msgNode, err := errNode.LookupByString("message"); err == nil {
					if msg, err := msgNode.AsString(); err == nil {
						return fmt.Errorf("blob accept receipt indicates failure: %s", msg)
					}
				}
				if nameNode, err := errNode.LookupByString("name"); err == nil {
					if name, err := nameNode.AsString(); err == nil {
						return fmt.Errorf("blob accept receipt indicates failure: %s", name)
					}
				}
			}
			return fmt.Errorf("blob accept receipt indicates failure: %v", xerr)
		}

		c.logger.Debug("poll attempt receipt verified", "attempt", attempt+1)
		return nil
	}

	return fmt.Errorf("accept receipt not found after %d attempts", PollRetries)
}

// base64DecodeString attempts to decode a base64 string, returning the decoded string or an error
func base64DecodeString(s string) (string, error) {
	// Try standard base64 first
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		// Try URL-safe base64
		decoded, err = base64.URLEncoding.DecodeString(strings.TrimSpace(s))
		if err != nil {
			return "", err
		}
	}
	return string(decoded), nil
}

// UploadCAR uploads CAR data to the customer's space using the provided delegation.
func (c *DelegatedClient) UploadCAR(ctx context.Context, spaceDID string, data []byte, dlg delegation.Delegation) (string, error) {
	// Decode CAR to get root and block positions
	roots, blocks, err := car.Decode(bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("failed to decode CAR: %w", err)
	}

	if len(roots) == 0 {
		return "", fmt.Errorf("CAR has no roots")
	}

	rootCID := roots[0]
	rootLink := cidlink.Link{Cid: rootCID.(cidlink.Link).Cid}

	// Collect block positions
	type blockPosition struct {
		hash   mh.Multihash
		offset uint64
		length uint64
	}
	var blockPositions []blockPosition

	carReader := bytes.NewReader(data)
	headerSize, err := readCARHeaderSize(carReader)
	if err != nil {
		return "", fmt.Errorf("failed to read CAR header: %w", err)
	}

	currentOffset := headerSize
	for blk, err := range blocks {
		if err != nil {
			return "", fmt.Errorf("error reading CAR block: %w", err)
		}
		blockCID := blk.Link().(cidlink.Link).Cid
		blockData := blk.Bytes()

		cidBytes := blockCID.Bytes()
		blockHeaderLength := varintSize(len(cidBytes)+len(blockData)) + len(cidBytes)
		blockEncodedSize := uint64(blockHeaderLength + len(blockData))

		blockPositions = append(blockPositions, blockPosition{
			hash:   blockCID.Hash(),
			offset: currentOffset + uint64(blockHeaderLength),
			length: uint64(len(blockData)),
		})
		currentOffset += blockEncodedSize
	}

	// Upload CAR blob
	_, shardMultihash, err := ComputeCID(data)
	if err != nil {
		return "", fmt.Errorf("failed to compute shard hash: %w", err)
	}
	shardCIDParsed := cid.NewCidV1(uint64(multicodec.Car), shardMultihash)
	shardLink := cidlink.Link{Cid: shardCIDParsed}

	_, err = c.UploadBlob(ctx, spaceDID, data, dlg)
	if err != nil {
		return "", fmt.Errorf("failed to upload CAR blob: %w", err)
	}

	// Build index
	indexView := blobindex.NewShardedDagIndexView(rootLink, -1)
	shardSlices := blobindex.NewMultihashMap[blobindex.Position](-1)

	for _, bp := range blockPositions {
		shardSlices.Set(bp.hash, blobindex.Position{
			Offset: bp.offset,
			Length: bp.length,
		})
	}
	shardSlices.Set(shardMultihash, blobindex.Position{Offset: 0, Length: uint64(len(data))})
	indexView.Shards().Set(shardMultihash, shardSlices)

	indexReader, err := blobindex.Archive(indexView)
	if err != nil {
		return "", fmt.Errorf("failed to archive index: %w", err)
	}
	indexData, err := io.ReadAll(indexReader)
	if err != nil {
		return "", fmt.Errorf("failed to read index data: %w", err)
	}

	// Upload index blob - this stores it with a raw CID
	indexCIDStr, indexMultihash, err := ComputeCID(indexData)
	if err != nil {
		return "", fmt.Errorf("failed to compute index hash: %w", err)
	}

	_, err = c.UploadBlob(ctx, spaceDID, indexData, dlg)
	if err != nil {
		return "", fmt.Errorf("failed to upload index blob: %w", err)
	}

	c.logger.Debug("index blob uploaded", "cid", indexCIDStr)

	// For index/add, we need to reference the index as a CAR
	// The blob is stored by multihash, but index/add expects the CAR codec
	indexCIDParsed := cid.NewCidV1(uint64(multicodec.Car), indexMultihash)
	c.logger.Debug("registering index with CAR CID", "cid", indexCIDParsed.String())

	// Register index
	if err := c.spaceIndexAdd(ctx, indexCIDParsed, uint64(len(indexData)), rootLink.Cid, dlg); err != nil {
		return "", fmt.Errorf("failed to register index: %w", err)
	}

	// Register upload
	if err := c.uploadAdd(ctx, rootLink, []ipld.Link{shardLink}, dlg); err != nil {
		return "", fmt.Errorf("failed to register upload: %w", err)
	}

	return rootLink.Cid.String(), nil
}

// spaceIndexAdd registers an index with Storacha using the provided delegation.
func (c *DelegatedClient) spaceIndexAdd(ctx context.Context, indexCID cid.Cid, indexSize uint64, rootCID cid.Cid, dlg delegation.Delegation) error {
	indexLink := cidlink.Link{Cid: indexCID}

	// Convert delegation to proof
	proofs := []delegation.Proof{delegation.FromDelegation(dlg)}

	// Create retrieval auth delegation
	retrievalAuth, err := contentcap.Retrieve.Delegate(
		c.cfg.ServiceSigner,
		c.conn.ID(),
		c.space.String(),
		contentcap.RetrieveCaveats{
			Blob:  contentcap.BlobDigest{Digest: indexCID.Hash()},
			Range: contentcap.Range{Start: 0, End: indexSize - 1},
		},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return fmt.Errorf("failed to create retrieval auth: %w", err)
	}

	// Create index/add invocation
	// BLOCKED: Content + WithFacts are both required for IPNI indexing, but
	// Storacha's server-side retrieval client crashes on missing X-Agent-Message header.
	// Waiting on Storacha to deploy https://github.com/storacha/go-ucanto/pull/83
	// Without this, uploads work but CIDs become unretrievable via gateway after ~18hrs
	// when IPNI caches expire, because the index is registered without content linkage.
	// rootLink := cidlink.Link{Cid: rootCID}
	inv, err := spaceindexcap.Add.Invoke(
		c.cfg.ServiceSigner,
		c.conn.ID(),
		c.space.String(),
		spaceindexcap.AddCaveats{
			Index: indexLink,
			// Content: rootLink, // BLOCKED: requires server-side fix (go-ucanto PR #83)
		},
		delegation.WithProof(proofs...),
		// delegation.WithFacts([]ucan.FactBuilder{authFact{auth: retrievalAuth.Link()}}), // BLOCKED: requires server-side fix
	)
	if err != nil {
		return fmt.Errorf("failed to create index/add invocation: %w", err)
	}

	// Attach retrieval auth blocks
	for blk, err := range retrievalAuth.Blocks() {
		if err != nil {
			return fmt.Errorf("failed to get retrieval auth block: %w", err)
		}
		if err := inv.Attach(blk); err != nil {
			return fmt.Errorf("failed to attach block: %w", err)
		}
	}

	bs, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(inv.Blocks()))
	if err != nil {
		return fmt.Errorf("failed to create blockstore: %w", err)
	}

	invView, err := invocation.NewInvocationView(inv.Link(), bs)
	if err != nil {
		return fmt.Errorf("failed to create invocation view: %w", err)
	}

	resp, err := client.Execute(ctx, []invocation.Invocation{invView}, c.conn)
	if err != nil {
		return fmt.Errorf("failed to execute index/add: %w", err)
	}

	rcptLink, found := resp.Get(inv.Link())
	if !found {
		return fmt.Errorf("no receipt for index/add")
	}

	rcptBs, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(resp.Blocks()))
	if err != nil {
		return fmt.Errorf("failed to create block store: %w", err)
	}

	rcpt, err := receipt.NewAnyReceipt(rcptLink, rcptBs)
	if err != nil {
		return fmt.Errorf("failed to read receipt: %w", err)
	}

	_, xerr := result.Unwrap(rcpt.Out())
	if xerr != nil {
		// Try to extract detailed error message
		if errNode, isNode := xerr.(ipld.Node); isNode {
			// Debug: print all fields
			if errNode.Kind() == datamodel.Kind_Map {
				iter := errNode.MapIterator()
				for !iter.Done() {
					k, v, err := iter.Next()
					if err != nil {
						break
					}
					kStr, _ := k.AsString()
					vStr, _ := v.AsString()
					c.logger.Debug("index/add error field", "key", kStr, "value", vStr)
				}
			}

			if msgNode, err := errNode.LookupByString("message"); err == nil {
				if msg, err := msgNode.AsString(); err == nil {
					return fmt.Errorf("index/add failed: %s", msg)
				}
			}
			if nameNode, err := errNode.LookupByString("name"); err == nil {
				if name, err := nameNode.AsString(); err == nil {
					return fmt.Errorf("index/add failed: %s", name)
				}
			}
		}
		return fmt.Errorf("index/add failed: %v", xerr)
	}

	return nil
}

// uploadAdd registers an upload with Storacha using the provided delegation.
func (c *DelegatedClient) uploadAdd(ctx context.Context, root ipld.Link, shards []ipld.Link, dlg delegation.Delegation) error {
	// Convert delegation to proof
	proofs := []delegation.Proof{delegation.FromDelegation(dlg)}

	inv, err := uploadcap.Add.Invoke(
		c.cfg.ServiceSigner,
		c.conn.ID(),
		c.space.String(),
		uploadcap.AddCaveats{
			Root:   root,
			Shards: shards,
		},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return fmt.Errorf("failed to create upload/add invocation: %w", err)
	}

	resp, err := client.Execute(ctx, []invocation.Invocation{inv}, c.conn)
	if err != nil {
		return fmt.Errorf("failed to execute upload/add: %w", err)
	}

	rcptLink, found := resp.Get(inv.Link())
	if !found {
		return fmt.Errorf("no receipt for upload/add")
	}

	bs, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(resp.Blocks()))
	if err != nil {
		return fmt.Errorf("failed to create block store: %w", err)
	}

	rcpt, err := receipt.NewAnyReceipt(rcptLink, bs)
	if err != nil {
		return fmt.Errorf("failed to read receipt: %w", err)
	}

	_, xerr := result.Unwrap(rcpt.Out())
	if xerr != nil {
		return fmt.Errorf("upload/add failed: %v", xerr)
	}

	return nil
}

// FetchBlobDirect retrieves data directly from Storacha using space/content/retrieve.
// This is faster and more reliable than using public IPFS gateways.
func (c *DelegatedClient) FetchBlobDirect(ctx context.Context, cidStr string, dlg delegation.Delegation) ([]byte, error) {
	parsedCID, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("invalid CID: %w", err)
	}

	// Convert delegation to proof
	proofs := []delegation.Proof{delegation.FromDelegation(dlg)}

	// Create retrieval auth delegation for this blob
	retrievalAuth, err := contentcap.Retrieve.Delegate(
		c.cfg.ServiceSigner,
		c.conn.ID(),
		c.space.String(),
		contentcap.RetrieveCaveats{
			Blob: contentcap.BlobDigest{Digest: parsedCID.Hash()},
			// Range for full blob retrieval
			Range: contentcap.Range{Start: 0, End: 0}, // 0,0 means full content
		},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create retrieval auth: %w", err)
	}

	// Invoke space/content/retrieve to get a presigned URL
	inv, err := contentcap.Retrieve.Invoke(
		c.cfg.ServiceSigner,
		c.conn.ID(),
		c.space.String(),
		contentcap.RetrieveCaveats{
			Blob:  contentcap.BlobDigest{Digest: parsedCID.Hash()},
			Range: contentcap.Range{Start: 0, End: 0},
		},
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create retrieve invocation: %w", err)
	}

	// Attach retrieval auth blocks
	for blk, err := range retrievalAuth.Blocks() {
		if err != nil {
			return nil, fmt.Errorf("failed to get retrieval auth block: %w", err)
		}
		if err := inv.Attach(blk); err != nil {
			return nil, fmt.Errorf("failed to attach block: %w", err)
		}
	}

	// Execute the retrieve invocation
	resp, err := client.Execute(ctx, []invocation.Invocation{inv}, c.conn)
	if err != nil {
		return nil, fmt.Errorf("failed to execute retrieve: %w", err)
	}

	// Get the receipt
	rcptLink, found := resp.Get(inv.Link())
	if !found {
		return nil, fmt.Errorf("no receipt for retrieve invocation")
	}

	bs, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(resp.Blocks()))
	if err != nil {
		return nil, fmt.Errorf("failed to create block store: %w", err)
	}

	rcpt, err := receipt.NewAnyReceipt(rcptLink, bs)
	if err != nil {
		return nil, fmt.Errorf("failed to read receipt: %w", err)
	}

	// Extract the retrieval URL from the response
	out, xerr := result.Unwrap(rcpt.Out())
	if xerr != nil {
		// Try to extract a better error message
		if errNode, isNode := xerr.(ipld.Node); isNode {
			if msgNode, err := errNode.LookupByString("message"); err == nil {
				if msg, err := msgNode.AsString(); err == nil {
					return nil, fmt.Errorf("retrieve failed: %s", msg)
				}
			}
			if nameNode, err := errNode.LookupByString("name"); err == nil {
				if name, err := nameNode.AsString(); err == nil {
					return nil, fmt.Errorf("retrieve failed: %s", name)
				}
			}
		}
		return nil, fmt.Errorf("retrieve failed: %v", xerr)
	}

	// The response should contain a presigned URL or direct content
	// Parse the response to get the URL and fetch from it
	outNode, ok := out.(ipld.Node)
	if !ok {
		return nil, fmt.Errorf("unexpected response type")
	}

	urlNode, err := outNode.LookupByString("url")
	if err != nil {
		return nil, fmt.Errorf("no url in retrieve response: %w", err)
	}

	retrieveURL, err := urlNode.AsString()
	if err != nil {
		return nil, fmt.Errorf("url is not a string: %w", err)
	}

	// Fetch from the presigned URL
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, retrieveURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpResp, err := c.cfg.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch from retrieve URL: %w", err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(httpResp.Body)
		return nil, fmt.Errorf("retrieve URL returned status %d: %s", httpResp.StatusCode, string(body))
	}

	data, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	return data, nil
}

// FetchBlobWithFallback tries direct Storacha retrieval first, falls back to gateway.
func (c *DelegatedClient) FetchBlobWithFallback(ctx context.Context, cidStr string, dlg delegation.Delegation) ([]byte, error) {
	// Try direct retrieval first (faster, more reliable)
	data, err := c.FetchBlobDirect(ctx, cidStr, dlg)
	if err == nil {
		c.logger.Debug("FetchBlobDirect succeeded", "cid", cidStr)
		return data, nil
	}

	c.logger.Debug("FetchBlobDirect failed, trying gateway fallback", "cid", cidStr, "error", err)

	// Fall back to public gateway
	return c.FetchBlobViaGateway(ctx, cidStr)
}

// FetchBlob retrieves data by CID.
// If a delegation is present in the context (via WithDelegation), uses direct Storacha
// retrieval (space/content/retrieve) with gateway fallback. This bypasses IPNI content
// routing which is currently broken due to the space/index/add Content field being
// disabled (see spaceIndexAdd comments and https://github.com/storacha/go-ucanto/pull/83).
// Without a delegation, falls back to gateway-only retrieval.
func (c *DelegatedClient) FetchBlob(ctx context.Context, cidStr string) ([]byte, error) {
	if dlg := GetDelegation(ctx); dlg != nil {
		return c.FetchBlobWithFallback(ctx, cidStr, dlg)
	}
	return c.FetchBlobViaGateway(ctx, cidStr)
}

// FetchBlobViaGateway retrieves data via IPFS gateway (old implementation).
func (c *DelegatedClient) FetchBlobViaGateway(ctx context.Context, cidStr string) ([]byte, error) {
	_, err := cid.Decode(cidStr)
	if err != nil {
		return nil, fmt.Errorf("invalid CID: %w", err)
	}

	url := fmt.Sprintf("%s/ipfs/%s", c.cfg.GatewayURL, cidStr)
	startTime := time.Now()
	c.logger.Debug("FetchBlob starting", "cid", cidStr)

	var lastErr error
	for attempt := 0; attempt < c.cfg.RetryAttempts; attempt++ {
		if attempt > 0 {
			delay := c.cfg.RetryDelay * time.Duration(1<<uint(attempt-1))
			c.logger.Debug("FetchBlob retry", "attempt", attempt+1, "delay", delay)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		attemptStart := time.Now()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			lastErr = err
			continue
		}

		resp, err := c.cfg.HTTPClient.Do(req)
		if err != nil {
			c.logger.Debug("FetchBlob attempt failed", "attempt", attempt+1, "duration", time.Since(attemptStart), "error", err)
			lastErr = err
			continue
		}

		if resp.StatusCode == http.StatusOK {
			data, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil {
				lastErr = err
				continue
			}
			c.logger.Debug("FetchBlob success", "cid", cidStr, "bytes", len(data), "duration", time.Since(startTime))
			return data, nil
		}
		resp.Body.Close()
		c.logger.Debug("FetchBlob attempt status", "attempt", attempt+1, "status", resp.StatusCode, "duration", time.Since(attemptStart))
		lastErr = fmt.Errorf("gateway returned status %d", resp.StatusCode)
	}

	c.logger.Debug("FetchBlob failed", "cid", cidStr, "duration", time.Since(startTime), "error", lastErr)
	return nil, fmt.Errorf("failed after %d attempts: %w", c.cfg.RetryAttempts, lastErr)
}

// RemoveBlob removes a blob from the customer's space using the provided delegation.
func (c *DelegatedClient) RemoveBlob(ctx context.Context, spaceDID string, digest []byte, dlg delegation.Delegation) error {
	// Create caveats with the blob digest
	caveats := RemoveCaveats{
		Digest: digest,
	}

	// Create capability
	capability := ucan.NewCapability(
		BlobRemoveAbility,
		c.space.String(),
		caveats,
	)

	// Create invocation with the provided delegation as proof
	proofs := []delegation.Proof{delegation.FromDelegation(dlg)}
	inv, err := invocation.Invoke(
		c.cfg.ServiceSigner,
		c.conn.ID(),
		capability,
		delegation.WithProof(proofs...),
	)
	if err != nil {
		return fmt.Errorf("failed to create invocation: %w", err)
	}

	// Execute the invocation
	resp, err := client.Execute(ctx, []invocation.Invocation{inv}, c.conn)
	if err != nil {
		return fmt.Errorf("failed to execute invocation: %w", err)
	}

	// Get the receipt
	rcptLink, found := resp.Get(inv.Link())
	if !found {
		return fmt.Errorf("no receipt found for invocation: %s", inv.Link())
	}

	// Create block store from response
	bs, err := blockstore.NewBlockStore(blockstore.WithBlocksIterator(resp.Blocks()))
	if err != nil {
		return fmt.Errorf("failed to create block store: %w", err)
	}

	// Read the receipt
	rcpt, err := receipt.NewAnyReceipt(rcptLink, bs)
	if err != nil {
		return fmt.Errorf("failed to read receipt: %w", err)
	}

	// Check result
	out := rcpt.Out()
	_, xerr := result.Unwrap(out)
	if xerr != nil {
		if errNode, isNode := xerr.(ipld.Node); isNode {
			if msgNode, err := errNode.LookupByString("message"); err == nil {
				if msg, err := msgNode.AsString(); err == nil {
					return fmt.Errorf("remove failed: %s", msg)
				}
			}
		}
		return fmt.Errorf("remove failed: %v", xerr)
	}

	return nil
}

// authFact is a fact builder for the retrieval auth delegation link.
type authFact struct {
	auth ipld.Link
}

func (a authFact) ToIPLD() (map[string]ipld.Node, error) {
	np := basicnode.Prototype.Link
	nb := np.NewBuilder()
	if err := nb.AssignLink(a.auth); err != nil {
		return nil, err
	}
	return map[string]ipld.Node{"retrievalAuth": nb.Build()}, nil
}
