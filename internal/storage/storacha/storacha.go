// Package storacha provides a Tessera storage driver that uses Storacha
// (decentralized storage network) as the backend.
package storacha

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	ufsio "github.com/ipfs/boxo/ipld/unixfs/io"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	format "github.com/ipfs/go-ipld-format"
	carv1 "github.com/ipld/go-car"
	"github.com/relves/ucanlog/internal/storage"
	"github.com/relves/ucanlog/internal/storage/storacha/uploadqueue"
	"github.com/storacha/go-libstoracha/blobindex"
	"github.com/transparency-dev/tessera"
)

// StartMode controls how storacha.New() interprets empty state.
type StartMode int

const (
	// ModeCreate indicates a brand-new log. Empty state is expected and correct.
	ModeCreate StartMode = iota

	// ModeResume indicates an existing log being restarted. Empty state means
	// the DB was wiped; New() returns an error instead of silently forking the log.
	ModeResume
)

// Config holds configuration for Storacha storage.
type Config struct {
	SpaceDID   string
	StateStore storage.StateStore
	LogDID     string
	GatewayURL string
	ServiceURL string
	Client     StorachaClient
	HTTPClient *http.Client

	// Mode controls fork-prevention behaviour. ModeCreate (default) allows empty
	// state; ModeResume treats empty state as a fatal error. See StartMode docs.
	Mode StartMode

	Logger *slog.Logger
}

// FlushReceipt holds the data needed to build a synchronous append receipt.
// It is populated by flushFn and can be read immediately after appender.Add returns.
type FlushReceipt struct {
	HeadCID    string
	TreeSize   uint64
	Checkpoint []byte // raw signed checkpoint note
}

// Storage implements tessera.Driver for Storacha-backed storage.
type Storage struct {
	mu          sync.Mutex
	cfg         Config
	clientRef   *clientRef
	objStore    *objStore
	uploadQueue *uploadqueue.Manager
	logger      *slog.Logger

	receiptMu     sync.RWMutex
	lastReceipt   FlushReceipt
}

// New creates a new Storacha storage driver.
func New(ctx context.Context, cfg Config) (tessera.Driver, error) {
	if cfg.SpaceDID == "" {
		return nil, fmt.Errorf("SpaceDID is required")
	}
	if cfg.StateStore == nil {
		return nil, fmt.Errorf("StateStore is required")
	}
	if cfg.LogDID == "" {
		return nil, fmt.Errorf("LogDID is required")
	}
	if cfg.GatewayURL == "" {
		cfg.GatewayURL = "https://ipfs.w3s.link"
	}
	if cfg.ServiceURL == "" {
		cfg.ServiceURL = "https://up.storacha.network"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	// Register the block-position extractor with the upload queue package once.
	uploadqueue.SetExtractBlockPositionsFn(func(data []byte) (map[cid.Cid]blobindex.Position, error) {
		return ExtractBlockPositions(data)
	})

	client := cfg.Client
	if client == nil {
		client = &placeholderClient{}
	}
	ref := newClientRef(client)
	objStore := newObjStore(cfg.Logger)

	// Cold-start recovery: prefer latest_head_car (local, no network) over gateway fetch.
	restored := false
	if headCID, _, carData, err := cfg.StateStore.GetLatestHeadCAR(ctx, cfg.LogDID); err == nil && headCID != "" && len(carData) > 0 {
		stateMap, parseErr := parseStateCAR(ctx, bytes.NewReader(carData))
		if parseErr != nil {
			cfg.Logger.Warn("failed to parse latest_head_car; will fall back to gateway", "headCID", headCID, "error", parseErr)
		} else {
			finalizedCIDs, manifestErr := extractManifest(stateMap)
			if manifestErr != nil {
				cfg.Logger.Warn("failed to parse manifest in latest_head_car; treating all as embedded", "error", manifestErr)
			}
			objStore.Load(stateMap)
			if finalizedCIDs != nil {
				objStore.LoadFinalizedCIDs(finalizedCIDs)
			}
			// Restore tree_state from the checkpoint embedded in the CAR, so a
			// crash that wiped tree_state but left latest_head_car intact doesn't
			// cause a silent fork on next append.
			if cpData, ok := stateMap["checkpoint"]; ok {
				if cpSize, cpRoot, cpErr := parseCheckpointUnsafe(cpData); cpErr == nil {
					if setErr := cfg.StateStore.SetTreeState(ctx, cfg.LogDID, cpSize, cpRoot); setErr != nil {
						cfg.Logger.Warn("failed to restore tree_state from latest_head_car checkpoint", "error", setErr)
					}
				} else {
					cfg.Logger.Warn("failed to parse checkpoint from latest_head_car; tree_state not restored", "error", cpErr)
				}
			}
			cfg.Logger.Info("restored in-memory state from latest_head_car",
				"headCID", headCID, "objects", len(stateMap), "finalizedBundles", len(finalizedCIDs))
			restored = true
		}
	}

	// Fallback: gateway fetch (first boot or latest_head_car missing/corrupt).
	if !restored {
		headCID, _, err := cfg.StateStore.GetHead(ctx, cfg.LogDID)
		if err != nil {
			return nil, fmt.Errorf("failed to read head from state store: %w", err)
		}
		if headCID != "" {
			stateMap, fetchErr := fetchAndParseStateCAR(ctx, cfg.HTTPClient, cfg.GatewayURL, headCID)
			if fetchErr != nil {
				// A known headCID means the log had prior data. Starting empty here
				// would silently fork the log (new entries at index 0 colliding with
				// existing ones). Surface the error so the operator can act.
				return nil, fmt.Errorf("cannot recover log state: gateway fetch of head CAR %s failed: %w", headCID, fetchErr)
			} else {
				finalizedCIDs, manifestErr := extractManifest(stateMap)
				if manifestErr != nil {
					cfg.Logger.Warn("failed to parse manifest from gateway; treating all as embedded", "error", manifestErr)
				}
				objStore.Load(stateMap)
				if finalizedCIDs != nil {
					objStore.LoadFinalizedCIDs(finalizedCIDs)
				}
				cfg.Logger.Info("restored in-memory state from gateway CAR",
					"headCID", headCID, "objects", len(stateMap), "finalizedBundles", len(finalizedCIDs))
				// Restore tree_state from the checkpoint embedded in the CAR so that
				// the coordinator picks up the correct log size instead of starting
				// from 0, which would silently fork the log.
				if cpData, ok := stateMap["checkpoint"]; ok {
					if cpSize, cpRoot, cpErr := parseCheckpointUnsafe(cpData); cpErr == nil {
						if setErr := cfg.StateStore.SetTreeState(ctx, cfg.LogDID, cpSize, cpRoot); setErr != nil {
							cfg.Logger.Warn("failed to persist restored tree state", "error", setErr)
						}
					} else {
						cfg.Logger.Warn("failed to parse checkpoint from gateway CAR; tree_state not restored", "error", cpErr)
					}
				}
			}
		}
	}

	// Fork-prevention guard: ModeResume with no recovered state means the DB was
	// wiped. Refuse to start rather than silently fork the log from index 0.
	if !restored && cfg.Mode == ModeResume {
		// Check whether the gateway fallback was attempted (headCID known but fetch
		// failed — that path already returned an error above). We only reach here
		// when the gateway fallback was never entered (headCID was ""), which means
		// there is no state anywhere. Hard error for ModeResume.
		headCID, _, err := cfg.StateStore.GetHead(ctx, cfg.LogDID)
		if err == nil && headCID == "" {
			return nil, fmt.Errorf(
				"cannot resume log %s: no head CID found in state store — "+
					"DB may have been wiped; call storacha.Recover() with a known headCID to restore state",
				cfg.LogDID,
			)
		}
	}

	// Build upload queue manager if the StateStore also implements QueueStore.
	var uq *uploadqueue.Manager
	if qs, ok := cfg.StateStore.(storage.QueueStore); ok {
		uq = uploadqueue.New(uploadqueue.Config{
			Store:    qs,
			Uploader: ref, // clientRef implements StorachaUploader via forwarding
			Logger:   cfg.Logger,
		})
		// Use context.Background() so workers live for the process lifetime,
		// not for the duration of the caller's (often short-lived) ctx.
		// Call Stop() to shut down gracefully.
		uq.Start(context.Background())
	}

	return &Storage{
		cfg:         cfg,
		clientRef:   ref,
		objStore:    objStore,
		uploadQueue: uq,
		logger:      cfg.Logger,
	}, nil
}



// Recover fetches the head CAR for the given headCID from the IPFS gateway,
// parses the embedded checkpoint to extract tree size and root hash, and writes
// the restored state (CAR bytes, tree_state, CID index) into cfg.StateStore.
//
// Use this after a total DB wipeout: once Recover() succeeds, the normal
// ModeResume startup path will find the restored state and continue the log
// without forking.
func Recover(ctx context.Context, cfg Config, headCID string) error {
	if cfg.StateStore == nil {
		return fmt.Errorf("StateStore is required")
	}
	if cfg.LogDID == "" {
		return fmt.Errorf("LogDID is required")
	}
	if headCID == "" {
		return fmt.Errorf("headCID is required")
	}
	if cfg.GatewayURL == "" {
		cfg.GatewayURL = "https://ipfs.w3s.link"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	cfg.Logger.Info("recovering log state from gateway", "logDID", cfg.LogDID, "headCID", headCID)

	// Fetch once; parse in-memory so we don't need a second round-trip.
	carData, err := fetchCARBytes(ctx, cfg.HTTPClient, cfg.GatewayURL, headCID)
	if err != nil {
		return fmt.Errorf("recover: gateway fetch of head CAR %s failed: %w", headCID, err)
	}

	stateMap, err := parseStateCAR(ctx, bytes.NewReader(carData))
	if err != nil {
		return fmt.Errorf("recover: failed to parse head CAR %s: %w", headCID, err)
	}

	// Extract tree state from the checkpoint embedded in the CAR.
	cpData, ok := stateMap["checkpoint"]
	if !ok {
		return fmt.Errorf("recover: CAR for %s contains no checkpoint", headCID)
	}
	cpSize, cpRoot, err := parseCheckpointUnsafe(cpData)
	if err != nil {
		return fmt.Errorf("recover: failed to parse checkpoint: %w", err)
	}

	// Persist tree state so ModeResume startup finds a valid size/root.
	if err := cfg.StateStore.SetTreeState(ctx, cfg.LogDID, cpSize, cpRoot); err != nil {
		return fmt.Errorf("recover: failed to persist tree state: %w", err)
	}

	// Persist CAR bytes so local cold-start avoids a gateway round-trip.
	if err := cfg.StateStore.SetLatestHeadCAR(ctx, cfg.LogDID, cpSize, headCID, carData); err != nil {
		cfg.Logger.Warn("recover: failed to persist latest_head_car", "error", err)
	}

	cfg.Logger.Info("recovery complete", "logDID", cfg.LogDID, "headCID", headCID, "treeSize", cpSize)
	return nil
}

// parseCheckpointUnsafe parses the newline-delimited checkpoint format used by
// Tessera without verifying the note signature. It returns (size, rootHash).
// Format: "<origin>\n<size>\n<base64-root>\n<signature>\n"
func parseCheckpointUnsafe(raw []byte) (uint64, []byte, error) {
	parts := strings.SplitN(string(raw), "\n", 4)
	if len(parts) < 3 {
		return 0, nil, fmt.Errorf("invalid checkpoint: too few lines")
	}
	size, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return 0, nil, fmt.Errorf("invalid checkpoint size %q: %w", parts[1], err)
	}
	root, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return 0, nil, fmt.Errorf("invalid checkpoint root hash: %w", err)
	}
	return size, root, nil
}

func fetchAndParseStateCAR(ctx context.Context, httpClient *http.Client, gatewayURL, headCID string) (map[string][]byte, error) {
	data, err := fetchCARBytes(ctx, httpClient, gatewayURL, headCID)
	if err != nil {
		return nil, fmt.Errorf("fetch head CAR: %w", err)
	}
	return parseStateCAR(ctx, bytes.NewReader(data))
}

// fetchCARBytes fetches the raw CAR bytes for headCID from the IPFS gateway.
func fetchCARBytes(ctx context.Context, httpClient *http.Client, gatewayURL, headCID string) ([]byte, error) {
	url := fmt.Sprintf("%s/ipfs/%s?format=car", gatewayURL, headCID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch failed with status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func parseStateCAR(ctx context.Context, reader io.Reader) (map[string][]byte, error) {
	ds := dssync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	header, err := carv1.LoadCar(ctx, bs, reader)
	if err != nil {
		return nil, fmt.Errorf("load CAR into blockstore: %w", err)
	}
	if len(header.Roots) == 0 {
		return nil, fmt.Errorf("state CAR has no roots")
	}

	bserv := blockservice.New(bs, offline.Exchange(bs))
	dagService := merkledag.NewDAGService(bserv)

	stateMap := make(map[string][]byte)
	if err := walkTree(ctx, dagService, header.Roots[0], "", stateMap); err != nil {
		return nil, err
	}
	return stateMap, nil
}

func walkTree(ctx context.Context, dagService format.DAGService, c cid.Cid, prefix string, out map[string][]byte) error {
	node, err := dagService.Get(ctx, c)
	if err != nil {
		// Block not in CAR — expected for linked (finalized) bundles whose data is
		// stored externally. Their path→CID mapping is recovered from _manifest.json.
		return nil
	}

	dir, err := ufsio.NewDirectoryFromNode(dagService, node)
	if err != nil {
		out[prefix] = append([]byte(nil), node.RawData()...)
		return nil
	}

	links, err := dir.Links(ctx)
	if err != nil {
		return fmt.Errorf("read directory links: %w", err)
	}
	for _, link := range links {
		childPath := link.Name
		if prefix != "" {
			childPath = prefix + "/" + link.Name
		}
		if err := walkTree(ctx, dagService, link.Cid, childPath, out); err != nil {
			return err
		}
	}
	return nil
}

// extractManifest reads _manifest.json from the parsed state map, decodes it into
// a map[string]cid.Cid, and removes it from stateMap. Returns nil if no manifest.
func extractManifest(stateMap map[string][]byte) (map[string]cid.Cid, error) {
	manifestData, ok := stateMap["_manifest.json"]
	if !ok {
		return nil, nil
	}
	delete(stateMap, "_manifest.json")

	var manifest map[string]string
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, fmt.Errorf("unmarshal manifest: %w", err)
	}

	result := make(map[string]cid.Cid, len(manifest))
	for path, cidStr := range manifest {
		c, err := cid.Decode(cidStr)
		if err != nil {
			return nil, fmt.Errorf("decode manifest CID for %s: %w", path, err)
		}
		result[path] = c
	}
	return result, nil
}

// placeholderClient is used when no client is provided.
type placeholderClient struct{}

func (p *placeholderClient) FetchBlob(ctx context.Context, cid string) ([]byte, error) {
	return nil, fmt.Errorf("no Storacha client configured: provide Config.Client")
}

func (p *placeholderClient) UploadFinalizedBlob(ctx context.Context, data []byte) (string, error) {
	return "", fmt.Errorf("no Storacha client configured: provide Config.Client")
}

func (p *placeholderClient) UploadFullStateCAR(ctx context.Context, carData []byte, rootCID cid.Cid, positions map[cid.Cid]blobindex.Position) (string, error) {
	return "", fmt.Errorf("no Storacha client configured: provide Config.Client")
}

// LastFlushReceipt returns the receipt data from the most recent flush.
// Valid immediately after appender.Add() returns.
func (s *Storage) LastFlushReceipt() FlushReceipt {
	s.receiptMu.RLock()
	defer s.receiptMu.RUnlock()
	return s.lastReceipt
}

// Close shuts down background workers (upload queue manager).
func (s *Storage) Close() {
	if s.uploadQueue != nil {
		s.uploadQueue.Stop()
	}
}

// SetClient updates the client used by this storage driver.
func (s *Storage) SetClient(client StorachaClient) {
	s.clientRef.Set(client)
}




