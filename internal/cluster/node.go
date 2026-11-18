package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cindersearch/cindersearch/internal/cache"
	"github.com/cindersearch/cindersearch/internal/checksum"
	"github.com/cindersearch/cindersearch/internal/query"
	"github.com/cindersearch/cindersearch/internal/s3store"
	"github.com/cindersearch/cindersearch/internal/segment"
	"github.com/cindersearch/cindersearch/internal/sharding"
	"github.com/cindersearch/cindersearch/internal/wal"
)

// RetrievalMode controls how _search returns document bodies.
type RetrievalMode string

const (
	// RetrievalDropIn returns standard Elasticsearch _source payloads for full client compatibility.
	RetrievalDropIn RetrievalMode = "dropin"
	// RetrievalPresigned omits _source on S3-committed docs and returns zero-hop Presigned S3 Range URLs.
	RetrievalPresigned RetrievalMode = "presigned"
	// RetrievalHybrid returns _s3_pointer and also inlines _source when hot in local NVMe cache.
	RetrievalHybrid RetrievalMode = "hybrid"
)

// NodeConfig configures a CinderSearch cluster node.
type NodeConfig struct {
	NodeID            string
	Roles             []string // Supported: data (default), coordinator, reader
	AdvertiseAddress  string
	NVMeDir           string
	NVMeMaxBytes      int64
	FlushInterval     time.Duration
	ReplicateInterval time.Duration
	ScrubInterval     time.Duration
	S3                *s3store.Client
	Peers             []sharding.NodeInfo
}

// IndexSettings mirrors Elasticsearch index creation settings.
type IndexSettings struct {
	NumberOfShards   int `json:"number_of_shards"`
	NumberOfReplicas int `json:"number_of_replicas"`
}

// IndexMetadata stores cluster-wide configuration for an Elasticsearch index.
type IndexMetadata struct {
	Name      string         `json:"name"`
	Settings  IndexSettings  `json:"settings"`
	Mappings  map[string]any `json:"mappings,omitempty"`
	CreatedAt int64          `json:"created_at"`
}

// S3Pointer provides clients with a zero-hop direct S3 HTTP Range pointer + CRC32C checksum.
type S3Pointer struct {
	URL        string `json:"url"`
	Range      string `json:"range"`
	Offset     int64  `json:"offset"`
	Length     int64  `json:"length"`
	CRC32C     string `json:"crc32c"`
	Bucket     string `json:"bucket"`
	Key        string `json:"key"`
	NVMeCached bool   `json:"nvme_cached"`
}

// ReplicateBatchRequest is sent asynchronously from primary shards to replica peers.
type ReplicateBatchRequest struct {
	IndexName string       `json:"index_name"`
	ShardID   int          `json:"shard_id"`
	FromNode  string       `json:"from_node"`
	Records   []wal.Record `json:"records"`
}

// ReplicateBatchResponse reports the replica's persisted high-watermark sequence.
type ReplicateBatchResponse struct {
	AckSeq uint64 `json:"ack_seq"`
}

// ShardSearchRequest executes a Query DSL evaluation on a single target shard.
type ShardSearchRequest struct {
	IndexName   string         `json:"index_name"`
	ShardID     int            `json:"shard_id"`
	Query       map[string]any `json:"query"`
	Mode        RetrievalMode  `json:"mode"`
	NeedAggs    bool           `json:"need_aggs"`
	NeedSort    bool           `json:"need_sort"`
	DeferSource bool           `json:"defer_source,omitempty"`
}

// ShardSearchHit carries a shard-local matched hit back to the coordinator.
type ShardSearchHit struct {
	Index      string            `json:"index"`
	ShardID    int               `json:"shard_id"`
	DocID      string            `json:"doc_id"`
	Score      float64           `json:"score"`
	Seq        uint64            `json:"seq"`
	DocsKey    string            `json:"docs_key,omitempty"`
	Offset     int64             `json:"offset"`
	Length     int64             `json:"length"`
	CRC32C     uint32            `json:"crc32c"`
	NVMeCached bool              `json:"nvme_cached"`
	RawJSON    json.RawMessage   `json:"raw_json,omitempty"`
	DocVals    segment.DocValues `json:"doc_vals,omitempty"`
}

// Node represents a single CinderSearch server participating in the cluster ring.
type Node struct {
	cfg        NodeConfig
	ring       *sharding.Ring
	nvmeCache  *cache.NVMeCache
	s3         *s3store.Client
	httpClient *http.Client

	mu      sync.RWMutex
	indices map[string]IndexMetadata
	shards  map[string]map[int]*Shard

	repSuccess atomic.Int64
	repErrors  atomic.Int64
	flushes    atomic.Int64
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// NewNode initializes a CinderSearch node, its NVMe cache, cluster ring, and role-appropriate background workers.
func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.NodeID == "" {
		cfg.NodeID = "node-1"
	}
	roles, err := sharding.NormalizeRoles(cfg.Roles)
	if err != nil {
		return nil, err
	}
	cfg.Roles = roles

	if cfg.NVMeDir == "" {
		cfg.NVMeDir = filepath.Join(os.TempDir(), "cindersearch-"+cfg.NodeID)
	}
	if cfg.NVMeMaxBytes <= 0 {
		cfg.NVMeMaxBytes = 256 * 1024 * 1024
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.ReplicateInterval <= 0 {
		cfg.ReplicateInterval = 40 * time.Millisecond
	}
	if cfg.ScrubInterval <= 0 {
		cfg.ScrubInterval = 5 * time.Second
	}

	nvmeCache, err := cache.NewNVMeCache(filepath.Join(cfg.NVMeDir, "cache"), cfg.NVMeMaxBytes)
	if err != nil {
		return nil, err
	}

	peers := make([]sharding.NodeInfo, 0, len(cfg.Peers)+1)
	selfIncluded := false
	for _, p := range cfg.Peers {
		if p.ID == cfg.NodeID {
			if len(p.Roles) == 0 {
				p.Roles = cfg.Roles
			}
			selfIncluded = true
		}
		peers = append(peers, p)
	}
	if !selfIncluded {
		peers = append(peers, sharding.NodeInfo{
			ID:      cfg.NodeID,
			Address: cfg.AdvertiseAddress,
			Roles:   cfg.Roles,
		})
	}

	n := &Node{
		cfg:       cfg,
		ring:      sharding.NewRing(64, peers...),
		nvmeCache: nvmeCache,
		s3:        cfg.S3,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		indices: make(map[string]IndexMetadata),
		shards:  make(map[string]map[int]*Shard),
		stopCh:  make(chan struct{}),
	}

	if n.HasRole(sharding.RoleData) {
		n.wg.Add(2)
		go n.replicationLoop()
		go n.flushLoop()
	}
	if n.HasRole(sharding.RoleData) || n.HasRole(sharding.RoleReader) {
		n.wg.Add(1)
		go n.scrubLoop()
	}

	return n, nil
}

// ID returns the node identifier.
func (n *Node) ID() string {
	return n.cfg.NodeID
}

// Roles returns the normalized roles assigned to this node.
func (n *Node) Roles() []string {
	return append([]string(nil), n.cfg.Roles...)
}

// HasRole reports whether this node holds the specified role.
func (n *Node) HasRole(role string) bool {
	for _, r := range n.cfg.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// Ring returns the node's consistent hash ring.
func (n *Node) Ring() *sharding.Ring {
	return n.ring
}

// NVMeCache returns the node's local NVMe cache instance.
func (n *Node) NVMeCache() *cache.NVMeCache {
	return n.nvmeCache
}

// S3 returns the configured S3 client.
func (n *Node) S3() *s3store.Client {
	return n.s3
}

// UpdatePeers updates the consistent hash ring membership.
func (n *Node) UpdatePeers(peers []sharding.NodeInfo) {
	for _, p := range peers {
		n.ring.AddNode(p)
	}
}

// Close stops background workers and closes open shard WALs.
func (n *Node) Close() error {
	close(n.stopCh)
	n.wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	for _, shardMap := range n.shards {
		for _, sh := range shardMap {
			_ = sh.Close()
		}
	}
	return nil
}

// CreateIndex registers an index across the cluster and opens its primary/replica shards locally.
func (n *Node) CreateIndex(ctx context.Context, meta IndexMetadata, broadcast bool) error {
	if meta.Name == "" {
		return fmt.Errorf("index name is required")
	}
	if meta.Settings.NumberOfShards <= 0 {
		meta.Settings.NumberOfShards = 3
	}
	if meta.Settings.NumberOfReplicas < 0 {
		meta.Settings.NumberOfReplicas = 1
	}
	if meta.CreatedAt == 0 {
		meta.CreatedAt = time.Now().UTC().UnixMilli()
	}

	if err := n.ApplyIndexMetadata(ctx, meta); err != nil {
		return err
	}

	if broadcast {
		for _, peer := range n.ring.Nodes() {
			if peer.ID == n.cfg.NodeID || peer.Address == "" {
				continue
			}
			_ = n.postJSON(ctx, baseURL(peer.Address)+"/_internal/index/sync", meta, nil)
		}
	}
	return nil
}

// ApplyIndexMetadata registers index metadata on this node and opens role-appropriate shards:
// - data: WAL-backed primary/replica shards owned by this node
// - reader: stateless read-only S3 shard views for all shards (zero WAL)
// - coordinator: metadata only (zero local shards)
func (n *Node) ApplyIndexMetadata(ctx context.Context, meta IndexMetadata) error {
	placements, err := n.ring.AllPlacements(meta.Name, meta.Settings.NumberOfShards, meta.Settings.NumberOfReplicas)
	if err != nil {
		return err
	}

	n.mu.Lock()
	n.indices[meta.Name] = meta
	if _, ok := n.shards[meta.Name]; !ok {
		n.shards[meta.Name] = make(map[int]*Shard)
	}
	n.mu.Unlock()

	if n.HasRole(sharding.RoleData) {
		for _, p := range placements {
			isPrimary := p.Primary.ID == n.cfg.NodeID
			isReplica := false
			for _, rep := range p.Replicas {
				if rep.ID == n.cfg.NodeID {
					isReplica = true
					break
				}
			}
			if !isPrimary && !isReplica {
				continue
			}

			n.mu.RLock()
			_, exists := n.shards[meta.Name][p.ShardID]
			n.mu.RUnlock()
			if exists {
				continue
			}

			sh, err := OpenShard(ctx, meta.Name, p.ShardID, isPrimary, n.cfg.NVMeDir, n.nvmeCache, n.s3)
			if err != nil {
				return err
			}
			n.mu.Lock()
			n.shards[meta.Name][p.ShardID] = sh
			n.mu.Unlock()
		}
		return nil
	}

	if n.HasRole(sharding.RoleReader) {
		for _, p := range placements {
			n.mu.RLock()
			_, exists := n.shards[meta.Name][p.ShardID]
			n.mu.RUnlock()
			if exists {
				continue
			}
			sh, err := OpenReadOnlyShard(ctx, meta.Name, p.ShardID, n.nvmeCache, n.s3)
			if err != nil {
				return err
			}
			n.mu.Lock()
			n.shards[meta.Name][p.ShardID] = sh
			n.mu.Unlock()
		}
	}
	return nil
}

// DeleteIndex deletes an index locally and across peers.
func (n *Node) DeleteIndex(ctx context.Context, indexName string, broadcast bool) error {
	n.mu.Lock()
	delete(n.indices, indexName)
	shardMap := n.shards[indexName]
	delete(n.shards, indexName)
	n.mu.Unlock()

	for _, sh := range shardMap {
		_ = sh.Close()
	}
	if broadcast {
		for _, peer := range n.ring.Nodes() {
			if peer.ID == n.cfg.NodeID || peer.Address == "" {
				continue
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodDelete, baseURL(peer.Address)+"/_internal/index/sync?index="+indexName, nil)
			if err == nil {
				resp, err := n.httpClient.Do(req)
				if err == nil {
					_ = resp.Body.Close()
				}
			}
		}
	}
	return nil
}

// GetIndex returns index metadata if it exists.
func (n *Node) GetIndex(indexName string) (IndexMetadata, bool) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	meta, ok := n.indices[indexName]
	return meta, ok
}

// ListIndices returns all known index definitions sorted by name.
func (n *Node) ListIndices() []IndexMetadata {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]IndexMetadata, 0, len(n.indices))
	for _, meta := range n.indices {
		out = append(out, meta)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// EnsureIndex auto-creates an index with default settings on first write (matching ES behavior).
func (n *Node) EnsureIndex(ctx context.Context, indexName string) (IndexMetadata, error) {
	if meta, ok := n.GetIndex(indexName); ok {
		return meta, nil
	}
	meta := IndexMetadata{
		Name: indexName,
		Settings: IndexSettings{
			NumberOfShards:   3,
			NumberOfReplicas: 1,
		},
	}
	if err := n.CreateIndex(ctx, meta, true); err != nil {
		return IndexMetadata{}, err
	}
	return meta, nil
}

// IndexDoc routes a document write to its primary shard node (local or remote),
// computes CRC32C, writes to NVMe WAL, and returns immediately while async replication runs.
func (n *Node) IndexDoc(ctx context.Context, indexName, docID string, rawJSON []byte, routing string) (string, int, uint32, uint64, error) {
	meta, err := n.EnsureIndex(ctx, indexName)
	if err != nil {
		return "", 0, 0, 0, err
	}
	if docID == "" {
		docID = generateDocID()
	}
	if routing == "" {
		routing = docID
	}

	shardID := sharding.ShardForKey(indexName, routing, meta.Settings.NumberOfShards)
	placement, err := n.ring.PlacementForShard(indexName, shardID, meta.Settings.NumberOfReplicas)
	if err != nil {
		return "", 0, 0, 0, err
	}

	if placement.Primary.ID == n.cfg.NodeID {
		n.mu.RLock()
		sh := n.shards[indexName][shardID]
		n.mu.RUnlock()
		if sh == nil {
			return "", 0, 0, 0, fmt.Errorf("primary shard %s[%d] not initialized on %s", indexName, shardID, n.cfg.NodeID)
		}
		rec, err := sh.IndexLocal(docID, rawJSON)
		if err != nil {
			return "", 0, 0, 0, err
		}
		return docID, shardID, checksum.Sum32C(rawJSON), rec.Seq, nil
	}

	// Forward to remote primary node.
	var resp struct {
		ID      string `json:"_id"`
		ShardID int    `json:"_shard"`
		CRC32C  uint32 `json:"_crc32c"`
		Seq     uint64 `json:"_seq_no"`
	}
	target := fmt.Sprintf("%s/%s/_doc/%s?routing=%s", baseURL(placement.Primary.Address), indexName, docID, routing)
	if err := n.putJSONRaw(ctx, target, rawJSON, &resp); err != nil {
		return "", 0, 0, 0, err
	}
	return docID, shardID, resp.CRC32C, resp.Seq, nil
}

// DeleteDoc routes a delete tombstone to the primary shard node.
func (n *Node) DeleteDoc(ctx context.Context, indexName, docID, routing string) (int, uint64, error) {
	meta, ok := n.GetIndex(indexName)
	if !ok {
		return 0, 0, fmt.Errorf("index_not_found_exception: %s", indexName)
	}
	if routing == "" {
		routing = docID
	}
	shardID := sharding.ShardForKey(indexName, routing, meta.Settings.NumberOfShards)
	placement, err := n.ring.PlacementForShard(indexName, shardID, meta.Settings.NumberOfReplicas)
	if err != nil {
		return 0, 0, err
	}

	if placement.Primary.ID == n.cfg.NodeID {
		n.mu.RLock()
		sh := n.shards[indexName][shardID]
		n.mu.RUnlock()
		if sh == nil {
			return 0, 0, fmt.Errorf("shard %s[%d] missing", indexName, shardID)
		}
		rec, err := sh.DeleteLocal(docID)
		if err != nil {
			return 0, 0, err
		}
		return shardID, rec.Seq, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, fmt.Sprintf("%s/%s/_doc/%s?routing=%s", baseURL(placement.Primary.Address), indexName, docID, routing), nil)
	if err != nil {
		return 0, 0, err
	}
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return 0, 0, fmt.Errorf("remote delete failed (%d)", resp.StatusCode)
	}
	return shardID, 0, nil
}

// GetDoc fetches a document by _id from its owning shard (primary or replica).
func (n *Node) GetDoc(ctx context.Context, indexName, docID, routing string) ([]byte, bool, uint32, *S3Pointer, error) {
	meta, ok := n.GetIndex(indexName)
	if !ok {
		return nil, false, 0, nil, fmt.Errorf("index_not_found_exception: %s", indexName)
	}
	if routing == "" {
		routing = docID
	}
	shardID := sharding.ShardForKey(indexName, routing, meta.Settings.NumberOfShards)
	placement, err := n.ring.PlacementForShard(indexName, shardID, meta.Settings.NumberOfReplicas)
	if err != nil {
		return nil, false, 0, nil, err
	}

	if placement.Primary.ID == n.cfg.NodeID {
		return n.getLocalDoc(ctx, indexName, shardID, docID)
	}

	// Check if this node holds a WAL-backed replica locally first for fast reads
	// (skip readOnly shards here so NRT reads hit the primary WAL before S3 flush).
	n.mu.RLock()
	localReplica := n.shards[indexName][shardID]
	n.mu.RUnlock()
	if localReplica != nil && !localReplica.readOnly {
		return n.getLocalDoc(ctx, indexName, shardID, docID)
	}

	// Forward to primary.
	url := fmt.Sprintf("%s/%s/_doc/%s?routing=%s", baseURL(placement.Primary.Address), indexName, docID, routing)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false, 0, nil, err
	}
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return nil, false, 0, nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, 0, nil, nil
	}
	var parsed struct {
		Found     bool            `json:"found"`
		Source    json.RawMessage `json:"_source"`
		CRC32C    string          `json:"_crc32c"`
		S3Pointer *S3Pointer      `json:"_s3_pointer,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return nil, false, 0, nil, err
	}
	var crc uint32
	if parsed.CRC32C != "" {
		if v, err := strconv.ParseUint(parsed.CRC32C, 16, 32); err == nil {
			crc = uint32(v)
		}
	}
	if crc == 0 && len(parsed.Source) > 0 {
		crc = checksum.Sum32C(parsed.Source)
	}
	return parsed.Source, parsed.Found, crc, parsed.S3Pointer, nil
}

func (n *Node) getLocalDoc(ctx context.Context, indexName string, shardID int, docID string) ([]byte, bool, uint32, *S3Pointer, error) {
	n.mu.RLock()
	sh := n.shards[indexName][shardID]
	n.mu.RUnlock()
	if sh == nil {
		return nil, false, 0, nil, nil
	}
	raw, found, crc, dm, seg, err := sh.GetDocByID(ctx, docID)
	if err != nil || !found {
		return nil, found, 0, nil, err
	}
	var ptr *S3Pointer
	if seg != nil && seg.DocsObjectKey != "" && n.s3 != nil {
		if u, err := n.s3.PresignGetURL(seg.DocsObjectKey, 5*time.Minute); err == nil {
			ptr = &S3Pointer{
				URL:        u,
				Range:      fmt.Sprintf("bytes=%d-%d", dm.Offset, dm.Offset+dm.Length-1),
				Offset:     dm.Offset,
				Length:     dm.Length,
				CRC32C:     checksum.FormatHex(dm.CRC32C),
				Bucket:     n.s3.Bucket(),
				Key:        seg.DocsObjectKey,
				NVMeCached: sh.IsDocsCachedOnNVMe(seg.DocsObjectKey),
			}
		}
	}
	return raw, true, crc, ptr, nil
}

// RefreshIndex refreshes local NRT memtables and optionally flushes buffered writes to S3.
func (n *Node) RefreshIndex(ctx context.Context, indexName string, flushToS3 bool, broadcast bool) error {
	n.mu.RLock()
	shardMap := n.shards[indexName]
	var localShards []*Shard
	for _, sh := range shardMap {
		localShards = append(localShards, sh)
	}
	n.mu.RUnlock()

	for _, sh := range localShards {
		if err := sh.Refresh(); err != nil {
			return err
		}
		if flushToS3 && sh.isPrimary && n.s3 != nil {
			if _, err := sh.FlushToS3(ctx); err != nil {
				return err
			}
			n.flushes.Add(1)
		}
	}

	if broadcast {
		for _, peer := range n.ring.Nodes() {
			if peer.ID == n.cfg.NodeID || peer.Address == "" {
				continue
			}
			u := fmt.Sprintf("%s/_internal/refresh?index=%s&flush=%t", baseURL(peer.Address), indexName, flushToS3)
			_ = n.postJSON(ctx, u, map[string]any{}, nil)
		}
	}
	return nil
}

// ApplyReplicaBatch applies incoming async WAL frames on a replica node.
func (n *Node) ApplyReplicaBatch(ctx context.Context, req ReplicateBatchRequest) (uint64, error) {
	if _, ok := n.GetIndex(req.IndexName); !ok {
		return 0, fmt.Errorf("unknown index %s on replica %s", req.IndexName, n.cfg.NodeID)
	}
	n.mu.RLock()
	sh := n.shards[req.IndexName][req.ShardID]
	n.mu.RUnlock()
	if sh == nil {
		return 0, fmt.Errorf("replica shard %s[%d] not found on %s", req.IndexName, req.ShardID, n.cfg.NodeID)
	}
	return sh.ApplyReplicaRecords(req.Records)
}

// Search executes a distributed Elasticsearch Query DSL request across all shards of indexNames.
func (n *Node) Search(ctx context.Context, indexNames []string, req query.SearchRequest, mode RetrievalMode) (map[string]any, error) {
	return n.SearchWithPreference(ctx, indexNames, req, mode, false)
}

// SearchWithPreference executes distributed search, optionally routing shard reads to stateless
// reader-role nodes first (falling back to data nodes if no readers are available).
func (n *Node) SearchWithPreference(ctx context.Context, indexNames []string, req query.SearchRequest, mode RetrievalMode, preferReader bool) (map[string]any, error) {
	start := time.Now()
	if mode == "" {
		mode = RetrievalDropIn
	}
	if len(indexNames) == 0 {
		for _, m := range n.ListIndices() {
			indexNames = append(indexNames, m.Name)
		}
	}

	aggsSpec := req.MergedAggs()
	sortSpecs := query.ParseSortSpecs(req.Sort)
	sourceSpec := query.ParseSourceSpec(req.Source)
	needSort := len(sortSpecs) > 0 && !(len(sortSpecs) == 1 && sortSpecs[0].Field == "_score")
	needAggs := len(aggsSpec) > 0

	var targetShards []struct {
		index string
		p     sharding.Placement
	}
	for _, idxName := range indexNames {
		meta, ok := n.GetIndex(idxName)
		if !ok {
			return nil, fmt.Errorf("index_not_found_exception: %s", idxName)
		}
		placements, err := n.ring.AllPlacements(idxName, meta.Settings.NumberOfShards, meta.Settings.NumberOfReplicas)
		if err != nil {
			return nil, err
		}
		for _, p := range placements {
			targetShards = append(targetShards, struct {
				index string
				p     sharding.Placement
			}{index: idxName, p: p})
		}
	}

	type shardResult struct {
		hits []ShardSearchHit
		err  error
	}
	resCh := make(chan shardResult, len(targetShards))
	var wg sync.WaitGroup

	for _, ts := range targetShards {
		wg.Add(1)
		go func(indexName string, p sharding.Placement) {
			defer wg.Done()
			sReq := ShardSearchRequest{
				IndexName:   indexName,
				ShardID:     p.ShardID,
				Query:       req.Query,
				Mode:        mode,
				NeedAggs:    needAggs,
				NeedSort:    needSort,
				DeferSource: true,
			}
			hits, err := n.executeShardSearchWithFailover(ctx, p, sReq, preferReader)
			resCh <- shardResult{hits: hits, err: err}
		}(ts.index, ts.p)
	}
	wg.Wait()
	close(resCh)

	totalShards := len(targetShards)
	successfulShards := 0
	failedShards := 0
	var firstErr error
	var allHits []ShardSearchHit
	for r := range resCh {
		if r.err != nil {
			failedShards++
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		successfulShards++
		allHits = append(allHits, r.hits...)
	}
	if successfulShards == 0 && failedShards > 0 && firstErr != nil {
		return nil, firstErr
	}

	var aggsOut map[string]any
	order := make([]int, len(allHits))
	for i := range order {
		order[i] = i
	}

	if needAggs || needSort {
		// Pack all hits into a single shared pseudo-Segment to avoid per-hit heap allocations.
		pseudoSeg := &segment.Segment{
			Docs:    make([]segment.DocMeta, len(allHits)),
			DocVals: make([]segment.DocValues, len(allHits)),
		}
		docRefs := make([]query.DocRef, len(allHits))
		for i, h := range allHits {
			pseudoSeg.Docs[i] = segment.DocMeta{DocID: h.DocID, Seq: h.Seq}
			pseudoSeg.DocVals[i] = h.DocVals
			docRefs[i] = query.DocRef{
				Seg:    pseudoSeg,
				DocIdx: uint32(i),
				Score:  h.Score,
			}
		}

		// Compute global aggregations across all matched docs prior to pagination.
		if needAggs {
			var err error
			aggsOut, err = query.ComputeAggregations(docRefs, aggsSpec)
			if err != nil {
				return nil, err
			}
		}

		sort.Slice(order, func(i, j int) bool {
			return query.CompareDocRefs(docRefs[order[i]], docRefs[order[j]], sortSpecs) < 0
		})
	} else if len(order) > 1 {
		scoreDesc := len(sortSpecs) == 0 || sortSpecs[0].Desc
		sort.Slice(order, func(i, j int) bool {
			hi, hj := allHits[order[i]], allHits[order[j]]
			if hi.Score != hj.Score {
				if scoreDesc {
					return hi.Score > hj.Score
				}
				return hi.Score < hj.Score
			}
			return hi.DocID < hj.DocID
		})
	}

	var maxScore any = nil
	if len(order) > 0 {
		ms := 0.0
		for _, idx := range order {
			if allHits[idx].Score > ms {
				ms = allHits[idx].Score
			}
		}
		maxScore = ms
	}

	from := req.From
	if from < 0 {
		from = 0
	}
	size := req.EffectiveSize()
	end := from + size
	if from > len(order) {
		from = len(order)
	}
	if end > len(order) {
		end = len(order)
	}

	esHits := make([]map[string]any, 0, end-from)
	for _, idx := range order[from:end] {
		h := allHits[idx]

		// Lazy-fetch _source ONLY for the paginated winning hits (avoiding reading thousands of non-returned docs).
		includeSource := sourceSpec.Enabled && (mode == RetrievalDropIn || h.DocsKey == "" || (mode == RetrievalHybrid && h.NVMeCached))
		if includeSource && len(h.RawJSON) == 0 && h.DocsKey != "" {
			raw, nvmeHit, err := n.readHitSource(ctx, h)
			if err != nil {
				return nil, err
			}
			h.RawJSON = raw
			h.NVMeCached = nvmeHit
		}

		item := map[string]any{
			"_index":  h.Index,
			"_id":     h.DocID,
			"_score":  h.Score,
			"_shard":  h.ShardID,
			"_crc32c": checksum.FormatHex(h.CRC32C),
		}

		if h.DocsKey != "" && n.s3 != nil && (mode == RetrievalPresigned || mode == RetrievalHybrid) {
			if presigned, err := n.s3.PresignGetURL(h.DocsKey, 5*time.Minute); err == nil {
				item["_s3_pointer"] = S3Pointer{
					URL:        presigned,
					Range:      fmt.Sprintf("bytes=%d-%d", h.Offset, h.Offset+h.Length-1),
					Offset:     h.Offset,
					Length:     h.Length,
					CRC32C:     checksum.FormatHex(h.CRC32C),
					Bucket:     n.s3.Bucket(),
					Key:        h.DocsKey,
					NVMeCached: h.NVMeCached,
				}
			}
		}

		if includeSource && len(h.RawJSON) > 0 {
			item["_source"] = query.FilterSource(h.RawJSON, sourceSpec)
		}
		esHits = append(esHits, item)
	}

	resp := map[string]any{
		"took":      time.Since(start).Milliseconds(),
		"timed_out": false,
		"_shards": map[string]any{
			"total":      totalShards,
			"successful": successfulShards,
			"skipped":    0,
			"failed":     failedShards,
		},
		"hits": map[string]any{
			"total": map[string]any{
				"value":    len(allHits),
				"relation": "eq",
			},
			"max_score": maxScore,
			"hits":      esHits,
		},
	}
	if len(aggsOut) > 0 {
		resp["aggregations"] = aggsOut
	}
	return resp, nil
}

func (n *Node) executeShardSearchWithFailover(ctx context.Context, p sharding.Placement, sReq ShardSearchRequest, preferReader bool) ([]ShardSearchHit, error) {
	dataOwners := make([]sharding.NodeInfo, 0, 1+len(p.Replicas))
	dataOwners = append(dataOwners, p.Primary)
	dataOwners = append(dataOwners, p.Replicas...)

	readers := n.ring.ReaderNodes()
	var rotatedReaders []sharding.NodeInfo
	if len(readers) > 0 {
		start := sReq.ShardID % len(readers)
		rotatedReaders = make([]sharding.NodeInfo, 0, len(readers))
		rotatedReaders = append(rotatedReaders, readers[start:]...)
		rotatedReaders = append(rotatedReaders, readers[:start]...)
	}

	candidates := make([]sharding.NodeInfo, 0, len(dataOwners)+len(rotatedReaders))
	if preferReader && len(rotatedReaders) > 0 {
		candidates = append(candidates, rotatedReaders...)
		candidates = append(candidates, dataOwners...)
	} else {
		candidates = append(candidates, dataOwners...)
		candidates = append(candidates, rotatedReaders...)
	}

	var lastErr error
	for _, owner := range candidates {
		if owner.ID == n.cfg.NodeID {
			hits, err := n.ExecuteLocalShardSearch(ctx, sReq)
			if err == nil {
				return hits, nil
			}
			lastErr = err
			continue
		}
		if owner.Address == "" {
			continue
		}
		var hits []ShardSearchHit
		if err := n.postJSON(ctx, baseURL(owner.Address)+"/_internal/shard/search", sReq, &hits); err == nil {
			return hits, nil
		} else {
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no healthy shard owner for %s[%d]", sReq.IndexName, sReq.ShardID)
	}
	return nil, lastErr
}

func (n *Node) readHitSource(ctx context.Context, h ShardSearchHit) ([]byte, bool, error) {
	n.mu.RLock()
	var sh *Shard
	if m := n.shards[h.Index]; m != nil {
		sh = m[h.ShardID]
	}
	n.mu.RUnlock()
	if sh != nil {
		return sh.ReadDocSourceByMeta(ctx, h.DocsKey, h.Offset, h.Length, h.CRC32C)
	}
	if n.nvmeCache != nil {
		if slice, hit, cacheErr := n.nvmeCache.GetRange(h.DocsKey, h.Offset, h.Length); hit && cacheErr == nil {
			if checksum.Verify(slice, h.CRC32C) == nil {
				return slice, true, nil
			}
			n.nvmeCache.InvalidateCorrupted(h.DocsKey)
		}
	}
	if n.s3 == nil {
		return nil, false, fmt.Errorf("s3 client not configured")
	}
	slice, err := n.s3.GetObjectRange(ctx, h.DocsKey, h.Offset, h.Offset+h.Length-1)
	if err != nil {
		return nil, false, err
	}
	if err := checksum.Verify(slice, h.CRC32C); err != nil {
		return nil, false, err
	}
	return slice, false, nil
}

// ExecuteLocalShardSearch evaluates Query DSL against all segments in a local shard.
func (n *Node) ExecuteLocalShardSearch(ctx context.Context, sReq ShardSearchRequest) ([]ShardSearchHit, error) {
	n.mu.RLock()
	sh := n.shards[sReq.IndexName][sReq.ShardID]
	n.mu.RUnlock()
	if sh == nil {
		return nil, fmt.Errorf("shard %s[%d] not hosted on %s", sReq.IndexName, sReq.ShardID, n.cfg.NodeID)
	}

	segs, latestSeq, err := sh.ActiveSegments(ctx)
	if err != nil {
		return nil, err
	}
	if len(segs) == 0 {
		return nil, nil
	}

	singleSeg := len(segs) == 1
	var seen map[string]struct{}
	if !singleSeg {
		seen = make(map[string]struct{})
	}
	var out []ShardSearchHit

	for segIdx, seg := range segs {
		segHits, err := query.EvaluateSegment(seg, sReq.Query)
		if err != nil {
			return nil, err
		}
		if len(segHits) == 0 {
			continue
		}
		if segIdx == 0 && out == nil {
			out = make([]ShardSearchHit, 0, len(segHits))
		}
		docsCached := seg.DocsObjectKey == "" || sh.IsDocsCachedOnNVMe(seg.DocsObjectKey)
		needSource := sReq.Mode == RetrievalDropIn || seg.DocsObjectKey == "" || (sReq.Mode == RetrievalHybrid && docsCached)
		eagerSource := needSource && (!sReq.DeferSource || seg.DocsObjectKey == "")

		for _, h := range segHits {
			dm := seg.Docs[h.DocIdx]
			if !singleSeg {
				if dm.Seq < latestSeq[dm.DocID] {
					continue
				}
				if _, ok := seen[dm.DocID]; ok {
					continue
				}
				seen[dm.DocID] = struct{}{}
			}

			hit := ShardSearchHit{
				Index:      sReq.IndexName,
				ShardID:    sReq.ShardID,
				DocID:      dm.DocID,
				Score:      h.Score,
				Seq:        dm.Seq,
				DocsKey:    seg.DocsObjectKey,
				Offset:     dm.Offset,
				Length:     dm.Length,
				CRC32C:     dm.CRC32C,
				NVMeCached: docsCached,
			}
			if sReq.NeedAggs || sReq.NeedSort {
				hit.DocVals = seg.DocVals[h.DocIdx]
			}
			if eagerSource {
				raw, nvmeHit, err := sh.ReadDocSource(ctx, seg, h.DocIdx)
				if err != nil {
					return nil, err
				}
				hit.RawJSON = raw
				hit.NVMeCached = nvmeHit
			}
			out = append(out, hit)
		}
	}
	return out, nil
}

// ReplicationStats reports async WAL replication and S3 flush diagnostics.
type ReplicationStats struct {
	NodeID                 string       `json:"node_id"`
	Roles                  []string     `json:"roles"`
	ReplicationBatchesSent int64        `json:"replication_batches_sent"`
	ReplicationErrors      int64        `json:"replication_errors"`
	S3SegmentFlushes       int64        `json:"s3_segment_flushes"`
	NVMeCache              cache.Stats  `json:"nvme_cache"`
	Shards                 []ShardStats `json:"shards"`
}

// Stats returns node-level replication, flush, NVMe cache, and shard diagnostics.
func (n *Node) Stats() ReplicationStats {
	n.mu.RLock()
	var shardStats []ShardStats
	for _, m := range n.shards {
		for _, sh := range m {
			shardStats = append(shardStats, sh.Stats())
		}
	}
	n.mu.RUnlock()
	sort.Slice(shardStats, func(i, j int) bool {
		if shardStats[i].IndexName == shardStats[j].IndexName {
			return shardStats[i].ShardID < shardStats[j].ShardID
		}
		return shardStats[i].IndexName < shardStats[j].IndexName
	})

	return ReplicationStats{
		NodeID:                 n.cfg.NodeID,
		Roles:                  n.Roles(),
		ReplicationBatchesSent: n.repSuccess.Load(),
		ReplicationErrors:      n.repErrors.Load(),
		S3SegmentFlushes:       n.flushes.Load(),
		NVMeCache:              n.nvmeCache.Stats(),
		Shards:                 shardStats,
	}
}

func (n *Node) replicationLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.ReplicateInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.replicateOnce(context.Background())
		}
	}
}

func (n *Node) replicateOnce(ctx context.Context) {
	n.mu.RLock()
	type shardWork struct {
		meta IndexMetadata
		sh   *Shard
	}
	var work []shardWork
	for idxName, m := range n.shards {
		meta := n.indices[idxName]
		for _, sh := range m {
			if sh.isPrimary {
				work = append(work, shardWork{meta: meta, sh: sh})
			}
		}
	}
	n.mu.RUnlock()

	for _, w := range work {
		if w.meta.Settings.NumberOfReplicas <= 0 {
			continue
		}
		p, err := n.ring.PlacementForShard(w.meta.Name, w.sh.shardID, w.meta.Settings.NumberOfReplicas)
		if err != nil {
			continue
		}
		for _, rep := range p.Replicas {
			if rep.Address == "" || rep.ID == n.cfg.NodeID {
				continue
			}
			w.sh.mu.RLock()
			lastAck := w.sh.replicaAckSeq[rep.ID]
			w.sh.mu.RUnlock()

			pending := w.sh.PendingReplication(lastAck)
			if len(pending) == 0 {
				continue
			}

			req := ReplicateBatchRequest{
				IndexName: w.meta.Name,
				ShardID:   w.sh.shardID,
				FromNode:  n.cfg.NodeID,
				Records:   pending,
			}
			var resp ReplicateBatchResponse
			if err := n.postJSON(ctx, baseURL(rep.Address)+"/_internal/replicate", req, &resp); err != nil {
				n.repErrors.Add(1)
				continue
			}
			w.sh.mu.Lock()
			if resp.AckSeq > w.sh.replicaAckSeq[rep.ID] {
				w.sh.replicaAckSeq[rep.ID] = resp.AckSeq
			}
			w.sh.mu.Unlock()
			n.repSuccess.Add(1)
		}
	}
}

func (n *Node) flushLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			if n.s3 == nil {
				continue
			}
			n.mu.RLock()
			var primaries []*Shard
			for _, m := range n.shards {
				for _, sh := range m {
					if sh.isPrimary {
						primaries = append(primaries, sh)
					}
				}
			}
			n.mu.RUnlock()
			for _, sh := range primaries {
				if ref, err := sh.FlushToS3(context.Background()); err == nil && ref != nil {
					n.flushes.Add(1)
				}
			}
		}
	}
}

func (n *Node) scrubLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.ScrubInterval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			_ = n.nvmeCache.Scrub()
		}
	}
}

func (n *Node) postJSON(ctx context.Context, targetURL string, body any, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("rpc %s status %d: %s", targetURL, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (n *Node) putJSONRaw(ctx context.Context, targetURL string, raw []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, targetURL, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("put %s status %d: %s", targetURL, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func baseURL(addr string) string {
	if strings.HasPrefix(addr, "http://") || strings.HasPrefix(addr, "https://") {
		return strings.TrimRight(addr, "/")
	}
	return "http://" + strings.TrimRight(addr, "/")
}

func generateDocID() string {
	var b [10]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
