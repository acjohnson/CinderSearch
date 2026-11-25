package cluster

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// ErrVersionConflict is returned when op_type=create targets an existing document ID (HTTP 409).
var ErrVersionConflict = errors.New("version_conflict_engine_exception")

const clusterIndexMetaPrefix = "_cluster/indices/"

func clusterIndexMetaKey(indexName string) string {
	return clusterIndexMetaPrefix + indexName + ".json"
}

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
	Merge             MergeConfig
	Discovery         DiscoveryConfig
	S3                *s3store.Client
	Peers             []sharding.NodeInfo
}

// DocWriteResult carries official Elasticsearch v8 document write metadata.
type DocWriteResult struct {
	ID          string `json:"_id"`
	ShardID     int    `json:"_shard"`
	CRC32C      uint32 `json:"_crc32c"`
	SeqNo       uint64 `json:"_seq_no"`
	PrimaryTerm uint64 `json:"_primary_term"`
	Version     uint64 `json:"_version"`
	Created     bool   `json:"created"`
	ShardsTotal int    `json:"shards_total"`
	ShardsSucc  int    `json:"shards_successful"`
}

// DocGetResult carries official Elasticsearch v8 GET /{index}/_doc/{id} metadata.
type DocGetResult struct {
	Found       bool
	Source      []byte
	CRC32C      uint32
	SeqNo       uint64
	PrimaryTerm uint64
	Version     uint64
	S3Pointer   *S3Pointer
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
	Aliases   []string       `json:"aliases,omitempty"`
	CreatedAt int64          `json:"created_at"`
}

// AliasAction represents a single add/remove operation in POST /_aliases.
type AliasAction struct {
	Action  string   // "add" or "remove"
	Index   string   // single index or wildcard pattern
	Indices []string // optional list of indices or patterns
	Alias   string   // alias name
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

	mu            sync.RWMutex
	reconcileMu   sync.Mutex
	advertiseAddr string
	startedAt     int64
	indices       map[string]IndexMetadata
	shards        map[string]map[int]*Shard

	repSuccess          atomic.Int64
	repErrors           atomic.Int64
	flushes             atomic.Int64
	mergesCompleted     atomic.Int64
	segmentsMerged      atomic.Int64
	tombstonesPurged    atomic.Int64
	versionsCompacted   atomic.Int64
	s3ObjectsReclaimed  atomic.Int64
	promotionsCompleted atomic.Int64
	draining            atomic.Bool
	leaseEpoch          atomic.Uint64
	membershipVer       atomic.Uint64
	mergeSem            chan struct{}
	stopCh              chan struct{}
	wg                  sync.WaitGroup
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
	cfg.Merge = cfg.Merge.Normalize()
	discCfg, err := cfg.Discovery.Normalize(strings.TrimSpace(cfg.AdvertiseAddress) != "")
	if err != nil {
		return nil, err
	}
	cfg.Discovery = discCfg

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
		cfg:           cfg,
		ring:          sharding.NewRing(64, peers...),
		nvmeCache:     nvmeCache,
		s3:            cfg.S3,
		advertiseAddr: strings.TrimSpace(cfg.AdvertiseAddress),
		startedAt:     time.Now().UTC().UnixMilli(),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
		indices:  make(map[string]IndexMetadata),
		shards:   make(map[string]map[int]*Shard),
		mergeSem: make(chan struct{}, cfg.Merge.MaxConcurrentMerges),
		stopCh:   make(chan struct{}),
	}

	if n.discoveryEnabled() {
		_ = n.PublishLease(context.Background())
		_, _ = n.SyncMembershipOnce(context.Background())
	} else if n.s3 != nil && len(n.ring.DataNodes()) > 0 {
		// Cold-boot recovery for static-peer mode: hydrate persisted index metadata, mappings, and aliases from S3.
		_ = n.LoadClusterMetadataFromS3(context.Background())
	}

	if n.HasRole(sharding.RoleData) {
		n.wg.Add(3)
		go n.replicationLoop()
		go n.flushLoop()
		go n.mergeLoop()
	}
	if n.HasRole(sharding.RoleData) || n.HasRole(sharding.RoleReader) {
		n.wg.Add(1)
		go n.scrubLoop()
	}
	if n.discoveryEnabled() {
		n.wg.Add(1)
		go n.membershipLoop()
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
		if p.ID == n.cfg.NodeID && p.Address != "" {
			n.SetAdvertiseAddress(p.Address)
		}
		n.ring.AddNode(p)
	}
}

// Close gracefully removes the node's S3 lease, stops background workers, and closes open shard WALs.
func (n *Node) Close() error {
	if n.discoveryEnabled() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = n.DeleteLease(ctx)
		cancel()
	}
	return n.stopAndCloseLocal()
}

// SimulateCrash stops background workers and closes local WAL handles without deleting the node's
// S3 membership lease, enabling deterministic tests of lease-TTL expiration and primary failover.
func (n *Node) SimulateCrash() error {
	return n.stopAndCloseLocal()
}

func (n *Node) stopAndCloseLocal() error {
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

// LoadClusterMetadataFromS3 restores persisted index definitions, mappings, and aliases from S3
// and opens role-appropriate shard handles on this node.
func (n *Node) LoadClusterMetadataFromS3(ctx context.Context) error {
	if n.s3 == nil {
		return nil
	}
	objs, err := n.s3.ListObjects(ctx, clusterIndexMetaPrefix)
	if err != nil {
		return err
	}
	for _, obj := range objs {
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		raw, _, err := n.s3.GetObject(ctx, obj.Key)
		if err != nil {
			continue
		}
		var meta IndexMetadata
		if err := json.Unmarshal(raw, &meta); err != nil || meta.Name == "" {
			continue
		}
		if err := n.ApplyIndexMetadata(ctx, meta); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) persistIndexMetadata(ctx context.Context, meta IndexMetadata) error {
	if n.s3 == nil {
		return nil
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	_, err = n.s3.PutObject(ctx, clusterIndexMetaKey(meta.Name), raw)
	return err
}

func (n *Node) deletePersistedIndexMetadata(ctx context.Context, indexName string) error {
	if n.s3 == nil {
		return nil
	}
	return n.s3.DeleteObject(ctx, clusterIndexMetaKey(indexName))
}

func (n *Node) broadcastIndexMetadata(ctx context.Context, meta IndexMetadata) {
	for _, peer := range n.ring.Nodes() {
		if peer.ID == n.cfg.NodeID || peer.Address == "" {
			continue
		}
		_ = n.postJSON(ctx, baseURL(peer.Address)+"/_internal/index/sync", meta, nil)
	}
}

// CreateIndex registers an index across the cluster, persists metadata to S3, and opens shards locally.
func (n *Node) CreateIndex(ctx context.Context, meta IndexMetadata, broadcast bool) error {
	if err := validateIndexName(meta.Name); err != nil {
		return err
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
	meta.Aliases = normalizeStringSet(meta.Aliases)

	if err := n.ApplyIndexMetadata(ctx, meta); err != nil {
		return err
	}
	if err := n.persistIndexMetadata(ctx, meta); err != nil {
		return err
	}

	if broadcast {
		n.broadcastIndexMetadata(ctx, meta)
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
			sh.SetOwnerNodeID(n.cfg.NodeID)
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
		_ = n.deletePersistedIndexMetadata(ctx, indexName)
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

// PutMapping merges mapping definitions into an existing index, persists to S3, and broadcasts to peers.
func (n *Node) PutMapping(ctx context.Context, indexName string, mappings map[string]any, broadcast bool) error {
	meta, ok := n.GetIndex(indexName)
	if !ok {
		return fmt.Errorf("index_not_found_exception: %s", indexName)
	}
	if meta.Mappings == nil {
		meta.Mappings = make(map[string]any)
	}
	for k, v := range mappings {
		if k == "properties" {
			newProps, okNew := v.(map[string]any)
			oldProps, okOld := meta.Mappings["properties"].(map[string]any)
			if okNew && okOld {
				merged := make(map[string]any, len(oldProps)+len(newProps))
				for pk, pv := range oldProps {
					merged[pk] = pv
				}
				for pk, pv := range newProps {
					merged[pk] = pv
				}
				meta.Mappings["properties"] = merged
				continue
			}
		}
		meta.Mappings[k] = v
	}

	n.mu.Lock()
	n.indices[indexName] = meta
	n.mu.Unlock()

	if err := n.persistIndexMetadata(ctx, meta); err != nil {
		return err
	}
	if broadcast {
		n.broadcastIndexMetadata(ctx, meta)
	}
	return nil
}

// UpdateAliases atomically applies add/remove alias actions, persists modified index metadata to S3,
// and broadcasts updates to cluster peers.
func (n *Node) UpdateAliases(ctx context.Context, actions []AliasAction, broadcast bool) error {
	if len(actions) == 0 {
		return fmt.Errorf("at least one alias action is required")
	}

	n.mu.Lock()
	working := make(map[string]IndexMetadata, len(n.indices))
	for k, v := range n.indices {
		cp := v
		cp.Aliases = append([]string(nil), v.Aliases...)
		working[k] = cp
	}
	n.mu.Unlock()

	modified := make(map[string]struct{})
	for _, act := range actions {
		alias := strings.TrimSpace(act.Alias)
		if alias == "" {
			return fmt.Errorf("alias name is required")
		}
		var patterns []string
		if act.Index != "" {
			patterns = append(patterns, strings.Split(act.Index, ",")...)
		}
		patterns = append(patterns, act.Indices...)
		if len(patterns) == 0 {
			return fmt.Errorf("index pattern is required for alias %q", alias)
		}

		targets, err := resolveIndicesFromMap(working, patterns)
		if err != nil {
			return err
		}
		if len(targets) == 0 {
			return fmt.Errorf("index_not_found_exception: %s", strings.Join(patterns, ","))
		}

		for _, idxName := range targets {
			meta := working[idxName]
			switch strings.ToLower(act.Action) {
			case "add":
				meta.Aliases = normalizeStringSet(append(meta.Aliases, alias))
			case "remove":
				filtered := make([]string, 0, len(meta.Aliases))
				for _, a := range meta.Aliases {
					if a != alias {
						filtered = append(filtered, a)
					}
				}
				meta.Aliases = filtered
			default:
				return fmt.Errorf("unsupported alias action %q", act.Action)
			}
			working[idxName] = meta
			modified[idxName] = struct{}{}
		}
	}

	n.mu.Lock()
	for idxName := range modified {
		n.indices[idxName] = working[idxName]
	}
	n.mu.Unlock()

	for idxName := range modified {
		meta := working[idxName]
		if err := n.persistIndexMetadata(ctx, meta); err != nil {
			return err
		}
		if broadcast {
			n.broadcastIndexMetadata(ctx, meta)
		}
	}
	return nil
}

// GetAliases returns Elasticsearch-formatted alias mappings for matching indices or alias names.
func (n *Node) GetAliases(indexPatterns []string, aliasFilter string) map[string]any {
	indices := n.ListIndices()
	out := make(map[string]any)
	for _, meta := range indices {
		if len(indexPatterns) > 0 {
			matched := false
			for _, pat := range indexPatterns {
				if pat == "_all" || pat == "*" || matchWildcard(pat, meta.Name) {
					matched = true
					break
				}
				for _, a := range meta.Aliases {
					if matchWildcard(pat, a) {
						matched = true
						break
					}
				}
				if matched {
					break
				}
			}
			if !matched {
				continue
			}
		}
		aliasesObj := make(map[string]any)
		for _, a := range meta.Aliases {
			if aliasFilter != "" && !matchWildcard(aliasFilter, a) {
				continue
			}
			aliasesObj[a] = map[string]any{}
		}
		if aliasFilter != "" && len(aliasesObj) == 0 {
			continue
		}
		out[meta.Name] = map[string]any{
			"aliases": aliasesObj,
		}
	}
	return out
}

// HasIndexOrAlias reports whether any index or alias matches pattern (used by HEAD /{index}).
func (n *Node) HasIndexOrAlias(pattern string) bool {
	resolved, err := n.ResolveIndices([]string{pattern})
	return err == nil && len(resolved) > 0
}

// ResolveIndices expands index names, aliases, _all, glob wildcards (* and ?), and -exclusions
// matching Elasticsearch multi-index resolution semantics.
func (n *Node) ResolveIndices(patterns []string) ([]string, error) {
	n.mu.RLock()
	snapshot := make(map[string]IndexMetadata, len(n.indices))
	for k, v := range n.indices {
		snapshot[k] = v
	}
	n.mu.RUnlock()
	return resolveIndicesFromMap(snapshot, patterns)
}

func resolveIndicesFromMap(indices map[string]IndexMetadata, patterns []string) ([]string, error) {
	allNames := make([]string, 0, len(indices))
	for name := range indices {
		allNames = append(allNames, name)
	}
	sort.Strings(allNames)

	if len(patterns) == 0 {
		return allNames, nil
	}

	selected := make(map[string]struct{})
	for _, raw := range patterns {
		pat := strings.TrimSpace(raw)
		if pat == "" {
			continue
		}
		exclude := false
		if strings.HasPrefix(pat, "-") {
			exclude = true
			pat = strings.TrimPrefix(pat, "-")
			if pat == "" {
				continue
			}
		}

		isWildcard := pat == "_all" || strings.ContainsAny(pat, "*?")
		var matched []string
		if pat == "_all" || pat == "*" {
			matched = allNames
		} else if isWildcard {
			for _, name := range allNames {
				if matchWildcard(pat, name) {
					matched = append(matched, name)
					continue
				}
				for _, alias := range indices[name].Aliases {
					if matchWildcard(pat, alias) {
						matched = append(matched, name)
						break
					}
				}
			}
		} else {
			if _, ok := indices[pat]; ok {
				matched = append(matched, pat)
			} else {
				for _, name := range allNames {
					for _, alias := range indices[name].Aliases {
						if alias == pat {
							matched = append(matched, name)
							break
						}
					}
				}
			}
			if len(matched) == 0 && !exclude {
				return nil, fmt.Errorf("index_not_found_exception: %s", pat)
			}
		}

		if exclude {
			for _, name := range matched {
				delete(selected, name)
			}
		} else {
			for _, name := range matched {
				selected[name] = struct{}{}
			}
		}
	}

	out := make([]string, 0, len(selected))
	for name := range selected {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}

func matchWildcard(pattern, s string) bool {
	p, t := []rune(pattern), []rune(s)
	pi, ti := 0, 0
	starIdx, matchIdx := -1, 0
	for ti < len(t) {
		if pi < len(p) && (p[pi] == '?' || p[pi] == t[ti]) {
			pi++
			ti++
		} else if pi < len(p) && p[pi] == '*' {
			starIdx = pi
			matchIdx = ti
			pi++
		} else if starIdx != -1 {
			pi = starIdx + 1
			matchIdx++
			ti = matchIdx
		} else {
			return false
		}
	}
	for pi < len(p) && p[pi] == '*' {
		pi++
	}
	return pi == len(p)
}

func normalizeStringSet(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			out = append(out, s)
		}
	}
	sort.Strings(out)
	return out
}

func validateIndexName(name string) error {
	if name == "" {
		return fmt.Errorf("index name is required")
	}
	if name == "." || name == ".." || len(name) > 255 {
		return fmt.Errorf("invalid_index_name_exception: invalid index name [%s]", name)
	}
	if name[0] == '_' || name[0] == '-' || name[0] == '+' {
		return fmt.Errorf("invalid_index_name_exception: index name [%s] must not start with '_', '-', or '+'", name)
	}
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("invalid_index_name_exception: invalid character %q in index name [%s]", r, name)
	}
	return nil
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
	res, err := n.IndexDocMeta(ctx, indexName, docID, rawJSON, routing, "")
	if err != nil {
		return "", res.ShardID, 0, 0, err
	}
	return res.ID, res.ShardID, res.CRC32C, res.SeqNo, nil
}

// IndexDocWithOpType supports Elasticsearch op_type=create (returning ErrVersionConflict if docID already exists).
func (n *Node) IndexDocWithOpType(ctx context.Context, indexName, docID string, rawJSON []byte, routing, opType string) (string, int, uint32, uint64, error) {
	res, err := n.IndexDocMeta(ctx, indexName, docID, rawJSON, routing, opType)
	if err != nil {
		return "", res.ShardID, 0, 0, err
	}
	return res.ID, res.ShardID, res.CRC32C, res.SeqNo, nil
}

// IndexDocMeta routes a document write and returns full Elasticsearch v8 write metadata
// (_id, _seq_no, _primary_term, _version, created/updated, and _shards counts), with
// automatic single failover retry if a remote primary became unreachable after promotion.
func (n *Node) IndexDocMeta(ctx context.Context, indexName, docID string, rawJSON []byte, routing, opType string) (DocWriteResult, error) {
	meta, err := n.EnsureIndex(ctx, indexName)
	if err != nil {
		return DocWriteResult{}, err
	}
	if docID == "" {
		docID = generateDocID()
	}
	if routing == "" {
		routing = docID
	}

	shardID := sharding.ShardForKey(indexName, routing, meta.Settings.NumberOfShards)

	for attempt := 0; attempt < 2; attempt++ {
		placement, err := n.ring.PlacementForShard(indexName, shardID, meta.Settings.NumberOfReplicas)
		if err != nil {
			return DocWriteResult{ShardID: shardID}, err
		}
		shardsTotal := 1 + len(placement.Replicas)

		if placement.Primary.ID == n.cfg.NodeID {
			n.mu.RLock()
			sh := n.shards[indexName][shardID]
			n.mu.RUnlock()
			if sh == nil {
				return DocWriteResult{ShardID: shardID}, fmt.Errorf("primary shard %s[%d] not initialized on %s", indexName, shardID, n.cfg.NodeID)
			}
			exists, err := sh.DocExists(ctx, docID)
			if err != nil {
				return DocWriteResult{ShardID: shardID}, err
			}
			if strings.EqualFold(opType, "create") && exists {
				return DocWriteResult{ShardID: shardID}, fmt.Errorf("%w: [%s]: document already exists", ErrVersionConflict, docID)
			}
			rec, err := sh.IndexLocal(docID, rawJSON)
			if err != nil {
				return DocWriteResult{ShardID: shardID}, err
			}
			return DocWriteResult{
				ID:          docID,
				ShardID:     shardID,
				CRC32C:      checksum.Sum32C(rawJSON),
				SeqNo:       rec.Seq,
				PrimaryTerm: sh.PrimaryTerm(),
				Version:     rec.Seq,
				Created:     !exists,
				ShardsTotal: shardsTotal,
				ShardsSucc:  1,
			}, nil
		}

		// Forward to remote primary node.
		var resp struct {
			ID          string `json:"_id"`
			ShardID     int    `json:"_shard"`
			CRC32C      uint32 `json:"_crc32c"`
			Seq         uint64 `json:"_seq_no"`
			PrimaryTerm uint64 `json:"_primary_term"`
			Version     uint64 `json:"_version"`
			Result      string `json:"result"`
		}
		target := fmt.Sprintf("%s/%s/_doc/%s?routing=%s", baseURL(placement.Primary.Address), indexName, docID, routing)
		if strings.EqualFold(opType, "create") {
			target += "&op_type=create"
		}
		err = n.putJSONRaw(ctx, target, rawJSON, &resp)
		if err == nil {
			term := resp.PrimaryTerm
			if term == 0 {
				term = 1
			}
			ver := resp.Version
			if ver == 0 {
				ver = resp.Seq
			}
			return DocWriteResult{
				ID:          docID,
				ShardID:     shardID,
				CRC32C:      resp.CRC32C,
				SeqNo:       resp.Seq,
				PrimaryTerm: term,
				Version:     ver,
				Created:     resp.Result == "created",
				ShardsTotal: shardsTotal,
				ShardsSucc:  1,
			}, nil
		}
		if errors.Is(err, ErrVersionConflict) || attempt == 1 || !n.discoveryEnabled() {
			return DocWriteResult{ShardID: shardID}, err
		}
		// Transport failure on remote primary: sync S3 membership once and retry if a new primary was promoted.
		changed, _ := n.SyncMembershipOnce(ctx)
		if !changed {
			return DocWriteResult{ShardID: shardID}, err
		}
	}
	return DocWriteResult{ShardID: shardID}, fmt.Errorf("write failover retries exhausted for %s[%d]", indexName, shardID)
}

// DeleteDoc routes a delete tombstone to the primary shard node.
func (n *Node) DeleteDoc(ctx context.Context, indexName, docID, routing string) (int, uint64, error) {
	res, err := n.DeleteDocMeta(ctx, indexName, docID, routing)
	if err != nil {
		return res.ShardID, 0, err
	}
	return res.ShardID, res.SeqNo, nil
}

// DeleteDocMeta routes a delete tombstone and returns full ES v8 metadata (_seq_no, _primary_term, _shards).
func (n *Node) DeleteDocMeta(ctx context.Context, indexName, docID, routing string) (DocWriteResult, error) {
	meta, ok := n.GetIndex(indexName)
	if !ok {
		return DocWriteResult{}, fmt.Errorf("index_not_found_exception: %s", indexName)
	}
	if routing == "" {
		routing = docID
	}
	shardID := sharding.ShardForKey(indexName, routing, meta.Settings.NumberOfShards)

	for attempt := 0; attempt < 2; attempt++ {
		placement, err := n.ring.PlacementForShard(indexName, shardID, meta.Settings.NumberOfReplicas)
		if err != nil {
			return DocWriteResult{ShardID: shardID}, err
		}
		shardsTotal := 1 + len(placement.Replicas)

		if placement.Primary.ID == n.cfg.NodeID {
			n.mu.RLock()
			sh := n.shards[indexName][shardID]
			n.mu.RUnlock()
			if sh == nil {
				return DocWriteResult{ShardID: shardID}, fmt.Errorf("shard %s[%d] missing", indexName, shardID)
			}
			rec, err := sh.DeleteLocal(docID)
			if err != nil {
				return DocWriteResult{ShardID: shardID}, err
			}
			return DocWriteResult{
				ID:          docID,
				ShardID:     shardID,
				SeqNo:       rec.Seq,
				PrimaryTerm: sh.PrimaryTerm(),
				Version:     rec.Seq,
				ShardsTotal: shardsTotal,
				ShardsSucc:  1,
			}, nil
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, fmt.Sprintf("%s/%s/_doc/%s?routing=%s", baseURL(placement.Primary.Address), indexName, docID, routing), nil)
		if err != nil {
			return DocWriteResult{ShardID: shardID}, err
		}
		resp, err := n.httpClient.Do(req)
		if err != nil {
			if attempt == 0 && n.discoveryEnabled() {
				if changed, _ := n.SyncMembershipOnce(ctx); changed {
					continue
				}
			}
			return DocWriteResult{ShardID: shardID}, err
		}
		var parsed struct {
			Seq         uint64 `json:"_seq_no"`
			PrimaryTerm uint64 `json:"_primary_term"`
			Version     uint64 `json:"_version"`
		}
		_ = json.NewDecoder(resp.Body).Decode(&parsed)
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			return DocWriteResult{ShardID: shardID}, fmt.Errorf("remote delete failed (%d)", resp.StatusCode)
		}
		term := parsed.PrimaryTerm
		if term == 0 {
			term = 1
		}
		ver := parsed.Version
		if ver == 0 {
			ver = parsed.Seq
		}
		return DocWriteResult{
			ID:          docID,
			ShardID:     shardID,
			SeqNo:       parsed.Seq,
			PrimaryTerm: term,
			Version:     ver,
			ShardsTotal: shardsTotal,
			ShardsSucc:  1,
		}, nil
	}
	return DocWriteResult{ShardID: shardID}, fmt.Errorf("delete failover retries exhausted for %s[%d]", indexName, shardID)
}

// GetDoc fetches a document by _id from its owning shard (primary or replica).
func (n *Node) GetDoc(ctx context.Context, indexName, docID, routing string) ([]byte, bool, uint32, *S3Pointer, error) {
	res, err := n.GetDocMeta(ctx, indexName, docID, routing)
	if err != nil {
		return nil, false, 0, nil, err
	}
	return res.Source, res.Found, res.CRC32C, res.S3Pointer, nil
}

// GetDocMeta fetches a document by _id with full ES v8 metadata (_seq_no, _primary_term, _version).
func (n *Node) GetDocMeta(ctx context.Context, indexName, docID, routing string) (DocGetResult, error) {
	meta, ok := n.GetIndex(indexName)
	if !ok {
		return DocGetResult{}, fmt.Errorf("index_not_found_exception: %s", indexName)
	}
	if routing == "" {
		routing = docID
	}
	shardID := sharding.ShardForKey(indexName, routing, meta.Settings.NumberOfShards)
	placement, err := n.ring.PlacementForShard(indexName, shardID, meta.Settings.NumberOfReplicas)
	if err != nil {
		return DocGetResult{}, err
	}

	if placement.Primary.ID == n.cfg.NodeID {
		return n.getLocalDocMeta(ctx, indexName, shardID, docID)
	}

	// Check if this node holds a WAL-backed replica locally first for fast reads
	// (skip readOnly shards here so NRT reads hit the primary WAL before S3 flush).
	n.mu.RLock()
	localReplica := n.shards[indexName][shardID]
	n.mu.RUnlock()
	if localReplica != nil && !localReplica.readOnly {
		return n.getLocalDocMeta(ctx, indexName, shardID, docID)
	}

	// Forward to primary, with automatic fallback to replicas if primary is unreachable.
	candidates := make([]sharding.NodeInfo, 0, 1+len(placement.Replicas))
	candidates = append(candidates, placement.Primary)
	candidates = append(candidates, placement.Replicas...)

	var lastErr error
	for _, owner := range candidates {
		if owner.Address == "" {
			continue
		}
		targetURL := fmt.Sprintf("%s/%s/_doc/%s?routing=%s", baseURL(owner.Address), indexName, docID, routing)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := n.httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusNotFound {
			_ = resp.Body.Close()
			return DocGetResult{Found: false}, nil
		}
		var parsed struct {
			Found       bool            `json:"found"`
			Source      json.RawMessage `json:"_source"`
			CRC32C      string          `json:"_crc32c"`
			SeqNo       uint64          `json:"_seq_no"`
			PrimaryTerm uint64          `json:"_primary_term"`
			Version     uint64          `json:"_version"`
			S3Pointer   *S3Pointer      `json:"_s3_pointer,omitempty"`
		}
		decErr := json.NewDecoder(resp.Body).Decode(&parsed)
		_ = resp.Body.Close()
		if decErr != nil {
			lastErr = decErr
			continue
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
		term := parsed.PrimaryTerm
		if term == 0 {
			term = 1
		}
		ver := parsed.Version
		if ver == 0 {
			ver = parsed.SeqNo
		}
		return DocGetResult{
			Found:       parsed.Found,
			Source:      parsed.Source,
			CRC32C:      crc,
			SeqNo:       parsed.SeqNo,
			PrimaryTerm: term,
			Version:     ver,
			S3Pointer:   parsed.S3Pointer,
		}, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no healthy shard owner for %s[%d]", indexName, shardID)
	}
	return DocGetResult{}, lastErr
}

// DocExists checks whether docID exists in indexName (supporting HEAD /{index}/_doc/{id}).
func (n *Node) DocExists(ctx context.Context, indexName, docID, routing string) (bool, error) {
	meta, ok := n.GetIndex(indexName)
	if !ok {
		return false, fmt.Errorf("index_not_found_exception: %s", indexName)
	}
	if routing == "" {
		routing = docID
	}
	shardID := sharding.ShardForKey(indexName, routing, meta.Settings.NumberOfShards)
	placement, err := n.ring.PlacementForShard(indexName, shardID, meta.Settings.NumberOfReplicas)
	if err != nil {
		return false, err
	}

	if placement.Primary.ID == n.cfg.NodeID {
		n.mu.RLock()
		sh := n.shards[indexName][shardID]
		n.mu.RUnlock()
		if sh == nil {
			return false, nil
		}
		return sh.DocExists(ctx, docID)
	}

	n.mu.RLock()
	localReplica := n.shards[indexName][shardID]
	n.mu.RUnlock()
	if localReplica != nil && !localReplica.readOnly {
		return localReplica.DocExists(ctx, docID)
	}

	url := fmt.Sprintf("%s/%s/_doc/%s?routing=%s", baseURL(placement.Primary.Address), indexName, docID, routing)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return false, err
	}
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if resp.StatusCode >= 300 {
		return false, fmt.Errorf("remote head doc failed (%d)", resp.StatusCode)
	}
	return true, nil
}

func (n *Node) getLocalDocMeta(ctx context.Context, indexName string, shardID int, docID string) (DocGetResult, error) {
	n.mu.RLock()
	sh := n.shards[indexName][shardID]
	n.mu.RUnlock()
	if sh == nil {
		return DocGetResult{}, nil
	}
	raw, found, crc, dm, seg, err := sh.GetDocByID(ctx, docID)
	if err != nil || !found {
		return DocGetResult{Found: found}, err
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
	return DocGetResult{
		Found:       true,
		Source:      raw,
		CRC32C:      crc,
		SeqNo:       dm.Seq,
		PrimaryTerm: sh.PrimaryTerm(),
		Version:     dm.Seq,
		S3Pointer:   ptr,
	}, nil
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
		if flushToS3 && sh.IsPrimary() && n.s3 != nil {
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

// Count returns the matching document count across indexPatterns without fetching _source payloads.
func (n *Node) Count(ctx context.Context, indexPatterns []string, queryDSL map[string]any, preferReader bool) (map[string]any, error) {
	zero := 0
	res, err := n.SearchWithPreference(ctx, indexPatterns, query.SearchRequest{
		Query: queryDSL,
		Size:  &zero,
	}, RetrievalDropIn, preferReader)
	if err != nil {
		return nil, err
	}
	total := res["hits"].(map[string]any)["total"].(map[string]any)["value"]
	return map[string]any{
		"count":   total,
		"_shards": res["_shards"],
	}, nil
}

// SearchWithPreference executes distributed search, optionally routing shard reads to stateless
// reader-role nodes first (falling back to data nodes if no readers are available).
func (n *Node) SearchWithPreference(ctx context.Context, indexNames []string, req query.SearchRequest, mode RetrievalMode, preferReader bool) (map[string]any, error) {
	start := time.Now()
	if mode == "" {
		mode = RetrievalDropIn
	}
	resolvedIndices, err := n.ResolveIndices(indexNames)
	if err != nil {
		return nil, err
	}
	if len(resolvedIndices) == 0 {
		return map[string]any{
			"took":      time.Since(start).Milliseconds(),
			"timed_out": false,
			"_shards": map[string]any{
				"total":      0,
				"successful": 0,
				"skipped":    0,
				"failed":     0,
			},
			"hits": map[string]any{
				"total": map[string]any{
					"value":    0,
					"relation": "eq",
				},
				"max_score": nil,
				"hits":      []map[string]any{},
			},
		}, nil
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
	for _, idxName := range resolvedIndices {
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

// ForceMergeRequest carries a peer RPC request to force-merge local primary shards of an index.
type ForceMergeRequest struct {
	IndexName string            `json:"index_name"`
	Options   ForceMergeOptions `json:"options"`
}

// ForceMergeSummary aggregates shard-level merge counts across an index or cluster.
type ForceMergeSummary struct {
	TotalShards       int `json:"total_shards"`
	SuccessfulShards  int `json:"successful_shards"`
	FailedShards      int `json:"failed_shards"`
	MergesExecuted    int `json:"merges_executed"`
	SegmentsMerged    int `json:"segments_merged"`
	TombstonesPurged  int `json:"tombstones_purged"`
	VersionsCompacted int `json:"versions_compacted"`
}

// ForceMerge compacts segments across all resolved indices (local and remote primary data nodes).
func (n *Node) ForceMerge(ctx context.Context, indexPatterns []string, opts ForceMergeOptions) (map[string]any, error) {
	resolved, err := n.ResolveIndices(indexPatterns)
	if err != nil {
		return nil, err
	}

	var total ForceMergeSummary
	for _, idxName := range resolved {
		localSum, err := n.ForceMergeLocal(ctx, idxName, opts)
		if err != nil {
			return nil, err
		}
		total.TotalShards += localSum.TotalShards
		total.SuccessfulShards += localSum.SuccessfulShards
		total.FailedShards += localSum.FailedShards
		total.MergesExecuted += localSum.MergesExecuted
		total.SegmentsMerged += localSum.SegmentsMerged
		total.TombstonesPurged += localSum.TombstonesPurged
		total.VersionsCompacted += localSum.VersionsCompacted

		for _, peer := range n.ring.DataNodes() {
			if peer.ID == n.cfg.NodeID || peer.Address == "" {
				continue
			}
			var peerSum ForceMergeSummary
			if err := n.postJSON(ctx, baseURL(peer.Address)+"/_internal/forcemerge", ForceMergeRequest{
				IndexName: idxName,
				Options:   opts,
			}, &peerSum); err != nil {
				total.FailedShards++
				continue
			}
			total.TotalShards += peerSum.TotalShards
			total.SuccessfulShards += peerSum.SuccessfulShards
			total.FailedShards += peerSum.FailedShards
			total.MergesExecuted += peerSum.MergesExecuted
			total.SegmentsMerged += peerSum.SegmentsMerged
			total.TombstonesPurged += peerSum.TombstonesPurged
			total.VersionsCompacted += peerSum.VersionsCompacted
		}
	}

	return map[string]any{
		"_shards": map[string]any{
			"total":      total.TotalShards,
			"successful": total.SuccessfulShards,
			"failed":     total.FailedShards,
		},
		"merges_executed":    total.MergesExecuted,
		"segments_merged":    total.SegmentsMerged,
		"tombstones_purged":  total.TombstonesPurged,
		"versions_compacted": total.VersionsCompacted,
	}, nil
}

// ForceMergeLocal executes ForceMerge on all primary shards of indexName hosted on this node.
func (n *Node) ForceMergeLocal(ctx context.Context, indexName string, opts ForceMergeOptions) (ForceMergeSummary, error) {
	n.mu.RLock()
	shardMap := n.shards[indexName]
	var primaries []*Shard
	for _, sh := range shardMap {
		if sh.IsPrimary() {
			primaries = append(primaries, sh)
		}
	}
	n.mu.RUnlock()

	var summary ForceMergeSummary
	for _, sh := range primaries {
		summary.TotalShards++
		select {
		case n.mergeSem <- struct{}{}:
		case <-ctx.Done():
			summary.FailedShards++
			return summary, ctx.Err()
		}
		res, err := sh.ForceMerge(ctx, opts)
		<-n.mergeSem
		if err != nil {
			summary.FailedShards++
			return summary, err
		}
		summary.SuccessfulShards++
		if res.Merged {
			n.recordMergeMetrics(res)
			summary.MergesExecuted++
			summary.SegmentsMerged += res.InputSegments
			summary.TombstonesPurged += res.TombstonesPurged
			summary.VersionsCompacted += res.VersionsCompacted
		}
	}
	return summary, nil
}

// LocalShardSegments returns ShardStats for local primary shards matching indexName (or all if empty).
func (n *Node) LocalShardSegments(indexName string) []ShardStats {
	n.mu.RLock()
	defer n.mu.RUnlock()
	var out []ShardStats
	for idx, m := range n.shards {
		if indexName != "" && idx != indexName {
			continue
		}
		for _, sh := range m {
			if sh.IsPrimary() {
				out = append(out, sh.Stats())
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].IndexName == out[j].IndexName {
			return out[i].ShardID < out[j].ShardID
		}
		return out[i].IndexName < out[j].IndexName
	})
	return out
}

// gatherClusterShardSegments collects primary shard segment metadata from this node and remote data peers.
func (n *Node) gatherClusterShardSegments(ctx context.Context, indexPatterns []string) ([]ShardStats, error) {
	resolved, err := n.ResolveIndices(indexPatterns)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(resolved))
	for _, name := range resolved {
		allowed[name] = struct{}{}
	}

	var all []ShardStats
	for _, st := range n.LocalShardSegments("") {
		if _, ok := allowed[st.IndexName]; ok {
			all = append(all, st)
		}
	}
	for _, peer := range n.ring.DataNodes() {
		if peer.ID == n.cfg.NodeID || peer.Address == "" {
			continue
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL(peer.Address)+"/_internal/segments", nil)
		if err != nil {
			continue
		}
		resp, err := n.httpClient.Do(req)
		if err != nil {
			continue
		}
		var peerStats []ShardStats
		if resp.StatusCode == http.StatusOK {
			_ = json.NewDecoder(resp.Body).Decode(&peerStats)
		}
		_ = resp.Body.Close()
		for _, st := range peerStats {
			if _, ok := allowed[st.IndexName]; ok {
				all = append(all, st)
			}
		}
	}
	sort.Slice(all, func(i, j int) bool {
		if all[i].IndexName == all[j].IndexName {
			return all[i].ShardID < all[j].ShardID
		}
		return all[i].IndexName < all[j].IndexName
	})
	return all, nil
}

// IndexSegments returns an Elasticsearch-formatted GET /{index}/_segments response.
func (n *Node) IndexSegments(ctx context.Context, indexPatterns []string) (map[string]any, error) {
	shards, err := n.gatherClusterShardSegments(ctx, indexPatterns)
	if err != nil {
		return nil, err
	}

	indicesOut := make(map[string]any)
	for _, st := range shards {
		idxObj, ok := indicesOut[st.IndexName].(map[string]any)
		if !ok {
			idxObj = map[string]any{"shards": map[string]any{}}
			indicesOut[st.IndexName] = idxObj
		}
		shardsMap := idxObj["shards"].(map[string]any)
		segMap := make(map[string]any, len(st.Segments))
		for _, seg := range st.Segments {
			segMap[seg.ID] = map[string]any{
				"num_docs":       seg.DocCount,
				"deleted_docs":   seg.DeletedCount,
				"size_in_bytes":  seg.SizeBytes,
				"min_seq":        seg.MinSeq,
				"max_seq":        seg.MaxSeq,
				"idx_key":        seg.IdxKey,
				"docs_key":       seg.DocsKey,
				"committed":      true,
				"search":         true,
				"storage_engine": "s3+nvme",
			}
		}
		shardsMap[strconv.Itoa(st.ShardID)] = []map[string]any{{
			"routing": map[string]any{
				"state":   "STARTED",
				"primary": st.IsPrimary,
			},
			"num_committed_segments": len(st.Segments),
			"pending_gc_objects":     st.PendingGCObjects,
			"segments":               segMap,
		}}
	}

	return map[string]any{
		"_shards": map[string]any{
			"total":      len(shards),
			"successful": len(shards),
			"failed":     0,
		},
		"indices": indicesOut,
	}, nil
}

// CatSegments returns a tabular Elasticsearch GET /_cat/segments JSON array.
func (n *Node) CatSegments(ctx context.Context, indexPatterns []string) ([]map[string]any, error) {
	shards, err := n.gatherClusterShardSegments(ctx, indexPatterns)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0)
	for _, st := range shards {
		for _, seg := range st.Segments {
			out = append(out, map[string]any{
				"index":        st.IndexName,
				"shard":        st.ShardID,
				"prirep":       "p",
				"segment":      seg.ID,
				"docs.count":   seg.DocCount,
				"docs.deleted": seg.DeletedCount,
				"size.bytes":   seg.SizeBytes,
				"committed":    true,
				"searchable":   true,
			})
		}
	}
	return out, nil
}

// RunMergeAndGCOnce runs one tiered merge evaluation and S3 GC sweep across local primary shards.
func (n *Node) RunMergeAndGCOnce(ctx context.Context) (mergesRun int, gcDeleted int) {
	if !n.HasRole(sharding.RoleData) || n.s3 == nil {
		return 0, 0
	}
	n.mu.RLock()
	var primaries []*Shard
	for _, m := range n.shards {
		for _, sh := range m {
			if sh.IsPrimary() {
				primaries = append(primaries, sh)
			}
		}
	}
	n.mu.RUnlock()

	for _, sh := range primaries {
		select {
		case n.mergeSem <- struct{}{}:
			res, err := sh.MergeIfNeeded(ctx, n.cfg.Merge)
			<-n.mergeSem
			if err == nil && res.Merged {
				n.recordMergeMetrics(res)
				mergesRun++
			}
		default:
			// Concurrency gate saturated: skip to avoid competing with active merges.
		}

		if gcRes, err := sh.SweepRetiredS3Objects(ctx, n.cfg.Merge.S3GCGrace); err == nil && gcRes.ObjectsDeleted > 0 {
			n.s3ObjectsReclaimed.Add(int64(gcRes.ObjectsDeleted))
			gcDeleted += gcRes.ObjectsDeleted
		}
	}
	return mergesRun, gcDeleted
}

func (n *Node) recordMergeMetrics(res MergeResult) {
	n.mergesCompleted.Add(1)
	n.segmentsMerged.Add(int64(res.InputSegments))
	n.tombstonesPurged.Add(int64(res.TombstonesPurged))
	n.versionsCompacted.Add(int64(res.VersionsCompacted))
}

func (n *Node) mergeLoop() {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.Merge.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.RunMergeAndGCOnce(context.Background())
		}
	}
}

// ReplicationStats reports async WAL replication, S3 flush, segment merge, failover, and GC diagnostics.
type ReplicationStats struct {
	NodeID                 string       `json:"node_id"`
	Roles                  []string     `json:"roles"`
	DiscoveryMode          string       `json:"discovery_mode"`
	Draining               bool         `json:"draining"`
	PromotionsCompleted    int64        `json:"promotions_completed"`
	ReplicationBatchesSent int64        `json:"replication_batches_sent"`
	ReplicationErrors      int64        `json:"replication_errors"`
	S3SegmentFlushes       int64        `json:"s3_segment_flushes"`
	MergesCompleted        int64        `json:"merges_completed"`
	SegmentsMerged         int64        `json:"segments_merged"`
	TombstonesPurged       int64        `json:"tombstones_purged"`
	VersionsCompacted      int64        `json:"versions_compacted"`
	S3ObjectsReclaimed     int64        `json:"s3_objects_reclaimed"`
	PendingGCObjects       int          `json:"pending_gc_objects"`
	NVMeCache              cache.Stats  `json:"nvme_cache"`
	Shards                 []ShardStats `json:"shards"`
}

// Stats returns node-level replication, flush, merge, failover, GC, NVMe cache, and shard diagnostics.
func (n *Node) Stats() ReplicationStats {
	n.mu.RLock()
	var shardStats []ShardStats
	pendingGC := 0
	for _, m := range n.shards {
		for _, sh := range m {
			st := sh.Stats()
			pendingGC += st.PendingGCObjects
			shardStats = append(shardStats, st)
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
		DiscoveryMode:          n.cfg.Discovery.Mode,
		Draining:               n.draining.Load(),
		PromotionsCompleted:    n.promotionsCompleted.Load(),
		ReplicationBatchesSent: n.repSuccess.Load(),
		ReplicationErrors:      n.repErrors.Load(),
		S3SegmentFlushes:       n.flushes.Load(),
		MergesCompleted:        n.mergesCompleted.Load(),
		SegmentsMerged:         n.segmentsMerged.Load(),
		TombstonesPurged:       n.tombstonesPurged.Load(),
		VersionsCompacted:      n.versionsCompacted.Load(),
		S3ObjectsReclaimed:     n.s3ObjectsReclaimed.Load(),
		PendingGCObjects:       pendingGC,
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
			if sh.IsPrimary() {
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
					if sh.IsPrimary() {
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
	if resp.StatusCode == http.StatusConflict {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("%w: %s", ErrVersionConflict, strings.TrimSpace(string(msg)))
	}
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
