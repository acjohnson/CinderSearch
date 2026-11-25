package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cindersearch/cindersearch/internal/s3store"
	"github.com/cindersearch/cindersearch/internal/sharding"
)

const clusterNodeLeasePrefix = "_cluster/nodes/"

const (
	// DiscoveryModeHybrid publishes and polls S3 node leases when S3 is configured,
	// while still honoring static --peers seeds during bootstrap.
	DiscoveryModeHybrid = "hybrid"
	// DiscoveryModeS3 discovers active peers exclusively from S3 node leases.
	DiscoveryModeS3 = "s3"
	// DiscoveryModeStatic disables S3 membership polling and uses static --peers / UpdatePeers only.
	DiscoveryModeStatic = "static"
)

const (
	NodeStateActive   = "active"
	NodeStateDraining = "draining"
)

// DiscoveryConfig configures gossip-free dynamic cluster membership via S3 heartbeat leases.
type DiscoveryConfig struct {
	Mode              string
	HeartbeatInterval time.Duration
	LeaseTTL          time.Duration
	SyncInterval      time.Duration
}

// DefaultDiscoveryConfig returns production defaults for dynamic S3 membership discovery.
func DefaultDiscoveryConfig() DiscoveryConfig {
	return DiscoveryConfig{
		Mode:              DiscoveryModeHybrid,
		HeartbeatInterval: 5 * time.Second,
		LeaseTTL:          15 * time.Second,
		SyncInterval:      5 * time.Second,
	}
}

// Normalize fills default durations and validates discovery mode.
func (c DiscoveryConfig) Normalize(hasAdvertiseAddr bool) (DiscoveryConfig, error) {
	def := DefaultDiscoveryConfig()
	mode := strings.ToLower(strings.TrimSpace(c.Mode))
	if mode == "" {
		if hasAdvertiseAddr {
			mode = DiscoveryModeHybrid
		} else {
			mode = DiscoveryModeStatic
		}
	}
	switch mode {
	case DiscoveryModeHybrid, DiscoveryModeS3, DiscoveryModeStatic:
		c.Mode = mode
	default:
		return DiscoveryConfig{}, fmt.Errorf("unsupported discovery mode %q (supported: hybrid, s3, static)", c.Mode)
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = def.HeartbeatInterval
	}
	if c.LeaseTTL <= 0 {
		c.LeaseTTL = def.LeaseTTL
	}
	if c.LeaseTTL <= c.HeartbeatInterval {
		c.LeaseTTL = c.HeartbeatInterval * 3
	}
	if c.SyncInterval <= 0 {
		c.SyncInterval = def.SyncInterval
	}
	return c, nil
}

// NodeLease is the heartbeat record persisted by each node under _cluster/nodes/<nodeID>.json.
type NodeLease struct {
	ID              string   `json:"id"`
	Address         string   `json:"address"`
	Roles           []string `json:"roles"`
	State           string   `json:"state"`
	StartedAt       int64    `json:"started_at"`
	LastHeartbeatMS int64    `json:"last_heartbeat_ms"`
	Epoch           uint64   `json:"epoch"`
}

// DrainResult reports the outcome of POST /_cinder/drain on a node.
type DrainResult struct {
	NodeID          string `json:"node_id"`
	Draining        bool   `json:"draining"`
	ShardsFlushed   int    `json:"shards_flushed"`
	SegmentsFlushed int    `json:"segments_flushed"`
	LeaseRemoved    bool   `json:"lease_removed"`
}

// MembershipSnapshot reports active & draining leases plus cluster membership metadata.
type MembershipSnapshot struct {
	NodeID              string              `json:"node_id"`
	DiscoveryMode       string              `json:"discovery_mode"`
	Draining            bool                `json:"draining"`
	MembershipVersion   uint64              `json:"membership_version"`
	PromotionsCompleted int64               `json:"promotions_completed"`
	ActiveNodes         []sharding.NodeInfo `json:"active_nodes"`
	Leases              []NodeLease         `json:"leases,omitempty"`
}

func clusterNodeLeaseKey(nodeID string) string {
	return clusterNodeLeasePrefix + nodeID + ".json"
}

func (n *Node) discoveryEnabled() bool {
	return n.s3 != nil && n.cfg.Discovery.Mode != DiscoveryModeStatic
}

// AdvertiseAddress returns this node's current advertised host:port address.
func (n *Node) AdvertiseAddress() string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.advertiseAddr
}

// SetAdvertiseAddress updates the address published in this node's S3 membership lease.
func (n *Node) SetAdvertiseAddress(addr string) {
	n.mu.Lock()
	n.advertiseAddr = strings.TrimSpace(addr)
	n.mu.Unlock()
}

// PublishLease writes or refreshes this node's heartbeat lease in S3.
func (n *Node) PublishLease(ctx context.Context) error {
	if !n.discoveryEnabled() {
		return nil
	}
	state := NodeStateActive
	if n.draining.Load() {
		state = NodeStateDraining
	}
	epoch := n.leaseEpoch.Add(1)
	lease := NodeLease{
		ID:              n.cfg.NodeID,
		Address:         n.AdvertiseAddress(),
		Roles:           n.Roles(),
		State:           state,
		StartedAt:       n.startedAt,
		LastHeartbeatMS: time.Now().UTC().UnixMilli(),
		Epoch:           epoch,
	}
	raw, err := json.Marshal(lease)
	if err != nil {
		return err
	}
	_, err = n.s3.PutObject(ctx, clusterNodeLeaseKey(n.cfg.NodeID), raw)
	return err
}

// DeleteLease removes this node's lease object from S3 (used during graceful drain/shutdown).
func (n *Node) DeleteLease(ctx context.Context) error {
	if !n.discoveryEnabled() {
		return nil
	}
	err := n.s3.DeleteObject(ctx, clusterNodeLeaseKey(n.cfg.NodeID))
	if errors.Is(err, s3store.ErrNotFound) {
		return nil
	}
	return err
}

// ListNodeLeases fetches all currently stored node leases from S3.
func (n *Node) ListNodeLeases(ctx context.Context) ([]NodeLease, error) {
	if n.s3 == nil {
		return nil, nil
	}
	objs, err := n.s3.ListObjects(ctx, clusterNodeLeasePrefix)
	if err != nil {
		return nil, err
	}
	out := make([]NodeLease, 0, len(objs))
	for _, obj := range objs {
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		raw, _, err := n.s3.GetObject(ctx, obj.Key)
		if err != nil {
			continue
		}
		var lease NodeLease
		if json.Unmarshal(raw, &lease) == nil && lease.ID != "" {
			out = append(out, lease)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// SyncMembershipOnce polls S3 node leases and cluster index metadata, updates the consistent hash ring
// atomically (evicting expired/draining nodes), and reconciles local shard ownership (including
// automated replica-to-primary promotion). Returns true if ring membership or indices changed.
func (n *Node) SyncMembershipOnce(ctx context.Context) (bool, error) {
	if !n.discoveryEnabled() {
		return false, nil
	}

	// Ensure our own lease is fresh before evaluating peers.
	if !n.draining.Load() {
		_ = n.PublishLease(ctx)
	}

	leases, err := n.ListNodeLeases(ctx)
	if err != nil {
		return false, err
	}

	now := time.Now().UTC()
	ttl := n.cfg.Discovery.LeaseTTL
	active := make([]sharding.NodeInfo, 0, len(leases)+1)
	seenSelf := false

	for _, lease := range leases {
		if lease.State == NodeStateDraining {
			continue
		}
		hbTime := time.UnixMilli(lease.LastHeartbeatMS).UTC()
		if now.Sub(hbTime) > ttl {
			continue
		}
		roles := lease.Roles
		if len(roles) == 0 {
			roles = []string{sharding.RoleData}
		}
		active = append(active, sharding.NodeInfo{
			ID:      lease.ID,
			Address: lease.Address,
			Roles:   roles,
		})
		if lease.ID == n.cfg.NodeID {
			seenSelf = true
		}
	}

	// Keep self in the active set unless explicitly draining.
	if !seenSelf && !n.draining.Load() {
		active = append(active, sharding.NodeInfo{
			ID:      n.cfg.NodeID,
			Address: n.AdvertiseAddress(),
			Roles:   n.Roles(),
		})
	}

	ringChanged := false
	if len(active) > 0 {
		ringChanged = n.ring.ReplaceNodes(active)
		if ringChanged {
			n.membershipVer.Add(1)
		}
	}

	// Sync any indices created or deleted on other nodes from S3 metadata.
	indicesChanged, metaErr := n.syncIndexMetadataFromS3(ctx)

	if ringChanged || indicesChanged {
		if err := n.ReconcileShards(ctx); err != nil {
			return ringChanged || indicesChanged, err
		}
	}

	return ringChanged || indicesChanged, metaErr
}

// syncIndexMetadataFromS3 reconciles n.indices against _cluster/indices/*.json in S3.
func (n *Node) syncIndexMetadataFromS3(ctx context.Context) (bool, error) {
	if n.s3 == nil {
		return false, nil
	}
	objs, err := n.s3.ListObjects(ctx, clusterIndexMetaPrefix)
	if err != nil {
		return false, err
	}

	remote := make(map[string]IndexMetadata, len(objs))
	for _, obj := range objs {
		if !strings.HasSuffix(obj.Key, ".json") {
			continue
		}
		raw, _, err := n.s3.GetObject(ctx, obj.Key)
		if err != nil {
			continue
		}
		var meta IndexMetadata
		if json.Unmarshal(raw, &meta) == nil && meta.Name != "" {
			remote[meta.Name] = meta
		}
	}

	changed := false
	var toApply []IndexMetadata
	var toDelete []string

	n.mu.RLock()
	for name, rMeta := range remote {
		cur, exists := n.indices[name]
		if !exists || !indexMetaEqual(cur, rMeta) {
			toApply = append(toApply, rMeta)
		}
	}
	for name := range n.indices {
		if _, exists := remote[name]; !exists {
			toDelete = append(toDelete, name)
		}
	}
	n.mu.RUnlock()

	for _, meta := range toApply {
		if err := n.ApplyIndexMetadata(ctx, meta); err == nil {
			changed = true
		}
	}
	for _, name := range toDelete {
		_ = n.DeleteIndex(ctx, name, false)
		changed = true
	}
	return changed, nil
}

func indexMetaEqual(a, b IndexMetadata) bool {
	if a.Name != b.Name || a.Settings != b.Settings || len(a.Aliases) != len(b.Aliases) {
		return false
	}
	for i := range a.Aliases {
		if a.Aliases[i] != b.Aliases[i] {
			return false
		}
	}
	aMap, _ := json.Marshal(a.Mappings)
	bMap, _ := json.Marshal(b.Mappings)
	return string(aMap) == string(bMap)
}

// ReconcileShards aligns local shard instances with the current consistent hash ring:
//   - Promotes surviving replicas to primary (incrementing primary_term in S3) when primary fails.
//   - Demotes displaced primaries after flushing uncommitted WAL records to S3.
//   - Opens newly assigned shards on scaled-out data/reader nodes directly from S3 manifests.
//   - Closes shards no longer assigned to this node.
func (n *Node) ReconcileShards(ctx context.Context) error {
	n.reconcileMu.Lock()
	defer n.reconcileMu.Unlock()

	indices := n.ListIndices()
	if len(indices) == 0 {
		return nil
	}

	if n.HasRole(sharding.RoleData) {
		draining := n.draining.Load()
		for _, meta := range indices {
			if draining {
				continue
			}
			placements, err := n.ring.AllPlacements(meta.Name, meta.Settings.NumberOfShards, meta.Settings.NumberOfReplicas)
			if err != nil {
				continue
			}
			desiredPrimary := make(map[int]bool, len(placements))
			desiredReplica := make(map[int]bool, len(placements))
			for _, p := range placements {
				if p.Primary.ID == n.cfg.NodeID {
					desiredPrimary[p.ShardID] = true
				}
				for _, rep := range p.Replicas {
					if rep.ID == n.cfg.NodeID {
						desiredReplica[p.ShardID] = true
						break
					}
				}
			}

			n.mu.Lock()
			if _, ok := n.shards[meta.Name]; !ok {
				n.shards[meta.Name] = make(map[int]*Shard)
			}
			existing := make(map[int]*Shard, len(n.shards[meta.Name]))
			for sid, sh := range n.shards[meta.Name] {
				existing[sid] = sh
			}
			n.mu.Unlock()

			// 1. Handle desired primary and replica shards.
			for _, p := range placements {
				sid := p.ShardID
				sh := existing[sid]
				if desiredPrimary[sid] {
					if sh == nil {
						opened, err := OpenShard(ctx, meta.Name, sid, true, n.cfg.NVMeDir, n.nvmeCache, n.s3)
						if err != nil {
							return err
						}
						opened.SetOwnerNodeID(n.cfg.NodeID)
						// If S3 already had a manifest owned by a different primary, bump primary_term to fence it.
						opened.mu.RLock()
						prevOwner := opened.primaryNode
						opened.mu.RUnlock()
						if prevOwner != "" && prevOwner != n.cfg.NodeID {
							if _, err := opened.PromoteToPrimary(ctx, n.cfg.NodeID); err != nil {
								_ = opened.Close()
								return err
							}
							n.promotionsCompleted.Add(1)
						}
						n.mu.Lock()
						n.shards[meta.Name][sid] = opened
						n.mu.Unlock()
					} else if !sh.IsPrimary() {
						if _, err := sh.PromoteToPrimary(ctx, n.cfg.NodeID); err != nil {
							return err
						}
						n.promotionsCompleted.Add(1)
					}
				} else if desiredReplica[sid] {
					if sh == nil {
						opened, err := OpenShard(ctx, meta.Name, sid, false, n.cfg.NVMeDir, n.nvmeCache, n.s3)
						if err != nil {
							return err
						}
						opened.SetOwnerNodeID(n.cfg.NodeID)
						n.mu.Lock()
						n.shards[meta.Name][sid] = opened
						n.mu.Unlock()
					} else if sh.IsPrimary() {
						_, _ = sh.FlushToS3(ctx)
						sh.DemoteToReplica()
					}
				}
			}

			// 2. Close shards no longer owned by this node.
			for sid, sh := range existing {
				if desiredPrimary[sid] || desiredReplica[sid] {
					continue
				}
				if sh.IsPrimary() {
					_, _ = sh.FlushToS3(ctx)
					sh.DemoteToReplica()
				}
				n.mu.Lock()
				delete(n.shards[meta.Name], sid)
				n.mu.Unlock()
				_ = sh.Close()
			}
		}
		return nil
	}

	if n.HasRole(sharding.RoleReader) {
		for _, meta := range indices {
			n.mu.Lock()
			if _, ok := n.shards[meta.Name]; !ok {
				n.shards[meta.Name] = make(map[int]*Shard)
			}
			n.mu.Unlock()
			for sid := 0; sid < meta.Settings.NumberOfShards; sid++ {
				n.mu.RLock()
				_, exists := n.shards[meta.Name][sid]
				n.mu.RUnlock()
				if exists {
					continue
				}
				sh, err := OpenReadOnlyShard(ctx, meta.Name, sid, n.nvmeCache, n.s3)
				if err != nil {
					return err
				}
				n.mu.Lock()
				if _, stillMissing := n.shards[meta.Name][sid]; !stillMissing {
					n.shards[meta.Name][sid] = sh
				} else {
					_ = sh.Close()
				}
				n.mu.Unlock()
			}
		}
	}

	return nil
}

// Drain gracefully prepares this node for termination or rolling upgrade:
// marks itself draining, flushes all primary shard WALs to S3, demotes primaries,
// and removes its membership lease from S3 so peers immediately promote replicas.
func (n *Node) Drain(ctx context.Context) (DrainResult, error) {
	n.draining.Store(true)
	if n.discoveryEnabled() {
		_ = n.PublishLease(ctx)
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

	shardsFlushed := 0
	segmentsFlushed := 0
	for _, sh := range primaries {
		ref, err := sh.FlushToS3(ctx)
		if err == nil {
			shardsFlushed++
			if ref != nil {
				segmentsFlushed++
				n.flushes.Add(1)
			}
		}
		sh.DemoteToReplica()
	}

	leaseRemoved := false
	if n.discoveryEnabled() {
		if err := n.DeleteLease(ctx); err == nil {
			leaseRemoved = true
		}
	}

	return DrainResult{
		NodeID:          n.cfg.NodeID,
		Draining:        true,
		ShardsFlushed:   shardsFlushed,
		SegmentsFlushed: segmentsFlushed,
		LeaseRemoved:    leaseRemoved,
	}, nil
}

// MembershipSnapshot returns current discovery mode, active ring nodes, and S3 leases.
func (n *Node) MembershipSnapshot(ctx context.Context) MembershipSnapshot {
	snap := MembershipSnapshot{
		NodeID:              n.cfg.NodeID,
		DiscoveryMode:       n.cfg.Discovery.Mode,
		Draining:            n.draining.Load(),
		MembershipVersion:   n.membershipVer.Load(),
		PromotionsCompleted: n.promotionsCompleted.Load(),
		ActiveNodes:         n.ring.Nodes(),
	}
	if n.s3 != nil {
		if leases, err := n.ListNodeLeases(ctx); err == nil {
			snap.Leases = leases
		}
	}
	return snap
}

func (n *Node) membershipLoop() {
	defer n.wg.Done()
	hbTicker := time.NewTicker(n.cfg.Discovery.HeartbeatInterval)
	syncTicker := time.NewTicker(n.cfg.Discovery.SyncInterval)
	defer hbTicker.Stop()
	defer syncTicker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-hbTicker.C:
			if !n.draining.Load() {
				_ = n.PublishLease(context.Background())
			}
		case <-syncTicker.C:
			_, _ = n.SyncMembershipOnce(context.Background())
		}
	}
}

// ClusterHealth computes official Elasticsearch v8 /_cluster/health metrics from the live ring.
func (n *Node) ClusterHealth(indexPatterns []string, level string) (map[string]any, error) {
	var targetIndices []IndexMetadata
	if len(indexPatterns) == 0 {
		targetIndices = n.ListIndices()
	} else {
		resolved, err := n.ResolveIndices(indexPatterns)
		if err != nil {
			return nil, err
		}
		for _, name := range resolved {
			if meta, ok := n.GetIndex(name); ok {
				targetIndices = append(targetIndices, meta)
			}
		}
	}

	allNodes := n.ring.Nodes()
	dataNodes := n.ring.DataNodes()
	dataCount := len(dataNodes)

	clusterStatus := "green"
	if dataCount == 0 {
		clusterStatus = "red"
	}

	totalActivePrimary := 0
	totalActiveShards := 0
	totalUnassigned := 0
	indicesDetail := make(map[string]any, len(targetIndices))

	for _, meta := range targetIndices {
		numShards := meta.Settings.NumberOfShards
		if numShards <= 0 {
			numShards = 1
		}
		numReplicas := meta.Settings.NumberOfReplicas
		if numReplicas < 0 {
			numReplicas = 0
		}

		var idxActivePri, idxActive, idxUnassigned int
		idxStatus := "green"

		if dataCount == 0 {
			idxActivePri = 0
			idxActive = 0
			idxUnassigned = numShards * (1 + numReplicas)
			idxStatus = "red"
		} else {
			assignedRepPerShard := numReplicas
			if assignedRepPerShard > dataCount-1 {
				assignedRepPerShard = dataCount - 1
			}
			idxActivePri = numShards
			idxActive = numShards * (1 + assignedRepPerShard)
			idxUnassigned = numShards * (numReplicas - assignedRepPerShard)
			if idxUnassigned > 0 {
				idxStatus = "yellow"
			}
		}

		totalActivePrimary += idxActivePri
		totalActiveShards += idxActive
		totalUnassigned += idxUnassigned

		if healthRank(idxStatus) < healthRank(clusterStatus) {
			clusterStatus = idxStatus
		}

		if level == "indices" || level == "shards" {
			idxEntry := map[string]any{
				"status":                idxStatus,
				"number_of_shards":      numShards,
				"number_of_replicas":    numReplicas,
				"active_primary_shards": idxActivePri,
				"active_shards":         idxActive,
				"relocating_shards":     0,
				"initializing_shards":   0,
				"unassigned_shards":     idxUnassigned,
			}
			if level == "shards" {
				shardsDetail := make(map[string]any, numShards)
				for s := 0; s < numShards; s++ {
					sActivePri := 0
					sActive := 0
					sUnassigned := 1 + numReplicas
					sStatus := "red"
					if dataCount > 0 {
						repCount := numReplicas
						if repCount > dataCount-1 {
							repCount = dataCount - 1
						}
						sActivePri = 1
						sActive = 1 + repCount
						sUnassigned = numReplicas - repCount
						sStatus = "green"
						if sUnassigned > 0 {
							sStatus = "yellow"
						}
					}
					shardsDetail[strconv.Itoa(s)] = map[string]any{
						"status":              sStatus,
						"primary_active":      sActivePri == 1,
						"active_shards":       sActive,
						"relocating_shards":   0,
						"initializing_shards": 0,
						"unassigned_shards":   sUnassigned,
					}
				}
				idxEntry["shards"] = shardsDetail
			}
			indicesDetail[meta.Name] = idxEntry
		}
	}

	activePct := 100.0
	if totalShards := totalActiveShards + totalUnassigned; totalShards > 0 {
		activePct = (float64(totalActiveShards) / float64(totalShards)) * 100.0
	}

	resp := map[string]any{
		"cluster_name":                    "cindersearch-cluster",
		"status":                          clusterStatus,
		"timed_out":                       false,
		"number_of_nodes":                 len(allNodes),
		"number_of_data_nodes":            dataCount,
		"active_primary_shards":           totalActivePrimary,
		"active_shards":                   totalActiveShards,
		"relocating_shards":               0,
		"initializing_shards":             0,
		"unassigned_shards":               totalUnassigned,
		"delayed_unassigned_shards":       0,
		"number_of_pending_tasks":         0,
		"number_of_in_flight_fetch":       0,
		"task_max_waiting_in_queue_millis": 0,
		"active_shards_percent_as_number": activePct,
		"number_of_indices":               len(targetIndices),
		"storage_backend":                 "s3+nvme",
		"checksum_algorithm":              "crc32c",
	}
	if level == "indices" || level == "shards" {
		resp["indices"] = indicesDetail
	}
	return resp, nil
}

// WaitForClusterHealth blocks until cluster health reaches waitForStatus ("green"|"yellow"|"red")
// or timeout elapses, matching Elasticsearch GET /_cluster/health?wait_for_status=...&timeout=... semantics.
func (n *Node) WaitForClusterHealth(ctx context.Context, indexPatterns []string, level, waitForStatus string, timeout time.Duration) (map[string]any, error) {
	waitForStatus = strings.ToLower(strings.TrimSpace(waitForStatus))
	if waitForStatus == "" {
		return n.ClusterHealth(indexPatterns, level)
	}
	if waitForStatus != "green" && waitForStatus != "yellow" && waitForStatus != "red" {
		return nil, fmt.Errorf("illegal_argument_exception: invalid wait_for_status [%s]", waitForStatus)
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}

	deadline := time.Now().Add(timeout)
	for {
		health, err := n.ClusterHealth(indexPatterns, level)
		if err != nil {
			return nil, err
		}
		curStatus, _ := health["status"].(string)
		if healthRank(curStatus) >= healthRank(waitForStatus) {
			return health, nil
		}
		if time.Now().After(deadline) {
			health["timed_out"] = true
			return health, nil
		}
		select {
		case <-ctx.Done():
			health["timed_out"] = true
			return health, nil
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func healthRank(status string) int {
	switch status {
	case "green":
		return 3
	case "yellow":
		return 2
	default:
		return 1
	}
}

// ClusterState returns an official Elasticsearch v8 /_cluster/state snapshot covering
// nodes, metadata.indices, and routing_table.indices.
func (n *Node) ClusterState(metrics []string) map[string]any {
	includeAll := len(metrics) == 0
	want := make(map[string]bool, len(metrics))
	for _, m := range metrics {
		m = strings.ToLower(strings.TrimSpace(m))
		if m == "_all" || m == "" {
			includeAll = true
		} else {
			want[m] = true
		}
	}

	resp := map[string]any{
		"cluster_name": "cindersearch-cluster",
		"state_uuid":   "cindersearch-state-v1",
		"version":      n.membershipVer.Load() + 1,
	}

	if includeAll || want["nodes"] {
		nodes := n.ring.Nodes()
		nodeMap := make(map[string]any, len(nodes))
		for _, nd := range nodes {
			nodeMap[nd.ID] = map[string]any{
				"name":              nd.ID,
				"transport_address": nd.Address,
				"roles":             nd.NormalizedRoles(),
			}
		}
		resp["nodes"] = nodeMap
	}

	indices := n.ListIndices()
	if includeAll || want["metadata"] {
		idxMeta := make(map[string]any, len(indices))
		for _, idx := range indices {
			aliases := idx.Aliases
			if aliases == nil {
				aliases = []string{}
			}
			mappings := idx.Mappings
			if mappings == nil {
				mappings = map[string]any{}
			}
			idxMeta[idx.Name] = map[string]any{
				"state":    "open",
				"settings": map[string]any{"index": idx.Settings},
				"mappings": mappings,
				"aliases":  aliases,
			}
		}
		resp["metadata"] = map[string]any{
			"indices": idxMeta,
		}
	}

	if includeAll || want["routing_table"] {
		routingIndices := make(map[string]any, len(indices))
		for _, idx := range indices {
			placements, err := n.ring.AllPlacements(idx.Name, idx.Settings.NumberOfShards, idx.Settings.NumberOfReplicas)
			if err != nil {
				continue
			}
			shardsMap := make(map[string]any, len(placements))
			for _, p := range placements {
				copies := make([]map[string]any, 0, 1+idx.Settings.NumberOfReplicas)
				copies = append(copies, map[string]any{
					"state":   "STARTED",
					"primary": true,
					"node":    p.Primary.ID,
					"shard":   p.ShardID,
					"index":   idx.Name,
				})
				for _, rep := range p.Replicas {
					copies = append(copies, map[string]any{
						"state":   "STARTED",
						"primary": false,
						"node":    rep.ID,
						"shard":   p.ShardID,
						"index":   idx.Name,
					})
				}
				for u := len(p.Replicas); u < idx.Settings.NumberOfReplicas; u++ {
					copies = append(copies, map[string]any{
						"state":   "UNASSIGNED",
						"primary": false,
						"node":    nil,
						"shard":   p.ShardID,
						"index":   idx.Name,
					})
				}
				shardsMap[strconv.Itoa(p.ShardID)] = copies
			}
			routingIndices[idx.Name] = map[string]any{"shards": shardsMap}
		}
		resp["routing_table"] = map[string]any{
			"indices": routingIndices,
		}
	}

	return resp
}

// PrimaryTermForShard returns the current primary_term for a local shard (defaulting to 1).
func (n *Node) PrimaryTermForShard(indexName string, shardID int) uint64 {
	n.mu.RLock()
	var sh *Shard
	if m := n.shards[indexName]; m != nil {
		sh = m[shardID]
	}
	n.mu.RUnlock()
	if sh == nil {
		return 1
	}
	return sh.PrimaryTerm()
}
