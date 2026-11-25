package sharding

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
)

const (
	// RoleData stores shards locally (NVMe WAL + S3 flush + NVMe LRU cache). Default role.
	RoleData = "data"
	// RoleCoordinator acts as a stateless HTTP router/aggregator without owning shards or WALs.
	RoleCoordinator = "coordinator"
	// RoleReader serves read-only searches directly from S3 + local NVMe cache without WALs.
	RoleReader = "reader"
)

// NormalizeRoles validates and canonicalizes node roles (defaulting empty to ["data"]).
func NormalizeRoles(roles []string) ([]string, error) {
	if len(roles) == 0 {
		return []string{RoleData}, nil
	}
	seen := make(map[string]struct{}, len(roles))
	out := make([]string, 0, len(roles))
	for _, r := range roles {
		role := strings.ToLower(strings.TrimSpace(r))
		if role == "" {
			continue
		}
		switch role {
		case RoleData, RoleCoordinator, RoleReader:
		default:
			return nil, fmt.Errorf("unsupported node role %q (supported: data, coordinator, reader)", r)
		}
		if _, ok := seen[role]; !ok {
			seen[role] = struct{}{}
			out = append(out, role)
		}
	}
	if len(out) == 0 {
		return []string{RoleData}, nil
	}
	sort.Strings(out)
	return out, nil
}

// NodeInfo describes a cluster peer participating in the cluster ring.
type NodeInfo struct {
	ID      string   `json:"id"`
	Address string   `json:"address"`
	Roles   []string `json:"roles,omitempty"`
}

// NormalizedRoles returns the node's roles, defaulting to ["data"] when unspecified.
func (n NodeInfo) NormalizedRoles() []string {
	if len(n.Roles) == 0 {
		return []string{RoleData}
	}
	return n.Roles
}

// HasRole reports whether the node carries the given role.
func (n NodeInfo) HasRole(role string) bool {
	for _, r := range n.NormalizedRoles() {
		if strings.EqualFold(r, role) {
			return true
		}
	}
	return false
}

// IsDataNode reports whether this node owns primary/replica shards in the hash ring.
func (n NodeInfo) IsDataNode() bool {
	return n.HasRole(RoleData)
}

// Placement defines the primary and replica owners for a given shard.
type Placement struct {
	ShardID  int        `json:"shard_id"`
	Primary  NodeInfo   `json:"primary"`
	Replicas []NodeInfo `json:"replicas"`
}

type vnodeEntry struct {
	hash   uint64
	nodeID string
}

// Ring maps documents onto logical shards and distributes primary/replica
// ownership across data-role cluster nodes using consistent hashing with virtual nodes.
type Ring struct {
	mu      sync.RWMutex
	vnodes  int
	nodes   map[string]NodeInfo
	ring    []vnodeEntry
	dataCnt int
}

// NewRing constructs a consistent hash ring with the requested virtual nodes per physical data node.
func NewRing(vnodesPerNode int, initialNodes ...NodeInfo) *Ring {
	if vnodesPerNode <= 0 {
		vnodesPerNode = 64
	}
	r := &Ring{
		vnodes: vnodesPerNode,
		nodes:  make(map[string]NodeInfo),
	}
	for _, n := range initialNodes {
		if len(n.Roles) == 0 {
			n.Roles = []string{RoleData}
		}
		r.nodes[n.ID] = n
	}
	r.rebuild()
	return r
}

// AddNode inserts or updates a node on the cluster ring.
func (r *Ring) AddNode(node NodeInfo) {
	if len(node.Roles) == 0 {
		node.Roles = []string{RoleData}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[node.ID] = node
	r.rebuild()
}

// RemoveNode removes a node from the ring.
func (r *Ring) RemoveNode(nodeID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.nodes, nodeID)
	r.rebuild()
}

// ReplaceNodes atomically replaces the ring's membership set and rebuilds virtual nodes
// only when the active node list, addresses, or roles changed. Returns true if changed.
func (r *Ring) ReplaceNodes(nodes []NodeInfo) bool {
	next := make(map[string]NodeInfo, len(nodes))
	for _, n := range nodes {
		if n.ID == "" {
			continue
		}
		roles := append([]string(nil), n.Roles...)
		if len(roles) == 0 {
			roles = []string{RoleData}
		} else {
			sort.Strings(roles)
		}
		n.Roles = roles
		next[n.ID] = n
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(next) == len(r.nodes) {
		same := true
		for id, nw := range next {
			cur, ok := r.nodes[id]
			if !ok || cur.Address != nw.Address || !equalStringSlice(cur.NormalizedRoles(), nw.NormalizedRoles()) {
				same = false
				break
			}
		}
		if same {
			return false
		}
	}

	r.nodes = next
	r.rebuild()
	return true
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ReplaceNodes atomically replaces the ring's membership set and rebuilds virtual nodes
// only when the active node list, addresses, or roles changed. Returns true if changed.
func (r *Ring) ReplaceNodes(nodes []NodeInfo) bool {
	next := make(map[string]NodeInfo, len(nodes))
	for _, n := range nodes {
		if n.ID == "" {
			continue
		}
		roles := append([]string(nil), n.Roles...)
		if len(roles) == 0 {
			roles = []string{RoleData}
		} else {
			sort.Strings(roles)
		}
		n.Roles = roles
		next[n.ID] = n
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if len(next) == len(r.nodes) {
		same := true
		for id, nw := range next {
			cur, ok := r.nodes[id]
			if !ok || cur.Address != nw.Address || !equalStringSlice(cur.NormalizedRoles(), nw.NormalizedRoles()) {
				same = false
				break
			}
		}
		if same {
			return false
		}
	}

	r.nodes = next
	r.rebuild()
	return true
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Nodes returns a sorted snapshot of all registered cluster nodes (data, coordinator, reader).
func (r *Ring) Nodes() []NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]NodeInfo, 0, len(r.nodes))
	for _, n := range r.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// DataNodes returns all registered nodes that participate in shard ownership (RoleData).
func (r *Ring) DataNodes() []NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []NodeInfo
	for _, n := range r.nodes {
		if n.IsDataNode() {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ReaderNodes returns stateless S3 reader nodes (RoleReader without RoleData).
func (r *Ring) ReaderNodes() []NodeInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []NodeInfo
	for _, n := range r.nodes {
		if n.HasRole(RoleReader) && !n.IsDataNode() {
			out = append(out, n)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (r *Ring) rebuild() {
	r.ring = r.ring[:0]
	r.dataCnt = 0
	for id, n := range r.nodes {
		if !n.IsDataNode() {
			continue
		}
		r.dataCnt++
		for v := 0; v < r.vnodes; v++ {
			h := hash64(fmt.Sprintf("%s#%d", id, v))
			r.ring = append(r.ring, vnodeEntry{hash: h, nodeID: id})
		}
	}
	sort.Slice(r.ring, func(i, j int) bool {
		if r.ring[i].hash == r.ring[j].hash {
			return r.ring[i].nodeID < r.ring[j].nodeID
		}
		return r.ring[i].hash < r.ring[j].hash
	})
}

// ShardForKey deterministically hashes a routing key or document ID to a shard index in [0, numShards).
func ShardForKey(indexName, routingKey string, numShards int) int {
	if numShards <= 1 {
		return 0
	}
	h := hash64(indexName + "\x00" + routingKey)
	return int(h % uint64(numShards))
}

// PlacementForShard resolves the primary and distinct replica data nodes for a shard.
func (r *Ring) PlacementForShard(indexName string, shardID, numReplicas int) (Placement, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 || r.dataCnt == 0 {
		return Placement{}, fmt.Errorf("cluster ring has no available data nodes")
	}

	targetHash := hash64(fmt.Sprintf("%s:shard:%d", indexName, shardID))
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].hash >= targetHash
	})
	if idx == len(r.ring) {
		idx = 0
	}

	desiredOwners := 1 + numReplicas
	if desiredOwners > r.dataCnt {
		desiredOwners = r.dataCnt
	}

	seen := make(map[string]struct{}, desiredOwners)
	owners := make([]NodeInfo, 0, desiredOwners)

	for offset := 0; offset < len(r.ring) && len(owners) < desiredOwners; offset++ {
		entry := r.ring[(idx+offset)%len(r.ring)]
		if _, ok := seen[entry.nodeID]; ok {
			continue
		}
		seen[entry.nodeID] = struct{}{}
		owners = append(owners, r.nodes[entry.nodeID])
	}

	return Placement{
		ShardID:  shardID,
		Primary:  owners[0],
		Replicas: owners[1:],
	}, nil
}

// AllPlacements returns placements for shards [0, numShards).
func (r *Ring) AllPlacements(indexName string, numShards, numReplicas int) ([]Placement, error) {
	if numShards <= 0 {
		numShards = 1
	}
	out := make([]Placement, 0, numShards)
	for s := 0; s < numShards; s++ {
		p, err := r.PlacementForShard(indexName, s, numReplicas)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

func hash64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}
