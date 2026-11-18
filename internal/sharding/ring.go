package sharding

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
)

// NodeInfo describes a cluster peer participating in the consistent hash ring.
type NodeInfo struct {
	ID      string `json:"id"`
	Address string `json:"address"`
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
// ownership across cluster nodes using consistent hashing with virtual nodes.
type Ring struct {
	mu       sync.RWMutex
	vnodes   int
	nodes    map[string]NodeInfo
	ring     []vnodeEntry
}

// NewRing constructs a consistent hash ring with the requested virtual nodes per physical node.
func NewRing(vnodesPerNode int, initialNodes ...NodeInfo) *Ring {
	if vnodesPerNode <= 0 {
		vnodesPerNode = 64
	}
	r := &Ring{
		vnodes: vnodesPerNode,
		nodes:  make(map[string]NodeInfo),
	}
	for _, n := range initialNodes {
		r.nodes[n.ID] = n
	}
	r.rebuild()
	return r
}

// AddNode inserts or updates a node on the consistent hash ring.
func (r *Ring) AddNode(node NodeInfo) {
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

// Nodes returns a sorted snapshot of all registered nodes.
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

func (r *Ring) rebuild() {
	r.ring = make([]vnodeEntry, 0, len(r.nodes)*r.vnodes)
	for id := range r.nodes {
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

// PlacementForShard resolves the primary and distinct replica nodes for a shard.
func (r *Ring) PlacementForShard(indexName string, shardID, numReplicas int) (Placement, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.ring) == 0 || len(r.nodes) == 0 {
		return Placement{}, fmt.Errorf("cluster ring has no available nodes")
	}

	targetHash := hash64(fmt.Sprintf("%s:shard:%d", indexName, shardID))
	idx := sort.Search(len(r.ring), func(i int) bool {
		return r.ring[i].hash >= targetHash
	})
	if idx == len(r.ring) {
		idx = 0
	}

	desiredOwners := 1 + numReplicas
	if desiredOwners > len(r.nodes) {
		desiredOwners = len(r.nodes)
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
