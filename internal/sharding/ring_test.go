package sharding

import (
	"fmt"
	"testing"
)

func TestConsistentRingDistributionAndDistinctOwners(t *testing.T) {
	nodes := []NodeInfo{
		{ID: "node-1", Address: "127.0.0.1:9201"},
		{ID: "node-2", Address: "127.0.0.1:9202"},
		{ID: "node-3", Address: "127.0.0.1:9203"},
	}
	ring := NewRing(64, nodes...)

	placements, err := ring.AllPlacements("logs", 6, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(placements) != 6 {
		t.Fatalf("expected 6 shard placements, got %d", len(placements))
	}

	primaryCounts := make(map[string]int)
	for _, p := range placements {
		primaryCounts[p.Primary.ID]++
		if len(p.Replicas) != 1 {
			t.Fatalf("shard %d: expected 1 replica, got %d", p.ShardID, len(p.Replicas))
		}
		if p.Replicas[0].ID == p.Primary.ID {
			t.Fatalf("shard %d: replica %s cannot match primary %s", p.ShardID, p.Replicas[0].ID, p.Primary.ID)
		}
	}

	// Verify ShardForKey spreads across shards.
	shardSeen := make(map[int]int)
	for i := 0; i < 300; i++ {
		s := ShardForKey("logs", fmt.Sprintf("doc-%d", i), 6)
		shardSeen[s]++
	}
	if len(shardSeen) != 6 {
		t.Fatalf("expected keys to distribute across all 6 shards, got %d shards", len(shardSeen))
	}
}
