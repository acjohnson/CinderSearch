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

func TestRingExcludesCoordinatorsAndReadersFromShardOwnership(t *testing.T) {
	nodes := []NodeInfo{
		{ID: "data-1", Address: "127.0.0.1:9201", Roles: []string{RoleData}},
		{ID: "data-2", Address: "127.0.0.1:9202", Roles: []string{RoleData}},
		{ID: "gw-1", Address: "127.0.0.1:9203", Roles: []string{RoleCoordinator}},
		{ID: "rdr-1", Address: "127.0.0.1:9204", Roles: []string{RoleReader}},
	}
	ring := NewRing(64, nodes...)

	if len(ring.Nodes()) != 4 || len(ring.DataNodes()) != 2 || len(ring.ReaderNodes()) != 1 {
		t.Fatalf("unexpected node role slices: all=%d data=%d reader=%d",
			len(ring.Nodes()), len(ring.DataNodes()), len(ring.ReaderNodes()))
	}

	// Request 5 replicas even though only 2 data nodes exist: must clamp to 2 data nodes and never assign gw-1/rdr-1.
	placements, err := ring.AllPlacements("logs", 6, 5)
	if err != nil {
		t.Fatalf("placements: %v", err)
	}
	for _, p := range placements {
		if !p.Primary.IsDataNode() {
			t.Fatalf("non-data node %s assigned as primary", p.Primary.ID)
		}
		if len(p.Replicas) != 1 {
			t.Fatalf("expected exactly 1 replica clamped to 2 data nodes, got %d", len(p.Replicas))
		}
		for _, rep := range p.Replicas {
			if !rep.IsDataNode() {
				t.Fatalf("non-data node %s assigned as replica", rep.ID)
			}
		}
	}

	// Ring with only coordinator/reader nodes must fail placement cleanly.
	coordOnly := NewRing(64, NodeInfo{ID: "gw", Address: "127.0.0.1:9203", Roles: []string{RoleCoordinator}})
	if _, err := coordOnly.PlacementForShard("logs", 0, 0); err == nil {
		t.Fatalf("expected error placing shard when zero data nodes exist")
	}
}

func TestNormalizeRolesValidation(t *testing.T) {
	def, err := NormalizeRoles(nil)
	if err != nil || len(def) != 1 || def[0] != RoleData {
		t.Fatalf("expected default [data], got %v err=%v", def, err)
	}
	multi, err := NormalizeRoles([]string{" Reader ", "data", "reader"})
	if err != nil || len(multi) != 2 || multi[0] != RoleData || multi[1] != RoleReader {
		t.Fatalf("unexpected normalized roles: %v err=%v", multi, err)
	}
	for _, unsupported := range []string{"master", "ml", "ingest", "data_frozen"} {
		if _, err := NormalizeRoles([]string{unsupported}); err == nil {
			t.Fatalf("expected unsupported role %q to return error", unsupported)
		}
	}
}
