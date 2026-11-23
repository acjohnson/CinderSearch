package main

import (
	"reflect"
	"testing"
	"time"

	"github.com/cindersearch/cindersearch/internal/sharding"
)

func TestParsePeersFlag(t *testing.T) {
	peers, err := parsePeersFlag("")
	if err != nil || len(peers) != 0 {
		t.Fatalf("expected empty peers for blank flag, got peers=%+v err=%v", peers, err)
	}

	raw := "data-1=10.0.0.1:9200, coord-1=10.0.0.2:9200@coordinator , reader-1=10.0.0.3:9200@reader+data, "
	peers, err = parsePeersFlag(raw)
	if err != nil {
		t.Fatalf("parsePeersFlag valid input: %v", err)
	}
	if len(peers) != 3 {
		t.Fatalf("expected 3 peers, got %d: %+v", len(peers), peers)
	}
	if peers[0].ID != "data-1" || peers[0].Address != "10.0.0.1:9200" || len(peers[0].Roles) != 0 {
		t.Fatalf("unexpected peer[0]: %+v", peers[0])
	}
	if peers[1].ID != "coord-1" || peers[1].Address != "10.0.0.2:9200" || !reflect.DeepEqual(peers[1].Roles, []string{sharding.RoleCoordinator}) {
		t.Fatalf("unexpected peer[1]: %+v", peers[1])
	}
	if peers[2].ID != "reader-1" || peers[2].Address != "10.0.0.3:9200" || !reflect.DeepEqual(peers[2].Roles, []string{sharding.RoleData, sharding.RoleReader}) {
		t.Fatalf("unexpected peer[2]: %+v", peers[2])
	}

	for _, bad := range []string{
		"missing-equals",
		"=10.0.0.1:9200",
		"node-1=",
		"node-1=@data",
		"node-1=10.0.0.1:9200@master",
		"node-1=10.0.0.1:9200@data+unknown",
	} {
		if _, err := parsePeersFlag(bad); err == nil {
			t.Fatalf("expected error for invalid peer spec %q", bad)
		}
	}
}

func TestBenchPercentiles(t *testing.T) {
	p50, p95, p99 := benchPercentiles(nil)
	if p50 != 0 || p95 != 0 || p99 != 0 {
		t.Fatalf("expected zeros for empty durations, got %v %v %v", p50, p95, p99)
	}

	in := []time.Duration{
		50 * time.Millisecond,
		10 * time.Millisecond,
		100 * time.Millisecond,
		20 * time.Millisecond,
		30 * time.Millisecond,
	}
	p50, p95, p99 = benchPercentiles(in)
	if p50 != 30.0 || p95 < 50.0 || p99 < 50.0 {
		t.Fatalf("unexpected percentiles: p50=%.2f p95=%.2f p99=%.2f", p50, p95, p99)
	}
	// Verify input slice was not mutated in-place.
	if in[0] != 50*time.Millisecond || in[1] != 10*time.Millisecond {
		t.Fatalf("expected input durations slice not to be mutated: %+v", in)
	}
}

func TestRunDemoSmoke(t *testing.T) {
	if err := runDemo(); err != nil {
		t.Fatalf("runDemo smoke test failed: %v", err)
	}
}

func TestBuildMergeConfig(t *testing.T) {
	cfg, err := buildMergeConfig(30, 6, 128, 1, 0.20, 600)
	if err != nil {
		t.Fatalf("buildMergeConfig valid defaults: %v", err)
	}
	if cfg.Interval != 30*time.Second ||
		cfg.MaxSegmentsPerShard != 6 ||
		cfg.MaxMergedSegmentBytes != 128*1024*1024 ||
		cfg.MaxConcurrentMerges != 1 ||
		cfg.ReclaimDeletesRatio != 0.20 ||
		cfg.S3GCGrace != 600*time.Second {
		t.Fatalf("unexpected merge config: %+v", cfg)
	}

	badCases := []struct {
		name          string
		intervalSec   int
		maxSegments   int
		maxBytesMB    int64
		maxConcurrent int
		reclaimRatio  float64
		gcGraceSec    int
	}{
		{"zero-interval", 0, 6, 128, 1, 0.20, 600},
		{"zero-max-segments", 30, 0, 128, 1, 0.20, 600},
		{"zero-max-bytes", 30, 6, 0, 1, 0.20, 600},
		{"zero-concurrency", 30, 6, 128, 0, 0.20, 600},
		{"zero-reclaim-ratio", 30, 6, 128, 1, 0.0, 600},
		{"ratio-over-one", 30, 6, 128, 1, 1.5, 600},
		{"zero-gc-grace", 30, 6, 128, 1, 0.20, 0},
	}
	for _, tc := range badCases {
		if _, err := buildMergeConfig(tc.intervalSec, tc.maxSegments, tc.maxBytesMB, tc.maxConcurrent, tc.reclaimRatio, tc.gcGraceSec); err == nil {
			t.Fatalf("expected error for invalid merge flag case %q", tc.name)
		}
	}
}
