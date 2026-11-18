package cache

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/cindersearch/cindersearch/internal/checksum"
)

func TestNVMeCacheHitLRUEvictionAndBitRotHealing(t *testing.T) {
	c, err := NewNVMeCache(t.TempDir(), 40) // Tight 40-byte budget to test LRU eviction
	if err != nil {
		t.Fatalf("new nvme cache: %v", err)
	}

	valA := []byte("aaaaaaaaaaaaaaaaaaaaaaaa") // 24 bytes
	crcA := checksum.Sum32C(valA)
	var loads atomic.Int32

	loaderA := func(ctx context.Context) ([]byte, uint32, error) {
		loads.Add(1)
		return valA, crcA, nil
	}

	// First call misses and hydrates from S3 loader.
	got, hit, err := c.GetOrFetch(context.Background(), "seg/a", crcA, loaderA)
	if err != nil || hit || string(got) != string(valA) {
		t.Fatalf("unexpected initial fetch: hit=%v err=%v", hit, err)
	}

	// Second call hits NVMe without calling S3 loader.
	got, hit, err = c.GetOrFetch(context.Background(), "seg/a", crcA, loaderA)
	if err != nil || !hit || string(got) != string(valA) {
		t.Fatalf("expected NVMe hit, got hit=%v err=%v", hit, err)
	}
	if loads.Load() != 1 {
		t.Fatalf("expected exactly 1 S3 load, got %d", loads.Load())
	}

	// Simulate bit-rot on NVMe file: GetOrFetch must detect corruption, purge, and heal from S3.
	if err := c.CorruptFileForTest("seg/a"); err != nil {
		t.Fatalf("corrupt nvme file: %v", err)
	}
	got, hit, err = c.GetOrFetch(context.Background(), "seg/a", crcA, loaderA)
	if err != nil || hit || string(got) != string(valA) {
		t.Fatalf("expected self-heal from S3 after bit-rot, got hit=%v err=%v", hit, err)
	}
	if loads.Load() != 2 {
		t.Fatalf("expected 2nd S3 load to heal bit-rot, got %d", loads.Load())
	}
	if st := c.Stats(); st.BitRotHealed != 1 {
		t.Fatalf("expected BitRotHealed=1, got %d", st.BitRotHealed)
	}

	// Put another 24-byte entry to trigger LRU eviction of seg/a (48 > 40).
	valB := []byte("bbbbbbbbbbbbbbbbbbbbbbbb")
	if err := c.Put("seg/b", valB, 0); err != nil {
		t.Fatalf("put seg/b: %v", err)
	}
	if st := c.Stats(); st.Evictions < 1 {
		t.Fatalf("expected LRU eviction, got stats %+v", st)
	}
}
