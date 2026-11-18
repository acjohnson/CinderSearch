package cache

import (
	"bytes"
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

func TestNVMeScrubAndOverwriteAccounting(t *testing.T) {
	c, err := NewNVMeCache(t.TempDir(), 256)
	if err != nil {
		t.Fatalf("new nvme cache: %v", err)
	}

	if err := c.Put("k1", []byte("1234567890"), 0); err != nil {
		t.Fatalf("put k1: %v", err)
	}
	if err := c.Put("k2", []byte("abcdefghij"), 0); err != nil {
		t.Fatalf("put k2: %v", err)
	}
	// Overwrite k1 with shorter value and check UsedBytes adjusts accurately.
	if err := c.Put("k1", []byte("12345"), 0); err != nil {
		t.Fatalf("overwrite k1: %v", err)
	}
	if st := c.Stats(); st.Entries != 2 || st.UsedBytes != 15 {
		t.Fatalf("expected 2 entries / 15 used bytes after overwrite, got %+v", st)
	}

	// Corrupt only k2; Scrub must remove k2 and keep k1 intact.
	if err := c.CorruptFileForTest("k2"); err != nil {
		t.Fatalf("corrupt k2: %v", err)
	}
	corrupted := c.Scrub()
	if len(corrupted) != 1 || corrupted[0] != "k2" {
		t.Fatalf("expected Scrub to report [k2], got %v", corrupted)
	}
	if got, hit, err := c.Get("k1"); err != nil || !hit || string(got) != "12345" {
		t.Fatalf("expected k1 intact after scrub, hit=%v err=%v got=%q", hit, err, string(got))
	}
	if _, hit, _ := c.Get("k2"); hit {
		t.Fatalf("expected k2 purged after scrub")
	}
}

func BenchmarkNVMeCachePut4KB(b *testing.B) {
	c, err := NewNVMeCache(b.TempDir(), 64*1024*1024)
	if err != nil {
		b.Fatalf("new nvme cache: %v", err)
	}
	val := bytes.Repeat([]byte("c"), 4096)
	crc := checksum.Sum32C(val)
	keys := []string{
		"seg/00.idx", "seg/01.idx", "seg/02.idx", "seg/03.idx",
		"seg/04.idx", "seg/05.idx", "seg/06.idx", "seg/07.idx",
		"seg/08.idx", "seg/09.idx", "seg/10.idx", "seg/11.idx",
		"seg/12.idx", "seg/13.idx", "seg/14.idx", "seg/15.idx",
	}

	b.SetBytes(int64(len(val)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := c.Put(keys[i&15], val, crc); err != nil {
			b.Fatalf("nvme put: %v", err)
		}
	}
	b.StopTimer()
}

func BenchmarkNVMeCacheGetHit4KB(b *testing.B) {
	c, err := NewNVMeCache(b.TempDir(), 64*1024*1024)
	if err != nil {
		b.Fatalf("new nvme cache: %v", err)
	}
	val := bytes.Repeat([]byte("c"), 4096)
	crc := checksum.Sum32C(val)
	if err := c.Put("seg/hit.idx", val, crc); err != nil {
		b.Fatalf("seed nvme cache: %v", err)
	}

	b.SetBytes(int64(len(val)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, hit, err := c.Get("seg/hit.idx")
		if err != nil || !hit || len(got) != len(val) {
			b.Fatalf("unexpected nvme get: hit=%v err=%v len=%d", hit, err, len(got))
		}
	}
	b.StopTimer()
}
