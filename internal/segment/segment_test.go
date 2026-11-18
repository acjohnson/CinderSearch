package segment

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cindersearch/cindersearch/internal/checksum"
)

func TestSegmentBuildMarshalAndBitRotDetection(t *testing.T) {
	docs := []InputDoc{
		{
			DocID:   "1",
			Seq:     1,
			RawJSON: []byte(`{"service":"checkout","latency_ms":245,"message":"s3 range timeout","created_at":"2026-11-17T10:00:00Z"}`),
		},
		{
			DocID:   "2",
			Seq:     2,
			RawJSON: []byte(`{"service":"search","latency_ms":7,"message":"nvme cache hit","created_at":"2026-11-17T10:05:00Z"}`),
		},
	}

	seg, idxBytes, docsBytes, err := Build("seg-1", "logs", 0, docs, "logs/shard-0/seg-1.idx", "logs/shard-0/seg-1.docs")
	if err != nil {
		t.Fatalf("build segment: %v", err)
	}
	if seg.LiveDocs != 2 {
		t.Fatalf("expected 2 live docs, got %d", seg.LiveDocs)
	}
	if len(docsBytes) != int(seg.Docs[0].Length+seg.Docs[1].Length) {
		t.Fatalf("packed docs blob length mismatch")
	}

	// Verify positional postings exist for phrase matching ("range" at pos 1, "timeout" at pos 2).
	pRange := seg.Postings[TermKey("message", "range")]
	pTimeout := seg.Postings[TermKey("message", "timeout")]
	if len(pRange) != 1 || len(pTimeout) != 1 {
		t.Fatalf("expected 1 posting each for range and timeout")
	}
	if pRange[0].Positions[0]+1 != pTimeout[0].Positions[0] {
		t.Fatalf("expected adjacent positions for 'range timeout', got %v and %v", pRange[0].Positions, pTimeout[0].Positions)
	}

	// Verify unmarshal round-trip.
	decoded, err := UnmarshalIndex(idxBytes)
	if err != nil {
		t.Fatalf("unmarshal segment index: %v", err)
	}
	if decoded.LiveDocs != 2 {
		t.Fatalf("decoded live docs mismatch: %d", decoded.LiveDocs)
	}

	// Corrupt a byte inside idxBytes and ensure ErrMismatch is caught.
	idxBytes[len(idxBytes)-5] ^= 0x10
	if _, err := UnmarshalIndex(idxBytes); !errors.Is(err, checksum.ErrMismatch) {
		t.Fatalf("expected ErrMismatch on corrupted segment index, got %v", err)
	}
}

func TestSegmentTombstonesDedupAndFlattening(t *testing.T) {
	docs := []InputDoc{
		// Earlier version of d1 should be superseded by Seq 3 delete tombstone
		{DocID: "d1", Seq: 1, RawJSON: []byte(`{"service":"auth","ok":true}`)},
		{DocID: "d2", Seq: 2, RawJSON: []byte(`{"service":"billing","ok":false,"tags":["prod","eu"],"nested":{"code":200,"region":"eu-west"}}`)},
		{DocID: "d1", Seq: 3, Deleted: true},
	}

	seg, _, docsBytes, err := Build("seg-tomb", "orders", 1, docs, "k.idx", "k.docs")
	if err != nil {
		t.Fatalf("build segment: %v", err)
	}
	if seg.LiveDocs != 1 {
		t.Fatalf("expected 1 live doc (d2), got %d", seg.LiveDocs)
	}
	if seg.Tombstones["d1"] != 3 {
		t.Fatalf("expected tombstone seq=3 for d1, got %+v", seg.Tombstones)
	}
	if len(seg.Docs) != 1 || seg.Docs[0].DocID != "d2" {
		t.Fatalf("expected only d2 in Docs, got %+v", seg.Docs)
	}
	if len(docsBytes) != int(seg.Docs[0].Length) {
		t.Fatalf("docs blob should omit deleted doc bytes")
	}

	dv := seg.DocVals[0]
	if dv.Numbers["nested.code"] != 200 || dv.Bools["ok"] != false {
		t.Fatalf("unexpected flattened nested/bool DocValues: %+v", dv)
	}
	if len(dv.Keywords["tags"]) != 2 || dv.Keywords["nested.region"][0] != "eu-west" {
		t.Fatalf("unexpected array/nested keyword DocValues: %+v", dv.Keywords)
	}
	if _, _, _, err := Build("bad", "orders", 0, []InputDoc{{DocID: "x", Seq: 1, RawJSON: []byte(`{bad json`)}}, "", ""); err == nil {
		t.Fatalf("expected error on invalid JSON document")
	}
}

func makeBenchInputDocs(n int) ([]InputDoc, int64) {
	services := []string{"checkout", "search", "payments", "inventory", "auth"}
	docs := make([]InputDoc, n)
	var totalBytes int64
	for i := 0; i < n; i++ {
		raw := []byte(fmt.Sprintf(
			`{"service":%q,"level":"info","latency_ms":%d,"message":"s3 range nvme segment lookup worker %d","created_at":"2026-11-17T10:15:00Z"}`,
			services[i%len(services)], (i%500)+1, i%16,
		))
		totalBytes += int64(len(raw))
		docs[i] = InputDoc{
			DocID:   fmt.Sprintf("doc-%d", i),
			Seq:     uint64(i + 1),
			RawJSON: raw,
		}
	}
	return docs, totalBytes
}

func BenchmarkSegmentBuild100Docs(b *testing.B) {
	docs, totalBytes := makeBenchInputDocs(100)
	b.SetBytes(totalBytes)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, _, err := Build("seg-bench", "logs", 0, docs, "logs/shard-0/seg-bench.idx", "logs/shard-0/seg-bench.docs"); err != nil {
			b.Fatalf("build segment: %v", err)
		}
	}
}

func BenchmarkSegmentUnmarshalIndex(b *testing.B) {
	docs, _ := makeBenchInputDocs(100)
	_, idxBytes, _, err := Build("seg-bench", "logs", 0, docs, "logs/shard-0/seg-bench.idx", "logs/shard-0/seg-bench.docs")
	if err != nil {
		b.Fatalf("seed segment: %v", err)
	}
	b.SetBytes(int64(len(idxBytes)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := UnmarshalIndex(idxBytes); err != nil {
			b.Fatalf("unmarshal segment index: %v", err)
		}
	}
}
