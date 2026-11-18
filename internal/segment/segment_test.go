package segment

import (
	"errors"
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
