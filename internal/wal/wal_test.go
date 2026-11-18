package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/cindersearch/cindersearch/internal/checksum"
)

func TestWALAppendReplayAndBitRotProtection(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shard-0.wal")

	w, err := Open(path, false)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}

	r1, err := w.Append(OpIndex, "logs", "doc-1", []byte(`{"msg":"hello world"}`))
	if err != nil {
		t.Fatalf("append r1: %v", err)
	}
	r2, err := w.Append(OpIndex, "logs", "doc-2", []byte(`{"msg":"nvme wal"}`))
	if err != nil {
		t.Fatalf("append r2: %v", err)
	}
	_ = w.Close()

	// Flip a byte near the end of the file to simulate tail bit-rot on r2.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read wal file: %v", err)
	}
	data[len(data)-3] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("corrupt wal file: %v", err)
	}

	// Reopen: r1 must replay intact, r2 must be rejected/truncated safely.
	w2, err := Open(path, false)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer w2.Close()

	recs := w2.Since(0)
	if len(recs) != 1 || recs[0].DocID != r1.DocID {
		t.Fatalf("expected only intact r1 after tail corruption, got %+v", recs)
	}

	// Verify replica rejection of corrupted record.
	corrupted := r2
	corrupted.Payload = []byte(`{"msg":"tampered"}`)
	if err := VerifyRecord(corrupted); !errors.Is(err, checksum.ErrMismatch) {
		t.Fatalf("expected ErrMismatch for tampered replica record, got %v", err)
	}

	if err := w2.CompactUpTo(r1.Seq); err != nil {
		t.Fatalf("compact wal: %v", err)
	}
	if w2.Len() != 0 {
		t.Fatalf("expected empty wal after compaction, got %d", w2.Len())
	}
}
