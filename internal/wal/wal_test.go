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

func TestWALPartialCompactionFsyncAndReplicaReplay(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "shard-sync.wal")

	w, err := Open(path, true) // fsyncOnWrite enabled
	if err != nil {
		t.Fatalf("open fsync wal: %v", err)
	}

	r1, err := w.Append(OpIndex, "logs", "d1", []byte(`{"v":1}`))
	if err != nil {
		t.Fatalf("append r1: %v", err)
	}
	r2, err := w.Append(OpDelete, "logs", "d1", nil)
	if err != nil {
		t.Fatalf("append r2: %v", err)
	}
	r3, err := w.Append(OpIndex, "logs", "d2", []byte(`{"v":2}`))
	if err != nil {
		t.Fatalf("append r3: %v", err)
	}

	// Partial compaction: drop r1 & r2 (<= r2.Seq), keep r3.
	if err := w.CompactUpTo(r2.Seq); err != nil {
		t.Fatalf("partial compact: %v", err)
	}
	if w.Len() != 1 {
		t.Fatalf("expected 1 retained record after partial compaction, got %d", w.Len())
	}
	_ = w.Close()

	// Reopen and verify retained r3 + next sequence increments past r3.Seq.
	w2, err := Open(path, false)
	if err != nil {
		t.Fatalf("reopen wal: %v", err)
	}
	defer w2.Close()

	recs := w2.Since(0)
	if len(recs) != 1 || recs[0].Seq != r3.Seq || recs[0].DocID != "d2" {
		t.Fatalf("unexpected retained WAL records: %+v", recs)
	}

	r4, err := w2.Append(OpIndex, "logs", "d3", []byte(`{"v":3}`))
	if err != nil {
		t.Fatalf("append r4: %v", err)
	}
	if r4.Seq <= r3.Seq {
		t.Fatalf("expected monotonic seq > %d, got %d", r3.Seq, r4.Seq)
	}

	// Replica WAL accepts verified r1 frame and advances cleanly.
	repWAL, err := Open(filepath.Join(dir, "replica.wal"), false)
	if err != nil {
		t.Fatalf("open replica wal: %v", err)
	}
	defer repWAL.Close()

	if err := repWAL.AppendReplica(r1); err != nil {
		t.Fatalf("append verified replica frame: %v", err)
	}
	if repWAL.Len() != 1 || repWAL.Since(0)[0].DocID != "d1" {
		t.Fatalf("unexpected replica WAL state: %+v", repWAL.Since(0))
	}
}
