package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/cindersearch/cindersearch/internal/checksum"
)

const (
	// MagicHeader identifies a valid CinderSearch WAL frame ("CWAL").
	MagicHeader uint32 = 0x4357414C
	frameHeaderBytes   = 4 + 8 + 1 + 2 + 2 + 4 + 4 // magic + seq + op + idxLen + idLen + payloadLen + crc32c
)

// OpType defines the mutation operation recorded in a WAL frame.
type OpType uint8

const (
	OpIndex  OpType = 1
	OpDelete OpType = 2
)

// Record represents a single checksum-protected mutation in the NVMe WAL.
type Record struct {
	Seq     uint64 `json:"seq"`
	Op      OpType `json:"op"`
	Index   string `json:"index"`
	DocID   string `json:"doc_id"`
	Payload []byte `json:"payload"`
	CRC32C  uint32 `json:"crc32c"`
}

// WAL implements an append-only, CRC32C-framed Write-Ahead Log designed for
// NVMe SSD write buffering with low-latency client acknowledgment and async replication.
type WAL struct {
	mu       sync.Mutex
	path     string
	file     *os.File
	writer   *bufio.Writer
	nextSeq  uint64
	records  []Record
	syncMode bool
}

// Open opens or creates a WAL file at path and replays any intact frames,
// halting safely if a corrupted tail frame is detected.
func Open(path string, fsyncOnWrite bool) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create wal dir: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}

	w := &WAL{
		path:     path,
		file:     f,
		writer:   bufio.NewWriterSize(f, 64*1024),
		nextSeq:  1,
		syncMode: fsyncOnWrite,
	}

	if err := w.replayLocked(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) replayLocked() error {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek wal start: %w", err)
	}

	var validOffset int64
	header := make([]byte, frameHeaderBytes)
	for {
		_, err := io.ReadFull(w.file, header)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read wal header: %w", err)
		}

		magic := binary.BigEndian.Uint32(header[0:4])
		if magic != MagicHeader {
			break
		}
		seq := binary.BigEndian.Uint64(header[4:12])
		op := OpType(header[12])
		idxLen := int(binary.BigEndian.Uint16(header[13:15]))
		idLen := int(binary.BigEndian.Uint16(header[15:17]))
		payloadLen := int(binary.BigEndian.Uint32(header[17:21]))
		expectedCRC := binary.BigEndian.Uint32(header[21:25])

		bodyLen := idxLen + idLen + payloadLen
		body := make([]byte, bodyLen)
		if _, err := io.ReadFull(w.file, body); err != nil {
			break
		}

		// Verify CRC over header[4:21] + body so metadata and payload bit-rot are caught.
		crcBuf := make([]byte, 17+bodyLen)
		copy(crcBuf[0:17], header[4:21])
		copy(crcBuf[17:], body)
		if err := checksum.Verify(crcBuf, expectedCRC); err != nil {
			// Truncate corrupted tail frame instead of propagating bit-rot.
			break
		}

		idxName := string(body[:idxLen])
		docID := string(body[idxLen : idxLen+idLen])
		payload := append([]byte(nil), body[idxLen+idLen:]...)

		w.records = append(w.records, Record{
			Seq:     seq,
			Op:      op,
			Index:   idxName,
			DocID:   docID,
			Payload: payload,
			CRC32C:  expectedCRC,
		})
		if seq >= w.nextSeq {
			w.nextSeq = seq + 1
		}
		validOffset += int64(frameHeaderBytes + bodyLen)
	}

	if err := w.file.Truncate(validOffset); err != nil {
		return fmt.Errorf("truncate wal tail: %w", err)
	}
	if _, err := w.file.Seek(validOffset, io.SeekStart); err != nil {
		return fmt.Errorf("seek wal end: %w", err)
	}
	return nil
}

// Append writes a new operation into the NVMe WAL, computing its CRC32C on the fly.
func (w *WAL) Append(op OpType, indexName, docID string, payload []byte) (Record, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	seq := w.nextSeq
	w.nextSeq++

	rec, err := w.writeFrameLocked(seq, op, indexName, docID, payload)
	if err != nil {
		return Record{}, err
	}
	return rec, nil
}

// AppendReplica verifies an incoming replicated WAL frame's CRC32C and writes it locally.
func (w *WAL) AppendReplica(rec Record) error {
	if err := VerifyRecord(rec); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	if rec.Seq >= w.nextSeq {
		w.nextSeq = rec.Seq + 1
	}
	_, err := w.writeFrameLocked(rec.Seq, rec.Op, rec.Index, rec.DocID, rec.Payload)
	return err
}

func (w *WAL) writeFrameLocked(seq uint64, op OpType, indexName, docID string, payload []byte) (Record, error) {
	idxBytes := []byte(indexName)
	idBytes := []byte(docID)
	bodyLen := len(idxBytes) + len(idBytes) + len(payload)

	frame := make([]byte, frameHeaderBytes+bodyLen)
	binary.BigEndian.PutUint32(frame[0:4], MagicHeader)
	binary.BigEndian.PutUint64(frame[4:12], seq)
	frame[12] = byte(op)
	binary.BigEndian.PutUint16(frame[13:15], uint16(len(idxBytes)))
	binary.BigEndian.PutUint16(frame[15:17], uint16(len(idBytes)))
	binary.BigEndian.PutUint32(frame[17:21], uint32(len(payload)))

	offset := frameHeaderBytes
	copy(frame[offset:], idxBytes)
	offset += len(idxBytes)
	copy(frame[offset:], idBytes)
	offset += len(idBytes)
	copy(frame[offset:], payload)

	crc := computeFrameCRC(frame[4:21], frame[frameHeaderBytes:])
	binary.BigEndian.PutUint32(frame[21:25], crc)

	if _, err := w.writer.Write(frame); err != nil {
		return Record{}, fmt.Errorf("write wal frame: %w", err)
	}
	if err := w.writer.Flush(); err != nil {
		return Record{}, fmt.Errorf("flush wal buffer: %w", err)
	}
	if w.syncMode {
		if err := w.file.Sync(); err != nil {
			return Record{}, fmt.Errorf("fsync wal: %w", err)
		}
	}

	rec := Record{
		Seq:     seq,
		Op:      op,
		Index:   indexName,
		DocID:   docID,
		Payload: append([]byte(nil), payload...),
		CRC32C:  crc,
	}
	w.records = append(w.records, rec)
	return rec, nil
}

// VerifyRecord checks that a WAL record's CRC32C matches its header and payload fields.
func VerifyRecord(rec Record) error {
	idxBytes := []byte(rec.Index)
	idBytes := []byte(rec.DocID)

	meta := make([]byte, 17)
	binary.BigEndian.PutUint64(meta[0:8], rec.Seq)
	meta[8] = byte(rec.Op)
	binary.BigEndian.PutUint16(meta[9:11], uint16(len(idxBytes)))
	binary.BigEndian.PutUint16(meta[11:13], uint16(len(idBytes)))
	binary.BigEndian.PutUint32(meta[13:17], uint32(len(rec.Payload)))

	body := make([]byte, len(idxBytes)+len(idBytes)+len(rec.Payload))
	off := 0
	copy(body[off:], idxBytes)
	off += len(idxBytes)
	copy(body[off:], idBytes)
	off += len(idBytes)
	copy(body[off:], rec.Payload)

	expected := computeFrameCRC(meta, body)
	if expected != rec.CRC32C {
		return fmt.Errorf("%w: wal seq %d expected 0x%08x, got 0x%08x", checksum.ErrMismatch, rec.Seq, rec.CRC32C, expected)
	}
	return nil
}

func computeFrameCRC(meta17, body []byte) uint32 {
	buf := make([]byte, len(meta17)+len(body))
	copy(buf, meta17)
	copy(buf[len(meta17):], body)
	return checksum.Sum32C(buf)
}

// Since returns all WAL records with Seq > afterSeq for asynchronous replication or recovery.
func (w *WAL) Since(afterSeq uint64) []Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []Record
	for _, r := range w.records {
		if r.Seq > afterSeq {
			cp := r
			cp.Payload = append([]byte(nil), r.Payload...)
			out = append(out, cp)
		}
	}
	return out
}

// CompactUpTo drops records <= flushedSeq after they are durably committed to S3.
func (w *WAL) CompactUpTo(flushedSeq uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	remaining := make([]Record, 0, len(w.records))
	for _, r := range w.records {
		if r.Seq > flushedSeq {
			remaining = append(remaining, r)
		}
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Truncate(0); err != nil {
		return err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	w.writer.Reset(w.file)
	w.records = nil

	for _, r := range remaining {
		if _, err := w.writeFrameLocked(r.Seq, r.Op, r.Index, r.DocID, r.Payload); err != nil {
			return err
		}
	}
	return nil
}

// Len returns the number of uncommitted records currently buffered in the WAL.
func (w *WAL) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.records)
}

// Close flushes and closes the WAL file handle.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.writer.Flush(); err != nil {
		_ = w.file.Close()
		return err
	}
	return w.file.Close()
}
