package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cindersearch/cindersearch/internal/cache"
	"github.com/cindersearch/cindersearch/internal/checksum"
	"github.com/cindersearch/cindersearch/internal/s3store"
	"github.com/cindersearch/cindersearch/internal/segment"
	"github.com/cindersearch/cindersearch/internal/wal"
)

// SegmentRef describes an immutable segment persisted on S3 in the shard manifest.
type SegmentRef struct {
	ID         string `json:"id"`
	IdxKey     string `json:"idx_key"`
	DocsKey    string `json:"docs_key"`
	IdxCRC32C  uint32 `json:"idx_crc32c"`
	DocsCRC32C uint32 `json:"docs_crc32c"`
	DocCount   int    `json:"doc_count"`
	MinSeq     uint64 `json:"min_seq"`
	MaxSeq     uint64 `json:"max_seq"`
	CreatedAt  int64  `json:"created_at"`
}

// ShardManifest is the authoritative S3 metadata listing all immutable segments for a shard.
type ShardManifest struct {
	IndexName  string       `json:"index_name"`
	ShardID    int          `json:"shard_id"`
	FlushedSeq uint64       `json:"flushed_seq"`
	Segments   []SegmentRef `json:"segments"`
}

// Shard manages one logical shard (primary or replica) backed by an NVMe WAL write buffer,
// an in-memory NRT segment, an NVMe LRU read cache, and S3 immutable segments.
type Shard struct {
	mu        sync.RWMutex
	indexName string
	shardID   int
	isPrimary bool

	wal       *wal.WAL
	nvmeCache *cache.NVMeCache
	s3        *s3store.Client

	memDocs    map[string]wal.Record // uncommitted/WAL-backed latest doc state
	memSeg     *segment.Segment      // NRT in-memory segment built from memDocs
	memDirty   bool
	flushedSeq uint64
	segments   []SegmentRef
	segCounter int

	replicaAckSeq map[string]uint64 // peerNodeID -> last replicated WAL seq
	lastWriteAt   atomic.Int64
}

// OpenShard initializes a shard's NVMe WAL, replays uncommitted frames into the NRT buffer,
// and loads any existing shard manifest from S3.
func OpenShard(
	ctx context.Context,
	indexName string,
	shardID int,
	isPrimary bool,
	nvmeDir string,
	nvmeCache *cache.NVMeCache,
	s3 *s3store.Client,
) (*Shard, error) {
	walPath := filepath.Join(nvmeDir, "wal", indexName, fmt.Sprintf("shard-%d.wal", shardID))
	w, err := wal.Open(walPath, false)
	if err != nil {
		return nil, fmt.Errorf("open shard wal: %w", err)
	}

	sh := &Shard{
		indexName:     indexName,
		shardID:       shardID,
		isPrimary:     isPrimary,
		wal:           w,
		nvmeCache:     nvmeCache,
		s3:            s3,
		memDocs:       make(map[string]wal.Record),
		replicaAckSeq: make(map[string]uint64),
	}

	// Load authoritative manifest from S3 if present.
	if s3 != nil {
		mKey := manifestKey(indexName, shardID)
		data, _, err := s3.GetObject(ctx, mKey)
		if err == nil {
			var man ShardManifest
			if jsonErr := json.Unmarshal(data, &man); jsonErr == nil {
				sh.flushedSeq = man.FlushedSeq
				sh.segments = man.Segments
				sh.segCounter = len(man.Segments)
			}
		} else if !errors.Is(err, s3store.ErrNotFound) {
			_ = w.Close()
			return nil, fmt.Errorf("load shard manifest: %w", err)
		}
	}

	// Replay any WAL records newer than flushedSeq into memDocs.
	for _, rec := range w.Since(sh.flushedSeq) {
		sh.memDocs[rec.DocID] = rec
		sh.memDirty = true
	}
	if sh.memDirty {
		if err := sh.rebuildMemSegLocked(); err != nil {
			_ = w.Close()
			return nil, err
		}
	}

	return sh, nil
}

// IndexLocal computes CRC32C on the fly, writes to the NVMe WAL for sub-ms durability,
// and updates the NRT memtable for immediate availability.
func (s *Shard) IndexLocal(docID string, rawJSON []byte) (wal.Record, error) {
	rec, err := s.wal.Append(wal.OpIndex, s.indexName, docID, rawJSON)
	if err != nil {
		return wal.Record{}, err
	}
	s.mu.Lock()
	s.memDocs[docID] = rec
	s.memDirty = true
	s.mu.Unlock()
	s.lastWriteAt.Store(time.Now().UnixNano())
	return rec, nil
}

// DeleteLocal appends a delete tombstone into the NVMe WAL.
func (s *Shard) DeleteLocal(docID string) (wal.Record, error) {
	rec, err := s.wal.Append(wal.OpDelete, s.indexName, docID, nil)
	if err != nil {
		return wal.Record{}, err
	}
	s.mu.Lock()
	s.memDocs[docID] = rec
	s.memDirty = true
	s.mu.Unlock()
	s.lastWriteAt.Store(time.Now().UnixNano())
	return rec, nil
}

// ApplyReplicaRecords verifies CRC32C on replicated WAL frames and persists them locally.
func (s *Shard) ApplyReplicaRecords(records []wal.Record) (uint64, error) {
	var highSeq uint64
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, rec := range records {
		if err := wal.VerifyRecord(rec); err != nil {
			return highSeq, err
		}
		if prev, ok := s.memDocs[rec.DocID]; ok && prev.Seq >= rec.Seq {
			if rec.Seq > highSeq {
				highSeq = rec.Seq
			}
			continue
		}
		if err := s.wal.AppendReplica(rec); err != nil {
			return highSeq, err
		}
		s.memDocs[rec.DocID] = rec
		s.memDirty = true
		if rec.Seq > highSeq {
			highSeq = rec.Seq
		}
	}
	return highSeq, nil
}

// PendingReplication returns WAL frames newer than afterSeq for async replica streaming.
func (s *Shard) PendingReplication(afterSeq uint64) []wal.Record {
	return s.wal.Since(afterSeq)
}

// Refresh ensures the NRT in-memory segment reflects all WAL writes.
func (s *Shard) Refresh() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.memDirty {
		return nil
	}
	return s.rebuildMemSegLocked()
}

func (s *Shard) rebuildMemSegLocked() error {
	if len(s.memDocs) == 0 {
		s.memSeg = nil
		s.memDirty = false
		return nil
	}
	inputs := make([]segment.InputDoc, 0, len(s.memDocs))
	for _, rec := range s.memDocs {
		inputs = append(inputs, segment.InputDoc{
			DocID:   rec.DocID,
			Seq:     rec.Seq,
			Deleted: rec.Op == wal.OpDelete,
			RawJSON: rec.Payload,
			CRC32C:  checksum.Sum32C(rec.Payload),
		})
	}
	seg, _, _, err := segment.Build("mem", s.indexName, s.shardID, inputs, "", "")
	if err != nil {
		return err
	}
	// Attach RawJSON onto memSeg.Docs for instant local NRT reads without S3 round-trip.
	for i := range seg.Docs {
		if rec, ok := s.memDocs[seg.Docs[i].DocID]; ok {
			seg.Docs[i].RawJSON = rec.Payload
		}
	}
	s.memSeg = seg
	s.memDirty = false
	return nil
}

// FlushToS3 compiles buffered WAL documents into an immutable segment (positional index,
// DocValues, and packed _source doc store), uploads it to S3 with CRC32C verification,
// warms the local NVMe read cache, updates the S3 manifest, and compacts the WAL.
func (s *Shard) FlushToS3(ctx context.Context) (*SegmentRef, error) {
	if s.s3 == nil {
		return nil, fmt.Errorf("s3 client not configured")
	}

	s.mu.Lock()
	if len(s.memDocs) == 0 {
		s.mu.Unlock()
		return nil, nil
	}
	inputs := make([]segment.InputDoc, 0, len(s.memDocs))
	var maxSeq uint64
	for _, rec := range s.memDocs {
		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
		inputs = append(inputs, segment.InputDoc{
			DocID:   rec.DocID,
			Seq:     rec.Seq,
			Deleted: rec.Op == wal.OpDelete,
			RawJSON: rec.Payload,
			CRC32C:  checksum.Sum32C(rec.Payload),
		})
	}
	s.segCounter++
	segID := fmt.Sprintf("seg-%06d", s.segCounter)
	idxKey := fmt.Sprintf("%s/shard-%d/%s.idx", s.indexName, s.shardID, segID)
	docsKey := fmt.Sprintf("%s/shard-%d/%s.docs", s.indexName, s.shardID, segID)
	s.mu.Unlock()

	seg, idxBytes, docsBytes, err := segment.Build(segID, s.indexName, s.shardID, inputs, idxKey, docsKey)
	if err != nil {
		return nil, fmt.Errorf("build s3 segment: %w", err)
	}

	docsCRC, err := s.s3.PutObject(ctx, docsKey, docsBytes)
	if err != nil {
		return nil, err
	}
	idxCRC, err := s.s3.PutObject(ctx, idxKey, idxBytes)
	if err != nil {
		return nil, err
	}

	// Warm NVMe read cache immediately so queries stay sub-millisecond after flush.
	if s.nvmeCache != nil {
		_ = s.nvmeCache.Put(idxKey, idxBytes, idxCRC)
		_ = s.nvmeCache.Put(docsKey, docsBytes, docsCRC)
	}

	ref := SegmentRef{
		ID:         segID,
		IdxKey:     idxKey,
		DocsKey:    docsKey,
		IdxCRC32C:  idxCRC,
		DocsCRC32C: docsCRC,
		DocCount:   seg.LiveDocs,
		MinSeq:     seg.MinSeq,
		MaxSeq:     maxSeq,
		CreatedAt:  time.Now().UTC().Unix(),
	}

	s.mu.Lock()
	s.segments = append(s.segments, ref)
	s.flushedSeq = maxSeq
	for docID, rec := range s.memDocs {
		if rec.Seq <= maxSeq {
			delete(s.memDocs, docID)
		}
	}
	s.memDirty = len(s.memDocs) > 0
	if s.memDirty {
		_ = s.rebuildMemSegLocked()
	} else {
		s.memSeg = nil
	}
	man := ShardManifest{
		IndexName:  s.indexName,
		ShardID:    s.shardID,
		FlushedSeq: s.flushedSeq,
		Segments:   append([]SegmentRef(nil), s.segments...),
	}
	s.mu.Unlock()

	manBytes, err := json.Marshal(man)
	if err != nil {
		return nil, err
	}
	if _, err := s.s3.PutObject(ctx, manifestKey(s.indexName, s.shardID), manBytes); err != nil {
		return nil, err
	}
	if err := s.wal.CompactUpTo(maxSeq); err != nil {
		return nil, err
	}
	return &ref, nil
}

// ActiveSegments returns all searchable segments for this shard (committed S3 segments
// hydrated through the NVMe cache + any uncommitted NRT memtable segment), ordered newest-first.
// It also returns tombstones newer than older segments so deleted/updated docs are suppressed.
func (s *Shard) ActiveSegments(ctx context.Context) ([]*segment.Segment, map[string]uint64, error) {
	s.mu.Lock()
	if s.memDirty {
		if err := s.rebuildMemSegLocked(); err != nil {
			s.mu.Unlock()
			return nil, nil, err
		}
	}
	mem := s.memSeg
	refs := append([]SegmentRef(nil), s.segments...)
	s.mu.Unlock()

	// Sync manifest from S3 if we are a replica or newly joined reader.
	if s.s3 != nil && !s.isPrimary {
		if data, _, err := s.s3.GetObject(ctx, manifestKey(s.indexName, s.shardID)); err == nil {
			var man ShardManifest
			if json.Unmarshal(data, &man) == nil && len(man.Segments) > len(refs) {
				refs = man.Segments
				s.mu.Lock()
				s.segments = man.Segments
				s.flushedSeq = man.FlushedSeq
				s.mu.Unlock()
			}
		}
	}

	var out []*segment.Segment
	if mem != nil {
		out = append(out, mem)
	}

	// Load committed S3 segments newest-first via NVMe read cache.
	for i := len(refs) - 1; i >= 0; i-- {
		ref := refs[i]
		var idxBytes []byte
		var err error
		if s.nvmeCache != nil {
			idxBytes, _, err = s.nvmeCache.GetOrFetch(ctx, ref.IdxKey, ref.IdxCRC32C, func(c context.Context) ([]byte, uint32, error) {
				return s.s3.GetObject(c, ref.IdxKey)
			})
		} else {
			idxBytes, _, err = s.s3.GetObject(ctx, ref.IdxKey)
		}
		if err != nil {
			return nil, nil, fmt.Errorf("load segment %s: %w", ref.ID, err)
		}
		seg, err := segment.UnmarshalIndex(idxBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("decode segment %s: %w", ref.ID, err)
		}
		seg.DocsCRC32C = ref.DocsCRC32C
		out = append(out, seg)
	}

	// Track highest sequence per DocID across segments + tombstones to suppress stale versions.
	latestSeq := make(map[string]uint64)
	for _, seg := range out {
		for docID, delSeq := range seg.Tombstones {
			if delSeq > latestSeq[docID] {
				latestSeq[docID] = delSeq
			}
		}
		for _, dm := range seg.Docs {
			if dm.Seq > latestSeq[dm.DocID] {
				latestSeq[dm.DocID] = dm.Seq
			}
		}
	}
	return out, latestSeq, nil
}

// ReadDocSource retrieves a document's _source JSON bytes, verifying CRC32C and reporting
// whether it was served directly from local NVMe/memory cache without hitting S3.
func (s *Shard) ReadDocSource(ctx context.Context, seg *segment.Segment, docIdx uint32) (raw []byte, nvmeHit bool, err error) {
	meta := seg.Docs[docIdx]
	if len(meta.RawJSON) > 0 {
		if err := checksum.Verify(meta.RawJSON, meta.CRC32C); err != nil {
			return nil, true, err
		}
		return meta.RawJSON, true, nil
	}

	if seg.DocsObjectKey == "" {
		return nil, false, fmt.Errorf("missing S3 object key for doc %s", meta.DocID)
	}

	// 1. Check NVMe cache for the packed .docs segment first.
	if s.nvmeCache != nil {
		if blob, hit, cacheErr := s.nvmeCache.Get(seg.DocsObjectKey); hit && cacheErr == nil {
			end := meta.Offset + meta.Length
			if end <= int64(len(blob)) {
				slice := blob[meta.Offset:end]
				if err := checksum.Verify(slice, meta.CRC32C); err == nil {
					return slice, true, nil
				}
			}
		}
	}

	// 2. Fallback: HTTP Range fetch of just this document's bytes from S3 + verify CRC32C.
	slice, err := s.s3.GetObjectRange(ctx, seg.DocsObjectKey, meta.Offset, meta.Offset+meta.Length-1)
	if err != nil {
		return nil, false, err
	}
	if err := checksum.Verify(slice, meta.CRC32C); err != nil {
		return nil, false, err
	}
	return slice, false, nil
}

// IsDocsCachedOnNVMe checks whether a segment's packed .docs object is currently intact in NVMe cache.
func (s *Shard) IsDocsCachedOnNVMe(docsKey string) bool {
	if s.nvmeCache == nil || docsKey == "" {
		return false
	}
	_, hit, err := s.nvmeCache.Get(docsKey)
	return hit && err == nil
}

// GetDocByID fetches the latest live document matching docID in this shard.
func (s *Shard) GetDocByID(ctx context.Context, docID string) (raw []byte, found bool, crc uint32, meta segment.DocMeta, seg *segment.Segment, err error) {
	segs, latestSeq, err := s.ActiveSegments(ctx)
	if err != nil {
		return nil, false, 0, segment.DocMeta{}, nil, err
	}
	for _, seg := range segs {
		if delSeq, isDel := seg.Tombstones[docID]; isDel && delSeq >= latestSeq[docID] {
			return nil, false, 0, segment.DocMeta{}, nil, nil
		}
		if idx, ok := seg.DocIDToIdx[docID]; ok {
			dm := seg.Docs[idx]
			if dm.Seq < latestSeq[docID] {
				continue
			}
			raw, _, err := s.ReadDocSource(ctx, seg, idx)
			if err != nil {
				return nil, false, 0, segment.DocMeta{}, nil, err
			}
			return raw, true, dm.CRC32C, dm, seg, nil
		}
	}
	return nil, false, 0, segment.DocMeta{}, nil, nil
}

// Stats returns shard-level diagnostics.
type ShardStats struct {
	IndexName    string       `json:"index"`
	ShardID      int          `json:"shard_id"`
	IsPrimary    bool         `json:"is_primary"`
	WALLength    int          `json:"wal_buffered_ops"`
	FlushedSeq   uint64       `json:"flushed_seq"`
	SegmentCount int          `json:"s3_segments"`
	Segments     []SegmentRef `json:"segments"`
}

// Stats returns a snapshot of the shard's WAL and S3 segment state.
func (s *Shard) Stats() ShardStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return ShardStats{
		IndexName:    s.indexName,
		ShardID:      s.shardID,
		IsPrimary:    s.isPrimary,
		WALLength:    s.wal.Len(),
		FlushedSeq:   s.flushedSeq,
		SegmentCount: len(s.segments),
		Segments:     append([]SegmentRef(nil), s.segments...),
	}
}

// SortedSegmentIDs returns committed S3 segment IDs for inspection.
func (s *Shard) SortedSegmentIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.segments))
	for _, seg := range s.segments {
		out = append(out, seg.ID)
	}
	sort.Strings(out)
	return out
}

// Close closes the shard's WAL handle.
func (s *Shard) Close() error {
	return s.wal.Close()
}

func manifestKey(indexName string, shardID int) string {
	return fmt.Sprintf("%s/shard-%d/manifest.json", indexName, shardID)
}
