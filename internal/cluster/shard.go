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
	flushMu   sync.Mutex
	indexName string
	shardID   int
	isPrimary bool
	readOnly  bool

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

	segCacheMu         sync.RWMutex
	decodedSegs        map[string]*segment.Segment // idxKey -> cached decoded immutable segment
	committedLatestSeq map[string]uint64           // highest seq across committed segments
	committedSeqSegCnt int                         // len(segments) when committedLatestSeq was built
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
		decodedSegs:   make(map[string]*segment.Segment),
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

// OpenReadOnlyShard opens a stateless S3-backed shard view (no local NVMe WAL files) for reader nodes.
func OpenReadOnlyShard(
	ctx context.Context,
	indexName string,
	shardID int,
	nvmeCache *cache.NVMeCache,
	s3 *s3store.Client,
) (*Shard, error) {
	sh := &Shard{
		indexName:     indexName,
		shardID:       shardID,
		isPrimary:     false,
		readOnly:      true,
		nvmeCache:     nvmeCache,
		s3:            s3,
		memDocs:       make(map[string]wal.Record),
		replicaAckSeq: make(map[string]uint64),
		decodedSegs:   make(map[string]*segment.Segment),
	}

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
			return nil, fmt.Errorf("load readonly shard manifest: %w", err)
		}
	}
	return sh, nil
}

// IndexLocal computes CRC32C on the fly, writes to the NVMe WAL for sub-ms durability,
// and updates the NRT memtable for immediate availability.
func (s *Shard) IndexLocal(docID string, rawJSON []byte) (wal.Record, error) {
	if s.readOnly || s.wal == nil {
		return wal.Record{}, fmt.Errorf("shard %s[%d] is read-only", s.indexName, s.shardID)
	}
	s.mu.Lock()
	rec, err := s.wal.Append(wal.OpIndex, s.indexName, docID, rawJSON)
	if err != nil {
		s.mu.Unlock()
		return wal.Record{}, err
	}
	s.memDocs[docID] = rec
	s.memDirty = true
	s.mu.Unlock()
	s.lastWriteAt.Store(time.Now().UnixNano())
	return rec, nil
}

// DeleteLocal appends a delete tombstone into the NVMe WAL.
func (s *Shard) DeleteLocal(docID string) (wal.Record, error) {
	if s.readOnly || s.wal == nil {
		return wal.Record{}, fmt.Errorf("shard %s[%d] is read-only", s.indexName, s.shardID)
	}
	s.mu.Lock()
	rec, err := s.wal.Append(wal.OpDelete, s.indexName, docID, nil)
	if err != nil {
		s.mu.Unlock()
		return wal.Record{}, err
	}
	s.memDocs[docID] = rec
	s.memDirty = true
	s.mu.Unlock()
	s.lastWriteAt.Store(time.Now().UnixNano())
	return rec, nil
}

// ApplyReplicaRecords verifies CRC32C on replicated WAL frames and persists them locally.
func (s *Shard) ApplyReplicaRecords(records []wal.Record) (uint64, error) {
	if s.readOnly || s.wal == nil {
		return 0, fmt.Errorf("shard %s[%d] is read-only", s.indexName, s.shardID)
	}
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
	if s.wal == nil {
		return nil
	}
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
	if s.readOnly || s.wal == nil {
		return nil, nil
	}
	if s.s3 == nil {
		return nil, fmt.Errorf("s3 client not configured")
	}

	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.Lock()
	if len(s.memDocs) == 0 {
		s.mu.Unlock()
		return nil, nil
	}
	inputs := make([]segment.InputDoc, 0, len(s.memDocs))
	flushedSeqs := make(map[string]uint64, len(s.memDocs))
	var maxSeq uint64
	for docID, rec := range s.memDocs {
		if rec.Seq > maxSeq {
			maxSeq = rec.Seq
		}
		flushedSeqs[docID] = rec.Seq
		inputs = append(inputs, segment.InputDoc{
			DocID:   rec.DocID,
			Seq:     rec.Seq,
			Deleted: rec.Op == wal.OpDelete,
			RawJSON: rec.Payload,
			CRC32C:  checksum.Sum32C(rec.Payload),
		})
	}
	nextCounter := s.segCounter + 1
	segID := fmt.Sprintf("seg-%06d", nextCounter)
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

	// Build and persist authoritative S3 manifest BEFORE mutating local memDocs or compacting WAL.
	// If S3 fails here, memDocs and WAL remain intact so the next flush retries safely.
	s.mu.RLock()
	nextFlushedSeq := s.flushedSeq
	if maxSeq > nextFlushedSeq {
		nextFlushedSeq = maxSeq
	}
	nextSegments := make([]SegmentRef, len(s.segments)+1)
	copy(nextSegments, s.segments)
	nextSegments[len(s.segments)] = ref
	s.mu.RUnlock()

	man := ShardManifest{
		IndexName:  s.indexName,
		ShardID:    s.shardID,
		FlushedSeq: nextFlushedSeq,
		Segments:   nextSegments,
	}
	manBytes, err := json.Marshal(man)
	if err != nil {
		return nil, err
	}
	if _, err := s.s3.PutObject(ctx, manifestKey(s.indexName, s.shardID), manBytes); err != nil {
		return nil, err
	}

	// Warm NVMe read cache and in-memory decoded segment cache after S3 commit succeeds.
	if s.nvmeCache != nil {
		_ = s.nvmeCache.Put(idxKey, idxBytes, idxCRC)
		_ = s.nvmeCache.Put(docsKey, docsBytes, docsCRC)
	}
	seg.DocsCRC32C = docsCRC
	s.segCacheMu.Lock()
	s.decodedSegs[idxKey] = seg
	s.committedLatestSeq = nil
	s.committedSeqSegCnt = 0
	s.segCacheMu.Unlock()

	s.mu.Lock()
	s.segCounter = nextCounter
	s.segments = nextSegments
	for docID, rec := range s.memDocs {
		if fSeq, ok := flushedSeqs[docID]; ok && rec.Seq <= fSeq {
			delete(s.memDocs, docID)
		}
	}
	compactSeq := maxSeq
	for _, rec := range s.memDocs {
		if rec.Seq > 0 && rec.Seq-1 < compactSeq {
			compactSeq = rec.Seq - 1
		}
	}
	if compactSeq > s.flushedSeq {
		s.flushedSeq = compactSeq
	}
	s.memDirty = len(s.memDocs) > 0
	if s.memDirty {
		_ = s.rebuildMemSegLocked()
	} else {
		s.memSeg = nil
	}
	s.mu.Unlock()

	if compactSeq > 0 {
		if err := s.wal.CompactUpTo(compactSeq); err != nil {
			return nil, err
		}
	}
	return &ref, nil
}

// ActiveSegments returns all searchable segments for this shard (committed S3 segments
// hydrated through the NVMe cache + any uncommitted NRT memtable segment), ordered newest-first.
// It also returns tombstones newer than older segments so deleted/updated docs are suppressed.
func (s *Shard) ActiveSegments(ctx context.Context) ([]*segment.Segment, map[string]uint64, error) {
	s.mu.RLock()
	dirty := s.memDirty
	var mem *segment.Segment
	var refs []SegmentRef
	if !dirty {
		mem = s.memSeg
		refs = append([]SegmentRef(nil), s.segments...)
	}
	s.mu.RUnlock()

	if dirty {
		s.mu.Lock()
		if s.memDirty {
			if err := s.rebuildMemSegLocked(); err != nil {
				s.mu.Unlock()
				return nil, nil, err
			}
		}
		mem = s.memSeg
		refs = append([]SegmentRef(nil), s.segments...)
		s.mu.Unlock()
	}

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
				s.segCacheMu.Lock()
				s.committedLatestSeq = nil
				s.committedSeqSegCnt = 0
				s.segCacheMu.Unlock()
			}
		}
	}

	out := make([]*segment.Segment, 0, len(refs)+1)
	if mem != nil {
		out = append(out, mem)
	}

	// Load committed S3 segments newest-first; reuse decoded immutable segments in memory
	// while still performing zero-alloc CRC32C verification on the NVMe .idx file to catch bit-rot.
	committedSegs := make([]*segment.Segment, 0, len(refs))
	for i := len(refs) - 1; i >= 0; i-- {
		ref := refs[i]

		s.segCacheMu.RLock()
		cachedSeg := s.decodedSegs[ref.IdxKey]
		s.segCacheMu.RUnlock()

		if cachedSeg != nil {
			if s.nvmeCache == nil {
				committedSegs = append(committedSegs, cachedSeg)
				continue
			}
			if ok, vErr := s.nvmeCache.VerifyCRC(ref.IdxKey, ref.IdxCRC32C); ok && vErr == nil {
				committedSegs = append(committedSegs, cachedSeg)
				continue
			}
		}

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

		s.segCacheMu.Lock()
		s.decodedSegs[ref.IdxKey] = seg
		s.segCacheMu.Unlock()
		committedSegs = append(committedSegs, seg)
	}
	out = append(out, committedSegs...)

	// Track highest sequence per DocID across committed segments (cached) + memtable overlay.
	var baseSeq map[string]uint64
	if len(committedSegs) > 0 {
		s.segCacheMu.RLock()
		if s.committedLatestSeq != nil && s.committedSeqSegCnt == len(refs) {
			baseSeq = s.committedLatestSeq
		}
		s.segCacheMu.RUnlock()

		if baseSeq == nil {
			built := make(map[string]uint64)
			for _, seg := range committedSegs {
				for docID, delSeq := range seg.Tombstones {
					if delSeq > built[docID] {
						built[docID] = delSeq
					}
				}
				for _, dm := range seg.Docs {
					if dm.Seq > built[dm.DocID] {
						built[dm.DocID] = dm.Seq
					}
				}
			}
			s.segCacheMu.Lock()
			s.committedLatestSeq = built
			s.committedSeqSegCnt = len(refs)
			s.segCacheMu.Unlock()
			baseSeq = built
		}
	}

	if mem == nil {
		if baseSeq == nil {
			baseSeq = map[string]uint64{}
		}
		return out, baseSeq, nil
	}

	latestSeq := make(map[string]uint64, len(baseSeq)+len(mem.Docs)+len(mem.Tombstones))
	for k, v := range baseSeq {
		latestSeq[k] = v
	}
	for docID, delSeq := range mem.Tombstones {
		if delSeq > latestSeq[docID] {
			latestSeq[docID] = delSeq
		}
	}
	for _, dm := range mem.Docs {
		if dm.Seq > latestSeq[dm.DocID] {
			latestSeq[dm.DocID] = dm.Seq
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
	return s.ReadDocSourceByMeta(ctx, seg.DocsObjectKey, meta.Offset, meta.Length, meta.CRC32C)
}

// ReadDocSourceByMeta fetches just [offset, offset+length) bytes for a single document from
// local NVMe cache (via ReadAt byte-range) or S3 Range fallback, verifying per-doc CRC32C.
func (s *Shard) ReadDocSourceByMeta(ctx context.Context, docsKey string, offset, length int64, crc32c uint32) (raw []byte, nvmeHit bool, err error) {
	if docsKey == "" {
		return nil, false, fmt.Errorf("missing S3 object key")
	}

	// 1. Check NVMe cache via byte-range ReadAt (avoids reading full .docs segment).
	if s.nvmeCache != nil {
		if slice, hit, cacheErr := s.nvmeCache.GetRange(docsKey, offset, length); hit && cacheErr == nil {
			if err := checksum.Verify(slice, crc32c); err == nil {
				return slice, true, nil
			}
			s.nvmeCache.InvalidateCorrupted(docsKey)
		}
	}

	// 2. Fallback: HTTP Range fetch of just this document's bytes from S3 + verify CRC32C.
	slice, err := s.s3.GetObjectRange(ctx, docsKey, offset, offset+length-1)
	if err != nil {
		return nil, false, err
	}
	if err := checksum.Verify(slice, crc32c); err != nil {
		return nil, false, err
	}
	return slice, false, nil
}

// IsDocsCachedOnNVMe checks whether a segment's packed .docs object is currently tracked on NVMe.
func (s *Shard) IsDocsCachedOnNVMe(docsKey string) bool {
	if s.nvmeCache == nil || docsKey == "" {
		return false
	}
	return s.nvmeCache.Has(docsKey)
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

// DocExists reports whether a live (non-deleted) document with docID exists in this shard
// without reading its _source bytes from NVMe or S3.
func (s *Shard) DocExists(ctx context.Context, docID string) (bool, error) {
	s.mu.RLock()
	if rec, ok := s.memDocs[docID]; ok {
		s.mu.RUnlock()
		return rec.Op != wal.OpDelete, nil
	}
	s.mu.RUnlock()

	segs, latestSeq, err := s.ActiveSegments(ctx)
	if err != nil {
		return false, err
	}
	for _, seg := range segs {
		if delSeq, isDel := seg.Tombstones[docID]; isDel && delSeq >= latestSeq[docID] {
			return false, nil
		}
		if idx, ok := seg.DocIDToIdx[docID]; ok {
			if seg.Docs[idx].Seq < latestSeq[docID] {
				continue
			}
			return true, nil
		}
	}
	return false, nil
}

// Stats returns shard-level diagnostics.
type ShardStats struct {
	IndexName    string       `json:"index"`
	ShardID      int          `json:"shard_id"`
	IsPrimary    bool         `json:"is_primary"`
	ReadOnly     bool         `json:"read_only,omitempty"`
	WALLength    int          `json:"wal_buffered_ops"`
	FlushedSeq   uint64       `json:"flushed_seq"`
	SegmentCount int          `json:"s3_segments"`
	Segments     []SegmentRef `json:"segments"`
}

// Stats returns a snapshot of the shard's WAL and S3 segment state.
func (s *Shard) Stats() ShardStats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	walLen := 0
	if s.wal != nil {
		walLen = s.wal.Len()
	}
	return ShardStats{
		IndexName:    s.indexName,
		ShardID:      s.shardID,
		IsPrimary:    s.isPrimary,
		ReadOnly:     s.readOnly,
		WALLength:    walLen,
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
	if s.wal == nil {
		return nil
	}
	return s.wal.Close()
}

func manifestKey(indexName string, shardID int) string {
	return fmt.Sprintf("%s/shard-%d/manifest.json", indexName, shardID)
}
