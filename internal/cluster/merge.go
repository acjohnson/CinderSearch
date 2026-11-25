package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cindersearch/cindersearch/internal/checksum"
	"github.com/cindersearch/cindersearch/internal/s3store"
	"github.com/cindersearch/cindersearch/internal/segment"
)

// MergeConfig configures Lucene-style TieredMergePolicy and crash-safe deferred S3 garbage collection.
// Defaults are tuned for latency-sensitive application and catalog search:
// low active segment count, bounded max merged segment size, strict concurrency=1 pacing,
// and a 10-minute S3 GC grace window exceeding the 5-minute Zero-Hop Presigned URL TTL.
type MergeConfig struct {
	Interval              time.Duration
	MaxSegmentsPerShard   int
	MaxMergedSegmentBytes int64
	MaxConcurrentMerges   int
	ReclaimDeletesRatio   float64
	S3GCGrace             time.Duration
}

// DefaultMergeConfig returns catalog-search-tuned defaults for segment merging and S3 GC.
func DefaultMergeConfig() MergeConfig {
	return MergeConfig{
		Interval:              30 * time.Second,
		MaxSegmentsPerShard:   6,
		MaxMergedSegmentBytes: 128 * 1024 * 1024, // 128 MiB cap prevents STCS-style huge rewrites
		MaxConcurrentMerges:   1,                 // strict single-merge pacing protects p99 search latency
		ReclaimDeletesRatio:   0.20,              // reclaim segments with >= 20% tombstones
		S3GCGrace:             10 * time.Minute,  // > 5m presigned S3 URL TTL for zero 404s
	}
}

// Normalize fills zero/negative fields with safe defaults while preserving explicit settings.
func (c MergeConfig) Normalize() MergeConfig {
	def := DefaultMergeConfig()
	if c.Interval <= 0 {
		c.Interval = def.Interval
	}
	if c.MaxSegmentsPerShard <= 0 {
		c.MaxSegmentsPerShard = def.MaxSegmentsPerShard
	}
	if c.MaxMergedSegmentBytes <= 0 {
		c.MaxMergedSegmentBytes = def.MaxMergedSegmentBytes
	}
	if c.MaxConcurrentMerges <= 0 {
		c.MaxConcurrentMerges = def.MaxConcurrentMerges
	}
	if c.ReclaimDeletesRatio <= 0 || c.ReclaimDeletesRatio > 1 {
		c.ReclaimDeletesRatio = def.ReclaimDeletesRatio
	}
	if c.S3GCGrace <= 0 {
		c.S3GCGrace = def.S3GCGrace
	}
	return c
}

// ForceMergeOptions controls on-demand POST /{index}/_forcemerge behavior.
type ForceMergeOptions struct {
	MaxNumSegments     int  `json:"max_num_segments"`
	OnlyExpungeDeletes bool `json:"only_expunge_deletes"`
	Flush              bool `json:"flush"`
}

// MergeResult reports the outcome of a single shard segment merge pass.
type MergeResult struct {
	Merged            bool        `json:"merged"`
	InputSegments     int         `json:"input_segments"`
	OutputSegment     *SegmentRef `json:"output_segment,omitempty"`
	TombstonesPurged  int         `json:"tombstones_purged"`
	VersionsCompacted int         `json:"versions_compacted"`
	RetiredObjects    int         `json:"retired_objects"`
}

// GCResult reports retired S3 object cleanup statistics for a shard.
type GCResult struct {
	ObjectsDeleted int `json:"objects_deleted"`
	PendingObjects int `json:"pending_objects"`
}

// SelectTieredMergeBatch selects a contiguous range [startIdx, endIdx] (inclusive) of segments
// to merge using a Lucene-style TieredMergePolicy:
//   - Never merges segments that already exceed MaxMergedSegmentBytes.
//   - Keeps active segment count near MaxSegmentsPerShard for low search fan-out.
//   - Prioritizes windows with high tombstone density (ReclaimDeletesRatio) for catalog updates/deletes.
//   - Uses contiguous windows to preserve sequence order and enable deterministic tombstone purging at index 0.
func SelectTieredMergeBatch(segments []SegmentRef, cfg MergeConfig) (startIdx, endIdx int, ok bool) {
	cfg = cfg.Normalize()
	n := len(segments)
	if n == 0 {
		return 0, 0, false
	}

	const maxMergeAtOnce = 10

	// Helper: check delete ratio for a contiguous slice [i..j].
	windowStats := func(i, j int) (totalBytes int64, liveDocs, deletedDocs int) {
		for k := i; k <= j; k++ {
			totalBytes += segments[k].SizeBytes
			liveDocs += segments[k].DocCount
			deletedDocs += segments[k].DeletedCount
		}
		return totalBytes, liveDocs, deletedDocs
	}

	// Pass 1: Segment count exceeds target floor -> pick lowest-cost merge window reducing fan-out.
	if n > cfg.MaxSegmentsPerShard {
		desiredWidth := n - cfg.MaxSegmentsPerShard + 1
		if desiredWidth < 2 {
			desiredWidth = 2
		}
		if desiredWidth > maxMergeAtOnce {
			desiredWidth = maxMergeAtOnce
		}

		bestScore := -1.0
		bestI, bestJ := -1, -1

		for i := 0; i < n-1; i++ {
			if segments[i].SizeBytes >= cfg.MaxMergedSegmentBytes {
				continue
			}
			var accBytes int64
			var accLive, accDel int
			maxJ := i + desiredWidth - 1
			if maxJ >= n {
				maxJ = n - 1
			}
			for j := i; j <= maxJ; j++ {
				if segments[j].SizeBytes >= cfg.MaxMergedSegmentBytes {
					break
				}
				if accBytes+segments[j].SizeBytes > cfg.MaxMergedSegmentBytes && j > i {
					break
				}
				accBytes += segments[j].SizeBytes
				accLive += segments[j].DocCount
				accDel += segments[j].DeletedCount
				width := j - i + 1
				if width < 2 {
					continue
				}
				// Score favors larger width reduction, smaller byte footprint, and higher delete ratio.
				delRatio := 0.0
				if total := accLive + accDel; total > 0 {
					delRatio = float64(accDel) / float64(total)
				}
				sizeFactor := 1.0 / (1.0 + float64(accBytes)/(1024*1024))
				baseBonus := 1.0
				if i == 0 && accDel > 0 {
					baseBonus = 1.25 // bonus for purging tombstones at base segment 0
				}
				score := float64(width) * (1.0 + 2.0*delRatio) * sizeFactor * baseBonus
				if score > bestScore {
					bestScore = score
					bestI, bestJ = i, j
				}
			}
		}
		if bestI >= 0 {
			return bestI, bestJ, true
		}
	}

	// Pass 2: Even when segment count <= target, reclaim high-tombstone segments by merging down to base 0.
	bestDelRatio := 0.0
	bestI, bestJ := -1, -1
	for j := 0; j < n; j++ {
		if segments[j].SizeBytes >= cfg.MaxMergedSegmentBytes {
			break
		}
		totalBytes, liveDocs, delDocs := windowStats(0, j)
		if j > 0 && totalBytes > cfg.MaxMergedSegmentBytes {
			break
		}
		if delDocs == 0 {
			continue
		}
		totalDocs := liveDocs + delDocs
		if totalDocs == 0 {
			continue
		}
		ratio := float64(delDocs) / float64(totalDocs)
		if ratio >= cfg.ReclaimDeletesRatio && ratio > bestDelRatio {
			bestDelRatio = ratio
			bestI, bestJ = 0, j
		}
	}
	if bestI >= 0 {
		return bestI, bestJ, true
	}

	return 0, 0, false
}

// MergeRange merges a contiguous slice [startIdx, endIdx] (inclusive) of committed S3 segments
// into a single immutable segment, deduplicating superseded versions by sequence number,
// deterministically purging tombstones when startIdx == 0, pre-warming NVMe cache,
// atomically swapping manifest.json on S3, and scheduling replaced objects for delayed S3 GC.
func (s *Shard) MergeRange(ctx context.Context, startIdx, endIdx int) (MergeResult, error) {
	if s.readOnly || !s.IsPrimary() {
		return MergeResult{}, nil
	}
	if s.s3 == nil {
		return MergeResult{}, fmt.Errorf("s3 client not configured")
	}

	// Serialize manifest commits with FlushToS3 without holding s.mu during S3 I/O or segment build.
	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.RLock()
	if !s.isPrimary || startIdx < 0 || endIdx >= len(s.segments) || startIdx > endIdx {
		s.mu.RUnlock()
		return MergeResult{}, nil
	}
	if startIdx == endIdx && (startIdx != 0 || s.segments[startIdx].DeletedCount == 0) {
		s.mu.RUnlock()
		return MergeResult{}, nil
	}
	batchRefs := append([]SegmentRef(nil), s.segments[startIdx:endIdx+1]...)
	allBefore := append([]SegmentRef(nil), s.segments[:startIdx]...)
	allAfter := append([]SegmentRef(nil), s.segments[endIdx+1:]...)
	flushedSeq := s.flushedSeq
	localTerm := s.primaryTerm
	if localTerm == 0 {
		localTerm = 1
	}
	ownerNodeID := s.ownerNodeID
	if ownerNodeID == "" {
		ownerNodeID = s.primaryNode
	}
	nextCounter := s.segCounter + 1
	retiredBefore := append([]RetiredSegmentRef(nil), s.retiredSegments...)
	s.mu.RUnlock()

	// Load each segment in the batch (oldest -> newest) and resolve winning DocID state by max Seq.
	type docWinner struct {
		docID   string
		seq     uint64
		deleted bool
		seg     *segment.Segment
		docIdx  uint32
		crc32c  uint32
	}
	winners := make(map[string]docWinner)
	versionsCompacted := 0

	for _, ref := range batchRefs {
		seg, err := s.loadCommittedSegment(ctx, ref)
		if err != nil {
			return MergeResult{}, fmt.Errorf("load segment %s for merge: %w", ref.ID, err)
		}
		for idx, dm := range seg.Docs {
			if prev, exists := winners[dm.DocID]; exists {
				if dm.Seq > prev.seq {
					if !prev.deleted {
						versionsCompacted++
					}
					winners[dm.DocID] = docWinner{
						docID:   dm.DocID,
						seq:     dm.Seq,
						deleted: false,
						seg:     seg,
						docIdx:  uint32(idx),
						crc32c:  dm.CRC32C,
					}
				} else {
					versionsCompacted++
				}
			} else {
				winners[dm.DocID] = docWinner{
					docID:   dm.DocID,
					seq:     dm.Seq,
					deleted: false,
					seg:     seg,
					docIdx:  uint32(idx),
					crc32c:  dm.CRC32C,
				}
			}
		}
		for docID, delSeq := range seg.Tombstones {
			if prev, exists := winners[docID]; exists {
				if delSeq >= prev.seq {
					if !prev.deleted {
						versionsCompacted++
					}
					winners[docID] = docWinner{
						docID:   docID,
						seq:     delSeq,
						deleted: true,
					}
				}
			} else {
				winners[docID] = docWinner{
					docID:   docID,
					seq:     delSeq,
					deleted: true,
				}
			}
		}
	}

	// Deterministic tombstone purging: safe iff no older unmerged segments precede startIdx.
	dropTombstones := startIdx == 0
	tombstonesPurged := 0
	inputs := make([]segment.InputDoc, 0, len(winners))

	for _, w := range winners {
		if w.deleted {
			if dropTombstones {
				tombstonesPurged++
				continue
			}
			inputs = append(inputs, segment.InputDoc{
				DocID:   w.docID,
				Seq:     w.seq,
				Deleted: true,
			})
			continue
		}
		raw, _, err := s.ReadDocSource(ctx, w.seg, w.docIdx)
		if err != nil {
			return MergeResult{}, fmt.Errorf("read doc %s during merge: %w", w.docID, err)
		}
		inputs = append(inputs, segment.InputDoc{
			DocID:   w.docID,
			Seq:     w.seq,
			Deleted: false,
			RawJSON: raw,
			CRC32C:  checksum.Sum32C(raw),
		})
	}

	var newRef *SegmentRef
	var newSeg *segment.Segment
	var newIdxBytes, newDocsBytes []byte

	if len(inputs) > 0 {
		segID := fmt.Sprintf("seg-%06d", nextCounter)
		idxKey := fmt.Sprintf("%s/shard-%d/%s.idx", s.indexName, s.shardID, segID)
		docsKey := fmt.Sprintf("%s/shard-%d/%s.docs", s.indexName, s.shardID, segID)

		seg, idxBytes, docsBytes, err := segment.Build(segID, s.indexName, s.shardID, inputs, idxKey, docsKey)
		if err != nil {
			return MergeResult{}, fmt.Errorf("build merged segment: %w", err)
		}
		docsCRC, err := s.s3.PutObject(ctx, docsKey, docsBytes)
		if err != nil {
			return MergeResult{}, fmt.Errorf("upload merged docs: %w", err)
		}
		idxCRC, err := s.s3.PutObject(ctx, idxKey, idxBytes)
		if err != nil {
			return MergeResult{}, fmt.Errorf("upload merged idx: %w", err)
		}
		seg.DocsCRC32C = docsCRC

		ref := SegmentRef{
			ID:           segID,
			IdxKey:       idxKey,
			DocsKey:      docsKey,
			IdxCRC32C:    idxCRC,
			DocsCRC32C:   docsCRC,
			DocCount:     seg.LiveDocs,
			DeletedCount: len(seg.Tombstones),
			SizeBytes:    int64(len(idxBytes) + len(docsBytes)),
			MinSeq:       seg.MinSeq,
			MaxSeq:       seg.MaxSeq,
			CreatedAt:    time.Now().UTC().Unix(),
		}
		newRef = &ref
		newSeg = seg
		newIdxBytes = idxBytes
		newDocsBytes = docsBytes
	}

	// Build replacement segment list and record retired segments for grace-period S3 GC.
	nextSegments := make([]SegmentRef, 0, len(allBefore)+len(allAfter)+1)
	nextSegments = append(nextSegments, allBefore...)
	if newRef != nil {
		nextSegments = append(nextSegments, *newRef)
	}
	nextSegments = append(nextSegments, allAfter...)

	nowUnix := time.Now().UTC().Unix()
	nextRetired := make([]RetiredSegmentRef, 0, len(retiredBefore)+len(batchRefs))
	nextRetired = append(nextRetired, retiredBefore...)
	for _, old := range batchRefs {
		nextRetired = append(nextRetired, RetiredSegmentRef{
			ID:        old.ID,
			IdxKey:    old.IdxKey,
			DocsKey:   old.DocsKey,
			SizeBytes: old.SizeBytes,
			RetiredAt: nowUnix,
		})
	}

	if err := s.checkPrimaryTermFencing(ctx, localTerm, ownerNodeID); err != nil {
		return MergeResult{}, err
	}

	man := ShardManifest{
		IndexName:       s.indexName,
		ShardID:         s.shardID,
		PrimaryTerm:     localTerm,
		PrimaryNode:     ownerNodeID,
		FlushedSeq:      flushedSeq,
		NextSegCounter:  nextCounter,
		Segments:        nextSegments,
		RetiredSegments: nextRetired,
	}
	manBytes, err := json.Marshal(man)
	if err != nil {
		return MergeResult{}, err
	}

	// Atomic S3 manifest commit BEFORE mutating local shard state or NVMe cache.
	if _, err := s.s3.PutObject(ctx, manifestKey(s.indexName, s.shardID), manBytes); err != nil {
		return MergeResult{}, fmt.Errorf("commit merged manifest: %w", err)
	}

	// Pre-warm NVMe read cache with merged segment BEFORE exposing it so catalog searches stay hot.
	if newRef != nil && s.nvmeCache != nil {
		_ = s.nvmeCache.Put(newRef.IdxKey, newIdxBytes, newRef.IdxCRC32C)
		_ = s.nvmeCache.Put(newRef.DocsKey, newDocsBytes, newRef.DocsCRC32C)
	}

	s.segCacheMu.Lock()
	if newRef != nil && newSeg != nil {
		s.decodedSegs[newRef.IdxKey] = newSeg
	}
	for _, old := range batchRefs {
		delete(s.decodedSegs, old.IdxKey)
	}
	s.committedLatestSeq = nil
	s.committedManifestVer = 0
	s.segCacheMu.Unlock()

	s.mu.Lock()
	s.segments = nextSegments
	s.retiredSegments = nextRetired
	s.segCounter = nextCounter
	s.manifestVer++
	s.mu.Unlock()

	// Proactively evict superseded segment keys from local NVMe cache to keep cache utilization lean.
	if s.nvmeCache != nil {
		for _, old := range batchRefs {
			s.nvmeCache.Delete(old.IdxKey)
			s.nvmeCache.Delete(old.DocsKey)
		}
	}

	return MergeResult{
		Merged:            true,
		InputSegments:     len(batchRefs),
		OutputSegment:     newRef,
		TombstonesPurged:  tombstonesPurged,
		VersionsCompacted: versionsCompacted,
		RetiredObjects:    len(batchRefs) * 2,
	}, nil
}

// MergeIfNeeded evaluates TieredMergePolicy on this shard and executes one bounded merge pass if warranted.
func (s *Shard) MergeIfNeeded(ctx context.Context, cfg MergeConfig) (MergeResult, error) {
	if s.readOnly || !s.IsPrimary() || s.s3 == nil {
		return MergeResult{}, nil
	}
	s.mu.RLock()
	snapshot := append([]SegmentRef(nil), s.segments...)
	s.mu.RUnlock()

	startIdx, endIdx, ok := SelectTieredMergeBatch(snapshot, cfg)
	if !ok {
		return MergeResult{}, nil
	}
	return s.MergeRange(ctx, startIdx, endIdx)
}

// ForceMerge compacts segments according to Elasticsearch _forcemerge semantics
// (max_num_segments or only_expunge_deletes).
func (s *Shard) ForceMerge(ctx context.Context, opts ForceMergeOptions) (MergeResult, error) {
	if s.readOnly || !s.IsPrimary() || s.s3 == nil {
		return MergeResult{}, nil
	}
	if opts.Flush {
		if _, err := s.FlushToS3(ctx); err != nil {
			return MergeResult{}, err
		}
	}

	s.mu.RLock()
	segs := append([]SegmentRef(nil), s.segments...)
	s.mu.RUnlock()

	n := len(segs)
	if n == 0 {
		return MergeResult{}, nil
	}

	if opts.OnlyExpungeDeletes {
		lastDirty := -1
		for i, seg := range segs {
			if seg.DeletedCount > 0 {
				lastDirty = i
			}
		}
		if lastDirty < 0 {
			return MergeResult{}, nil
		}
		return s.MergeRange(ctx, 0, lastDirty)
	}

	target := opts.MaxNumSegments
	if target <= 0 {
		target = 1
	}
	if n > target {
		// Merge oldest (n - target + 1) segments into 1 segment -> leaves exactly target segments.
		return s.MergeRange(ctx, 0, n-target)
	}
	if target == 1 && n == 1 && segs[0].DeletedCount > 0 {
		return s.MergeRange(ctx, 0, 0)
	}
	return MergeResult{}, nil
}

// SweepRetiredS3Objects deletes retired segment .idx and .docs objects from S3 once their
// grace period has elapsed and updates manifest.json to prune them.
func (s *Shard) SweepRetiredS3Objects(ctx context.Context, grace time.Duration) (GCResult, error) {
	if s.readOnly || !s.IsPrimary() || s.s3 == nil {
		return GCResult{}, nil
	}

	s.flushMu.Lock()
	defer s.flushMu.Unlock()

	s.mu.RLock()
	if !s.isPrimary {
		s.mu.RUnlock()
		return GCResult{}, nil
	}
	retired := append([]RetiredSegmentRef(nil), s.retiredSegments...)
	segments := append([]SegmentRef(nil), s.segments...)
	flushedSeq := s.flushedSeq
	segCounter := s.segCounter
	localTerm := s.primaryTerm
	if localTerm == 0 {
		localTerm = 1
	}
	ownerNodeID := s.ownerNodeID
	if ownerNodeID == "" {
		ownerNodeID = s.primaryNode
	}
	s.mu.RUnlock()

	if len(retired) == 0 {
		return GCResult{}, nil
	}
	if err := s.checkPrimaryTermFencing(ctx, localTerm, ownerNodeID); err != nil {
		return GCResult{PendingObjects: len(retired) * 2}, err
	}

	now := time.Now().UTC()
	var remaining []RetiredSegmentRef
	deletedObjs := 0

	for _, r := range retired {
		retiredAt := time.Unix(r.RetiredAt, 0).UTC()
		if grace > 0 && now.Sub(retiredAt) < grace {
			remaining = append(remaining, r)
			continue
		}
		delOK := true
		if r.IdxKey != "" {
			if err := s.s3.DeleteObject(ctx, r.IdxKey); err != nil && !errors.Is(err, s3store.ErrNotFound) {
				delOK = false
			} else {
				deletedObjs++
			}
		}
		if r.DocsKey != "" {
			if err := s.s3.DeleteObject(ctx, r.DocsKey); err != nil && !errors.Is(err, s3store.ErrNotFound) {
				delOK = false
			} else {
				deletedObjs++
			}
		}
		if !delOK {
			remaining = append(remaining, r)
		}
	}

	if deletedObjs == 0 {
		return GCResult{
			ObjectsDeleted: 0,
			PendingObjects: len(remaining) * 2,
		}, nil
	}

	man := ShardManifest{
		IndexName:       s.indexName,
		ShardID:         s.shardID,
		PrimaryTerm:     localTerm,
		PrimaryNode:     ownerNodeID,
		FlushedSeq:      flushedSeq,
		NextSegCounter:  segCounter,
		Segments:        segments,
		RetiredSegments: remaining,
	}
	manBytes, err := json.Marshal(man)
	if err != nil {
		return GCResult{ObjectsDeleted: deletedObjs, PendingObjects: len(remaining) * 2}, err
	}
	if _, err := s.s3.PutObject(ctx, manifestKey(s.indexName, s.shardID), manBytes); err != nil {
		return GCResult{ObjectsDeleted: deletedObjs, PendingObjects: len(remaining) * 2}, err
	}

	s.mu.Lock()
	s.retiredSegments = remaining
	s.mu.Unlock()

	return GCResult{
		ObjectsDeleted: deletedObjs,
		PendingObjects: len(remaining) * 2,
	}, nil
}

func (s *Shard) loadCommittedSegment(ctx context.Context, ref SegmentRef) (*segment.Segment, error) {
	s.segCacheMu.RLock()
	cachedSeg := s.decodedSegs[ref.IdxKey]
	s.segCacheMu.RUnlock()
	if cachedSeg != nil {
		if s.nvmeCache == nil {
			return cachedSeg, nil
		}
		if ok, err := s.nvmeCache.VerifyCRC(ref.IdxKey, ref.IdxCRC32C); ok && err == nil {
			return cachedSeg, nil
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
		return nil, err
	}
	seg, err := segment.UnmarshalIndex(idxBytes)
	if err != nil {
		return nil, err
	}
	seg.DocsCRC32C = ref.DocsCRC32C
	return seg, nil
}

func manifestSegCounter(man ShardManifest) int {
	maxC := man.NextSegCounter
	if len(man.Segments) > maxC {
		maxC = len(man.Segments)
	}
	for _, seg := range man.Segments {
		if n := parseSegNumber(seg.ID); n > maxC {
			maxC = n
		}
	}
	for _, r := range man.RetiredSegments {
		if n := parseSegNumber(r.ID); n > maxC {
			maxC = n
		}
	}
	return maxC
}

func parseSegNumber(id string) int {
	if !strings.HasPrefix(id, "seg-") {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimPrefix(id, "seg-"))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func segmentRefsEqual(a, b []SegmentRef) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID || a[i].IdxCRC32C != b[i].IdxCRC32C {
			return false
		}
	}
	return true
}
