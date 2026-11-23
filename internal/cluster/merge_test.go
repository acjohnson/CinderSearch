package cluster_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cindersearch/cindersearch/internal/api"
	"github.com/cindersearch/cindersearch/internal/cache"
	"github.com/cindersearch/cindersearch/internal/checksum"
	"github.com/cindersearch/cindersearch/internal/cluster"
	"github.com/cindersearch/cindersearch/internal/query"
	"github.com/cindersearch/cindersearch/internal/s3store"
	"github.com/cindersearch/cindersearch/internal/sharding"
)

func TestTieredMergePolicySelection(t *testing.T) {
	cfg := cluster.DefaultMergeConfig()
	const mib = 1024 * 1024

	// 1. Clean low-segment-count shard (below MaxSegmentsPerShard=6, 0 deletes) -> no merge needed.
	clean := []cluster.SegmentRef{
		{ID: "seg-000001", DocCount: 100, SizeBytes: 4 * mib},
		{ID: "seg-000002", DocCount: 100, SizeBytes: 4 * mib},
		{ID: "seg-000003", DocCount: 100, SizeBytes: 4 * mib},
	}
	if _, _, ok := cluster.SelectTieredMergeBatch(clean, cfg); ok {
		t.Fatalf("expected no merge for 3 clean segments below target floor")
	}

	// 2. Segment count exceeds MaxSegmentsPerShard (8 > 6) -> selects contiguous merge batch.
	many := make([]cluster.SegmentRef, 8)
	for i := range many {
		many[i] = cluster.SegmentRef{
			ID:        fmt.Sprintf("seg-%06d", i+1),
			DocCount:  50,
			SizeBytes: 2 * mib,
		}
	}
	start, end, ok := cluster.SelectTieredMergeBatch(many, cfg)
	if !ok || end <= start || start < 0 || end >= len(many) {
		t.Fatalf("expected contiguous merge batch for 8 small segments, got [%d..%d] ok=%v", start, end, ok)
	}

	// 3. Oversized segments (>= MaxMergedSegmentBytes=128 MiB) are never selected for rewrite.
	oversized := make([]cluster.SegmentRef, 8)
	for i := range oversized {
		oversized[i] = cluster.SegmentRef{
			ID:        fmt.Sprintf("seg-%06d", i+1),
			DocCount:  50000,
			SizeBytes: 140 * mib,
		}
	}
	if s, e, ok := cluster.SelectTieredMergeBatch(oversized, cfg); ok {
		t.Fatalf("expected oversized segments (>=128 MiB) to be skipped, got [%d..%d]", s, e)
	}

	// Mixed oversized base segment + small trailing segments exceeding target count:
	// oversized index 0 must be excluded while small trailing segments merge.
	mixed := append([]cluster.SegmentRef{{ID: "seg-000001", DocCount: 50000, SizeBytes: 150 * mib}}, many[:6]...)
	start, end, ok = cluster.SelectTieredMergeBatch(mixed, cfg)
	if !ok || start == 0 || end <= start {
		t.Fatalf("expected oversized seg 0 to be excluded from merge window, got [%d..%d] ok=%v", start, end, ok)
	}

	// 4. High tombstone density (>= 20%) triggers reclaim merge down to base index 0 even when len <= 6.
	dirty := []cluster.SegmentRef{
		{ID: "seg-000001", DocCount: 100, DeletedCount: 0, SizeBytes: 4 * mib},
		{ID: "seg-000002", DocCount: 10, DeletedCount: 40, SizeBytes: 1 * mib},
		{ID: "seg-000003", DocCount: 50, DeletedCount: 0, SizeBytes: 2 * mib},
	}
	start, end, ok = cluster.SelectTieredMergeBatch(dirty, cfg)
	if !ok || start != 0 || end < 1 {
		t.Fatalf("expected high-tombstone reclaim merge starting at base 0, got [%d..%d] ok=%v", start, end, ok)
	}
}

func TestShardMergeTombstoneReclamationAndNoZombieResurrection(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-mrg", "sk-mrg", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-merge-tombstone",
		AccessKey: "ak-mrg",
		SecretKey: "sk-mrg",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	nvmeCache, err := cache.NewNVMeCache(t.TempDir(), 32*1024*1024)
	if err != nil {
		t.Fatalf("new nvme cache: %v", err)
	}
	sh, err := cluster.OpenShard(ctx, "catalog", 0, true, t.TempDir(), nvmeCache, s3Client)
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}
	defer sh.Close()

	// Segment 1 (base): sku-1 v1, sku-2 v1, sku-keep v1.
	if _, err := sh.IndexLocal("sku-1", []byte(`{"sku":"sku-1","price":100,"title":"widget v1"}`)); err != nil {
		t.Fatalf("index sku-1 v1: %v", err)
	}
	if _, err := sh.IndexLocal("sku-2", []byte(`{"sku":"sku-2","price":200,"title":"retired gadget"}`)); err != nil {
		t.Fatalf("index sku-2 v1: %v", err)
	}
	if _, err := sh.IndexLocal("sku-keep", []byte(`{"sku":"sku-keep","price":50,"title":"steady item"}`)); err != nil {
		t.Fatalf("index sku-keep: %v", err)
	}
	ref1, err := sh.FlushToS3(ctx)
	if err != nil || ref1 == nil {
		t.Fatalf("flush seg-1: ref=%v err=%v", ref1, err)
	}

	// Segment 2: update sku-1 -> v2, delete sku-2 (tombstone).
	if _, err := sh.IndexLocal("sku-1", []byte(`{"sku":"sku-1","price":150,"title":"widget v2"}`)); err != nil {
		t.Fatalf("index sku-1 v2: %v", err)
	}
	if _, err := sh.DeleteLocal("sku-2"); err != nil {
		t.Fatalf("delete sku-2: %v", err)
	}
	ref2, err := sh.FlushToS3(ctx)
	if err != nil || ref2 == nil {
		t.Fatalf("flush seg-2: ref=%v err=%v", ref2, err)
	}
	if ref2.DeletedCount != 1 {
		t.Fatalf("expected seg-2 DeletedCount=1, got %d", ref2.DeletedCount)
	}

	// Segment 3: update sku-1 -> v3.
	if _, err := sh.IndexLocal("sku-1", []byte(`{"sku":"sku-1","price":300,"title":"widget v3"}`)); err != nil {
		t.Fatalf("index sku-1 v3: %v", err)
	}
	ref3, err := sh.FlushToS3(ctx)
	if err != nil || ref3 == nil {
		t.Fatalf("flush seg-3: ref=%v err=%v", ref3, err)
	}

	// Phase A: Non-base merge [1..2] (seg-2 + seg-3). Must RETAIN tombstone for sku-2 so seg-1 never resurrects it!
	nonBaseRes, err := sh.MergeRange(ctx, 1, 2)
	if err != nil {
		t.Fatalf("non-base MergeRange(1,2): %v", err)
	}
	if !nonBaseRes.Merged || nonBaseRes.OutputSegment == nil {
		t.Fatalf("expected merged output segment from MergeRange(1,2): %+v", nonBaseRes)
	}
	if nonBaseRes.TombstonesPurged != 0 {
		t.Fatalf("non-base merge must NOT purge tombstones, purged=%d", nonBaseRes.TombstonesPurged)
	}
	if nonBaseRes.OutputSegment.DeletedCount != 1 {
		t.Fatalf("expected retained tombstone DeletedCount=1 in non-base merged segment, got %d", nonBaseRes.OutputSegment.DeletedCount)
	}
	if nonBaseRes.VersionsCompacted < 1 {
		t.Fatalf("expected superseded sku-1 v2 compacted, got VersionsCompacted=%d", nonBaseRes.VersionsCompacted)
	}

	// Anti-zombie invariant: sku-2 MUST remain deleted!
	if raw, found, _, _, _, err := sh.GetDocByID(ctx, "sku-2"); err != nil || found {
		t.Fatalf("zombie resurrection detected for sku-2 after non-base merge! found=%v err=%v raw=%s", found, err, string(raw))
	}
	raw1, found, _, _, _, err := sh.GetDocByID(ctx, "sku-1")
	if err != nil || !found || !bytes.Contains(raw1, []byte(`"price":300`)) {
		t.Fatalf("expected sku-1 v3 intact after non-base merge, found=%v err=%v raw=%s", found, err, string(raw1))
	}

	// NVMe cache must be pre-warmed with the new merged segment and have evicted old seg-2 & seg-3 keys.
	if !sh.IsDocsCachedOnNVMe(nonBaseRes.OutputSegment.DocsKey) {
		t.Fatalf("expected merged segment %s to be pre-warmed on NVMe cache", nonBaseRes.OutputSegment.DocsKey)
	}
	if sh.IsDocsCachedOnNVMe(ref2.DocsKey) || sh.IsDocsCachedOnNVMe(ref3.DocsKey) {
		t.Fatalf("expected superseded seg-2 and seg-3 docs keys to be evicted from NVMe cache")
	}

	// Phase B: Base merge [0..1] (seg-1 + merged seg-4). Now startIdx == 0, so tombstones MUST purge deterministically!
	baseRes, err := sh.MergeRange(ctx, 0, 1)
	if err != nil {
		t.Fatalf("base MergeRange(0,1): %v", err)
	}
	if !baseRes.Merged || baseRes.OutputSegment == nil {
		t.Fatalf("expected merged output segment from base MergeRange(0,1): %+v", baseRes)
	}
	if baseRes.TombstonesPurged != 1 {
		t.Fatalf("expected base merge to deterministically purge 1 tombstone, got %d", baseRes.TombstonesPurged)
	}
	if baseRes.OutputSegment.DeletedCount != 0 || baseRes.OutputSegment.DocCount != 2 {
		t.Fatalf("expected 2 live docs and 0 tombstones after base merge, got %+v", baseRes.OutputSegment)
	}

	if _, found, _, _, _, err := sh.GetDocByID(ctx, "sku-2"); err != nil || found {
		t.Fatalf("sku-2 must remain permanently deleted after base merge, found=%v err=%v", found, err)
	}
	raw1, found, _, _, _, err = sh.GetDocByID(ctx, "sku-1")
	if err != nil || !found || !bytes.Contains(raw1, []byte(`"price":300`)) {
		t.Fatalf("expected sku-1 v3 intact after base merge, found=%v err=%v raw=%s", found, err, string(raw1))
	}
	rawKeep, found, _, _, _, err := sh.GetDocByID(ctx, "sku-keep")
	if err != nil || !found || !bytes.Contains(rawKeep, []byte(`"steady item"`)) {
		t.Fatalf("expected sku-keep intact after base merge, found=%v err=%v raw=%s", found, err, string(rawKeep))
	}
	if !sh.IsDocsCachedOnNVMe(baseRes.OutputSegment.DocsKey) {
		t.Fatalf("expected final base-merged segment pre-warmed on NVMe")
	}
	if sh.IsDocsCachedOnNVMe(ref1.DocsKey) || sh.IsDocsCachedOnNVMe(nonBaseRes.OutputSegment.DocsKey) {
		t.Fatalf("expected superseded segments evicted from NVMe after base merge")
	}
}

func TestS3GCGraceWindowPreservesPresignedURLsAndSurvivesColdBoot(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-gc", "sk-gc", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-gc-grace",
		AccessKey: "ak-gc",
		SecretKey: "sk-gc",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	nvmeDir := t.TempDir()
	nvmeCache, err := cache.NewNVMeCache(t.TempDir(), 32*1024*1024)
	if err != nil {
		t.Fatalf("new nvme cache: %v", err)
	}
	sh, err := cluster.OpenShard(ctx, "products", 0, true, nvmeDir, nvmeCache, s3Client)
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}

	// Flush two segments so we have an in-flight presigned URL pointing to seg-000001.docs.
	if _, err := sh.IndexLocal("prod-1", []byte(`{"sku":"prod-1","name":"espresso machine"}`)); err != nil {
		t.Fatalf("index prod-1: %v", err)
	}
	ref1, err := sh.FlushToS3(ctx)
	if err != nil || ref1 == nil {
		t.Fatalf("flush seg-1: ref=%v err=%v", ref1, err)
	}

	segsBefore, _, err := sh.ActiveSegments(ctx)
	if err != nil || len(segsBefore) != 1 || len(segsBefore[0].Docs) != 1 {
		t.Fatalf("expected 1 active segment with 1 doc, got segs=%d err=%v", len(segsBefore), err)
	}
	dm1 := segsBefore[0].Docs[0]
	presignedURL, err := s3Client.PresignGetURL(ref1.DocsKey, 5*time.Minute)
	if err != nil {
		t.Fatalf("presign seg-1 docs: %v", err)
	}
	byteRange := fmt.Sprintf("bytes=%d-%d", dm1.Offset, dm1.Offset+dm1.Length-1)

	if _, err := sh.IndexLocal("prod-2", []byte(`{"sku":"prod-2","name":"burr grinder"}`)); err != nil {
		t.Fatalf("index prod-2: %v", err)
	}
	ref2, err := sh.FlushToS3(ctx)
	if err != nil || ref2 == nil {
		t.Fatalf("flush seg-2: ref=%v err=%v", ref2, err)
	}

	// ForceMerge into 1 segment (retires seg-000001 and seg-000002 -> 4 S3 objects pending GC).
	fmRes, err := sh.ForceMerge(ctx, cluster.ForceMergeOptions{MaxNumSegments: 1, Flush: true})
	if err != nil || !fmRes.Merged || fmRes.RetiredObjects != 4 {
		t.Fatalf("ForceMerge failed: res=%+v err=%v", fmRes, err)
	}

	// Sweep during active 10m grace window must NOT delete retired S3 objects!
	gcEarly, err := sh.SweepRetiredS3Objects(ctx, 10*time.Minute)
	if err != nil {
		t.Fatalf("early GC sweep: %v", err)
	}
	if gcEarly.ObjectsDeleted != 0 || gcEarly.PendingObjects != 4 {
		t.Fatalf("expected 0 deleted and 4 pending during 10m grace window, got %+v", gcEarly)
	}

	// In-flight Zero-Hop Presigned HTTP 206 Range request against retired seg-1 MUST succeed (zero 404s!).
	rangeReq, _ := http.NewRequest(http.MethodGet, presignedURL, nil)
	rangeReq.Header.Set("Range", byteRange)
	rangeResp, err := http.DefaultClient.Do(rangeReq)
	if err != nil {
		t.Fatalf("presigned range GET during grace window: %v", err)
	}
	rawDoc, _ := io.ReadAll(rangeResp.Body)
	_ = rangeResp.Body.Close()
	if rangeResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206 Partial Content during grace window, got %d", rangeResp.StatusCode)
	}
	if checksum.Sum32C(rawDoc) != dm1.CRC32C || !bytes.Contains(rawDoc, []byte("espresso machine")) {
		t.Fatalf("corrupted presigned doc during grace window: %s", string(rawDoc))
	}

	// Simulate pod restart / cold boot with fresh NVMe cache: retired_segments queue & monotonic segCounter must survive!
	_ = sh.Close()
	coldCache, err := cache.NewNVMeCache(t.TempDir(), 32*1024*1024)
	if err != nil {
		t.Fatalf("new cold cache: %v", err)
	}
	reopened, err := cluster.OpenShard(ctx, "products", 0, true, nvmeDir, coldCache, s3Client)
	if err != nil {
		t.Fatalf("reopen shard after cold boot: %v", err)
	}
	defer reopened.Close()

	st := reopened.Stats()
	if st.SegmentCount != 1 || st.PendingGCObjects != 4 {
		t.Fatalf("expected 1 active segment and 4 pending GC objects after cold boot, got %+v", st)
	}

	// Verify monotonic segment ID counter prevents key collision after merge + cold boot.
	if _, err := reopened.IndexLocal("prod-3", []byte(`{"sku":"prod-3","name":"milk frother"}`)); err != nil {
		t.Fatalf("index prod-3: %v", err)
	}
	ref4, err := reopened.FlushToS3(ctx)
	if err != nil || ref4 == nil {
		t.Fatalf("flush post-boot seg: ref=%v err=%v", ref4, err)
	}
	if ref4.ID != "seg-000004" {
		t.Fatalf("expected monotonic next segment ID seg-000004, got %s", ref4.ID)
	}

	// Grace period expires (grace=0) -> sweep must delete all 4 retired S3 objects and prune manifest.json.
	gcFinal, err := reopened.SweepRetiredS3Objects(ctx, 0)
	if err != nil {
		t.Fatalf("expired GC sweep: %v", err)
	}
	if gcFinal.ObjectsDeleted != 4 || gcFinal.PendingObjects != 0 {
		t.Fatalf("expected 4 deleted and 0 pending after grace expiry, got %+v", gcFinal)
	}

	if _, _, err := s3Client.GetObject(ctx, ref1.IdxKey); !errors.Is(err, s3store.ErrNotFound) {
		t.Fatalf("expected retired %s deleted from S3, got err=%v", ref1.IdxKey, err)
	}
	if _, _, err := s3Client.GetObject(ctx, ref1.DocsKey); !errors.Is(err, s3store.ErrNotFound) {
		t.Fatalf("expected retired %s deleted from S3, got err=%v", ref1.DocsKey, err)
	}

	// All live docs must remain readable after retired objects are deleted from S3.
	for _, id := range []string{"prod-1", "prod-2", "prod-3"} {
		if _, found, _, _, _, err := reopened.GetDocByID(ctx, id); err != nil || !found {
			t.Fatalf("expected %s readable after S3 GC sweep, found=%v err=%v", id, found, err)
		}
	}
}

func TestMergeS3FaultInjectionIsRollbackSafe(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-mf", "sk-mf", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-merge-fault",
		AccessKey: "ak-mf",
		SecretKey: "sk-mf",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	nvmeCache, err := cache.NewNVMeCache(t.TempDir(), 32*1024*1024)
	if err != nil {
		t.Fatalf("new nvme cache: %v", err)
	}
	sh, err := cluster.OpenShard(ctx, "inventory", 0, true, t.TempDir(), nvmeCache, s3Client)
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}
	defer sh.Close()

	if _, err := sh.IndexLocal("item-1", []byte(`{"item":"keyboard"}`)); err != nil {
		t.Fatalf("index item-1: %v", err)
	}
	if _, err := sh.FlushToS3(ctx); err != nil {
		t.Fatalf("flush seg-1: %v", err)
	}
	if _, err := sh.IndexLocal("item-2", []byte(`{"item":"mouse"}`)); err != nil {
		t.Fatalf("index item-2: %v", err)
	}
	if _, err := sh.FlushToS3(ctx); err != nil {
		t.Fatalf("flush seg-2: %v", err)
	}

	// Inject S3 500 on manifest.json PUT during ForceMerge: pre-merge shard state must remain untouched!
	s3Srv.SetFaultInjector(func(method, bucket, key string) (int, string) {
		if method == http.MethodPut && strings.HasSuffix(key, "manifest.json") {
			return http.StatusInternalServerError, "simulated s3 manifest outage during merge"
		}
		return 0, ""
	})

	if _, err := sh.ForceMerge(ctx, cluster.ForceMergeOptions{MaxNumSegments: 1, Flush: false}); err == nil {
		t.Fatalf("expected ForceMerge to fail while S3 manifest PUT returns 500")
	}

	st := sh.Stats()
	if st.SegmentCount != 2 || st.PendingGCObjects != 0 {
		t.Fatalf("expected rollback-safe pre-merge state (2 segments, 0 pending GC), got %+v", st)
	}
	for _, id := range []string{"item-1", "item-2"} {
		if _, found, _, _, _, err := sh.GetDocByID(ctx, id); err != nil || !found {
			t.Fatalf("expected %s intact after failed merge commit, found=%v err=%v", id, found, err)
		}
	}

	// Clear fault injector and verify retry succeeds cleanly.
	s3Srv.SetFaultInjector(nil)
	res, err := sh.ForceMerge(ctx, cluster.ForceMergeOptions{MaxNumSegments: 1, Flush: false})
	if err != nil || !res.Merged {
		t.Fatalf("expected ForceMerge retry after S3 recovery to succeed, res=%+v err=%v", res, err)
	}
	if sh.Stats().SegmentCount != 1 {
		t.Fatalf("expected 1 merged segment after recovery, got %d", sh.Stats().SegmentCount)
	}
	for _, id := range []string{"item-1", "item-2"} {
		if _, found, _, _, _, err := sh.GetDocByID(ctx, id); err != nil || !found {
			t.Fatalf("expected %s readable after recovered merge, found=%v err=%v", id, found, err)
		}
	}
}

func TestConcurrentSearchWriteAndForceMergeRaceSafe(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-cr", "sk-cr", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-merge-race",
		AccessKey: "ak-cr",
		SecretKey: "sk-cr",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	specs := []struct {
		id    string
		roles []string
	}{
		{id: "data-1", roles: []string{sharding.RoleData}},
		{id: "coord-1", roles: []string{sharding.RoleCoordinator}},
		{id: "reader-1", roles: []string{sharding.RoleReader}},
	}
	nodes := make([]*cluster.Node, len(specs))
	httpSrvs := make([]*httptest.Server, len(specs))
	peers := make([]sharding.NodeInfo, len(specs))

	for i, sp := range specs {
		n, err := cluster.NewNode(cluster.NodeConfig{
			NodeID:            sp.id,
			Roles:             sp.roles,
			NVMeDir:           t.TempDir(),
			NVMeMaxBytes:      32 * 1024 * 1024,
			FlushInterval:     40 * time.Millisecond,
			ReplicateInterval: 40 * time.Millisecond,
			ScrubInterval:     100 * time.Millisecond,
			Merge: cluster.MergeConfig{
				Interval:              50 * time.Millisecond,
				MaxSegmentsPerShard:   3,
				MaxMergedSegmentBytes: 64 * 1024 * 1024,
				MaxConcurrentMerges:   1,
				ReclaimDeletesRatio:   0.20,
				S3GCGrace:             10 * time.Minute,
			},
			S3: s3Client,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", sp.id, err)
		}
		nodes[i] = n
		httpSrvs[i] = httptest.NewServer(api.NewHandler(n))
		peers[i] = sharding.NodeInfo{
			ID:      sp.id,
			Address: strings.TrimPrefix(httpSrvs[i].URL, "http://"),
			Roles:   sp.roles,
		}
	}
	defer func() {
		for i := range nodes {
			_ = nodes[i].Close()
			httpSrvs[i].Close()
		}
	}()
	for _, n := range nodes {
		n.UpdatePeers(peers)
	}

	dataNode, coordNode, readerNode := nodes[0], nodes[1], nodes[2]

	if err := coordNode.CreateIndex(ctx, cluster.IndexMetadata{
		Name:     "catalog",
		Settings: cluster.IndexSettings{NumberOfShards: 2, NumberOfReplicas: 0},
	}, true); err != nil {
		t.Fatalf("create catalog index: %v", err)
	}

	// Seed 2 segments and warm reader-1 before concurrent merges shrink the manifest.
	for i := 0; i < 6; i++ {
		body := fmt.Sprintf(`{"sku":"seed-%d","category":"books","active":true}`, i)
		if _, _, _, _, err := coordNode.IndexDoc(ctx, "catalog", fmt.Sprintf("seed-%d", i), []byte(body), ""); err != nil {
			t.Fatalf("seed doc %d: %v", i, err)
		}
		if i == 2 || i == 5 {
			if err := coordNode.RefreshIndex(ctx, "catalog", true, true); err != nil {
				t.Fatalf("seed flush: %v", err)
			}
		}
	}
	if _, err := coordNode.SearchWithPreference(ctx, []string{"catalog"}, query.SearchRequest{
		Query: map[string]any{"match": map[string]any{"category": "books"}},
	}, cluster.RetrievalDropIn, true); err != nil {
		t.Fatalf("warm reader search: %v", err)
	}

	var wg sync.WaitGroup
	// 2 writer goroutines: each writes 12 docs, updates half, and deletes 4 -> 8 live docs per worker (16 total).
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 12; i++ {
				id := fmt.Sprintf("w%d-%d", worker, i)
				body := fmt.Sprintf(`{"sku":%q,"category":"books","rev":1}`, id)
				if _, _, _, _, err := coordNode.IndexDoc(ctx, "catalog", id, []byte(body), ""); err != nil {
					t.Errorf("write %s: %v", id, err)
				}
				if i%2 == 0 {
					upd := fmt.Sprintf(`{"sku":%q,"category":"books","rev":2}`, id)
					if _, _, _, _, err := coordNode.IndexDoc(ctx, "catalog", id, []byte(upd), ""); err != nil {
						t.Errorf("update %s: %v", id, err)
					}
				}
				if i < 4 {
					if _, _, _, err := coordNode.DeleteDoc(ctx, "catalog", id); err != nil {
						t.Errorf("delete %s: %v", id, err)
					}
				}
				if i%4 == 3 {
					_ = coordNode.RefreshIndex(ctx, "catalog", true, true)
				}
			}
		}(w)
	}

	// 2 concurrent searcher goroutines hitting both data and reader paths while merges run.
	for s := 0; s < 2; s++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			preferReader := id == 1
			for i := 0; i < 15; i++ {
				if _, err := coordNode.SearchWithPreference(ctx, []string{"catalog"}, query.SearchRequest{
					Query: map[string]any{"match": map[string]any{"category": "books"}},
				}, cluster.RetrievalDropIn, preferReader); err != nil {
					t.Errorf("concurrent search (reader=%v): %v", preferReader, err)
				}
				time.Sleep(4 * time.Millisecond)
			}
		}(s)
	}

	// 1 compactor goroutine triggering explicit merges & background merge passes concurrently.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			_, _ = dataNode.RunMergeAndGCOnce(ctx)
			_, _ = coordNode.ForceMerge(ctx, []string{"catalog"}, cluster.ForceMergeOptions{
				MaxNumSegments: 2,
				Flush:          true,
			})
			time.Sleep(10 * time.Millisecond)
		}
	}()

	wg.Wait()

	// Final flush + force-merge to 1 segment per shard (total expected live docs = 6 seeds + 16 worker docs = 22).
	if _, err := coordNode.ForceMerge(ctx, []string{"catalog"}, cluster.ForceMergeOptions{
		MaxNumSegments: 1,
		Flush:          true,
	}); err != nil {
		t.Fatalf("final ForceMerge: %v", err)
	}

	// Verify exact live count on data-1.
	dataRes, err := coordNode.SearchWithPreference(ctx, []string{"catalog"}, query.SearchRequest{
		Query: map[string]any{"match_all": map[string]any{}},
		Size:  func() *int { v := 50; return &v }(),
	}, cluster.RetrievalDropIn, false)
	if err != nil {
		t.Fatalf("final data search: %v", err)
	}
	if got := dataRes["hits"].(map[string]any)["total"].(map[string]any)["value"].(int); got != 22 {
		t.Fatalf("expected 22 live docs on data node after concurrent writes+merges, got %d", got)
	}

	// Verify stateless reader-1 cleanly synced the compacted 1-segment manifest and returns exact same 22 docs!
	readerRes, err := coordNode.SearchWithPreference(ctx, []string{"catalog"}, query.SearchRequest{
		Query: map[string]any{"match_all": map[string]any{}},
		Size:  func() *int { v := 50; return &v }(),
	}, cluster.RetrievalDropIn, true)
	if err != nil {
		t.Fatalf("final reader search: %v", err)
	}
	if got := readerRes["hits"].(map[string]any)["total"].(map[string]any)["value"].(int); got != 22 {
		t.Fatalf("expected 22 live docs on stateless reader after compacted manifest sync, got %d", got)
	}
	for _, sh := range readerNode.Stats().Shards {
		if sh.SegmentCount > 1 {
			t.Fatalf("expected reader shard %d to sync compacted manifest (<=1 segment), got %d", sh.ShardID, sh.SegmentCount)
		}
	}
}
