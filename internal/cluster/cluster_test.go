package cluster_test

import (
	"bytes"
	"context"
	"encoding/json"
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

func TestThreeNodeClusterESCompatAsyncRepZeroHopAndBitRotHeal(t *testing.T) {
	s3Srv := s3store.NewMockServer("ak-demo", "sk-demo", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-test",
		AccessKey: "ak-demo",
		SecretKey: "sk-demo",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	// Start 3 HTTP test servers representing 3 cluster nodes.
	nodes := make([]*cluster.Node, 3)
	httpSrvs := make([]*httptest.Server, 3)
	peerInfos := make([]sharding.NodeInfo, 3)

	for i := 0; i < 3; i++ {
		nodeID := "node-" + string(rune('1'+i))
		n, err := cluster.NewNode(cluster.NodeConfig{
			NodeID:            nodeID,
			NVMeDir:           t.TempDir(),
			NVMeMaxBytes:      64 * 1024 * 1024,
			FlushInterval:     10 * time.Second,
			ReplicateInterval: 15 * time.Millisecond,
			ScrubInterval:     10 * time.Second,
			S3:                s3Client,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", nodeID, err)
		}
		nodes[i] = n
		httpSrvs[i] = httptest.NewServer(api.NewHandler(n))
		peerInfos[i] = sharding.NodeInfo{
			ID:      nodeID,
			Address: strings.TrimPrefix(httpSrvs[i].URL, "http://"),
		}
	}
	defer func() {
		for i := 0; i < 3; i++ {
			_ = nodes[i].Close()
			httpSrvs[i].Close()
		}
	}()

	for _, n := range nodes {
		n.UpdatePeers(peerInfos)
	}

	// 1. Create ES index with 3 shards and 1 replica via node-1.
	createPayload := `{"settings":{"number_of_shards":3,"number_of_replicas":1}}`
	req, _ := http.NewRequest(http.MethodPut, httpSrvs[0].URL+"/logs", strings.NewReader(createPayload))
	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("create index failed: err=%v resp=%v", err, resp)
	}
	_ = resp.Body.Close()

	// 2. Bulk NDJSON ingest via node-2 (cross-node shard routing!).
	bulkBody := strings.Join([]string{
		`{"index":{"_id":"evt-1"}}`,
		`{"service":"checkout","level":"error","latency_ms":412,"message":"s3 range timeout on shard 2"}`,
		`{"index":{"_id":"evt-2"}}`,
		`{"service":"checkout","level":"warn","latency_ms":140,"message":"nvme write queue spike"}`,
		`{"index":{"_id":"evt-3"}}`,
		`{"service":"search","level":"info","latency_ms":8,"message":"nvme cache hit for inverted index"}`,
		"",
	}, "\n")
	bulkResp, err := http.Post(httpSrvs[1].URL+"/logs/_bulk", "application/x-ndjson", strings.NewReader(bulkBody))
	if err != nil || bulkResp.StatusCode != http.StatusOK {
		t.Fatalf("bulk ingest failed: err=%v resp=%v", err, bulkResp)
	}
	_ = bulkResp.Body.Close()

	// 3. Verify asynchronous WAL replication propagates to replicas quickly.
	deadline := time.Now().Add(2 * time.Second)
	var repBatches int64
	for time.Now().Before(deadline) {
		repBatches = 0
		for _, n := range nodes {
			repBatches += n.Stats().ReplicationBatchesSent
		}
		if repBatches >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if repBatches == 0 {
		t.Fatalf("expected async replication batches > 0")
	}

	// 4. Drop-in Elasticsearch Query DSL search + aggregations before S3 flush (NRT WAL search).
	searchBody := `{
		"query": {
			"bool": {
				"must": [{ "match_phrase": { "message": "range timeout" } }],
				"filter": [{ "range": { "latency_ms": { "gte": 100 } } }]
			}
		},
		"aggs": {
			"by_service": { "terms": { "field": "service" } },
			"latency_stats": { "stats": { "field": "latency_ms" } }
		}
	}`
	sResp, err := http.Post(httpSrvs[2].URL+"/logs/_search", "application/json", strings.NewReader(searchBody))
	if err != nil {
		t.Fatalf("search request: %v", err)
	}
	var esOut map[string]any
	_ = json.NewDecoder(sResp.Body).Decode(&esOut)
	_ = sResp.Body.Close()

	hitsObj := esOut["hits"].(map[string]any)
	totalObj := hitsObj["total"].(map[string]any)
	if int(totalObj["value"].(float64)) != 1 {
		t.Fatalf("expected 1 match_phrase+range hit, got %+v", esOut)
	}

	// 5. Flush buffered WAL segments to S3 and verify zero-hop Presigned S3 Range retrieval!
	fResp, err := http.Post(httpSrvs[0].URL+"/logs/_refresh?flush=true", "application/json", nil)
	if err != nil || fResp.StatusCode != http.StatusOK {
		t.Fatalf("flush refresh failed: err=%v resp=%v", err, fResp)
	}
	_ = fResp.Body.Close()

	zeroHopQuery := `{"query":{"term":{"_id":"evt-3"}}}`
	zhResp, err := http.Post(httpSrvs[0].URL+"/logs/_search?retrieval=presigned", "application/json", strings.NewReader(zeroHopQuery))
	if err != nil {
		t.Fatalf("zero-hop search: %v", err)
	}
	var zhOut map[string]any
	_ = json.NewDecoder(zhResp.Body).Decode(&zhOut)
	_ = zhResp.Body.Close()

	zhHits := zhOut["hits"].(map[string]any)["hits"].([]any)
	if len(zhHits) != 1 {
		t.Fatalf("expected 1 zero-hop hit, got %d", len(zhHits))
	}
	hit0 := zhHits[0].(map[string]any)
	if _, hasInline := hit0["_source"]; hasInline {
		t.Fatalf("expected _source omitted in presigned zero-hop mode")
	}
	ptr := hit0["_s3_pointer"].(map[string]any)
	presignedURL := ptr["url"].(string)
	byteRange := ptr["range"].(string)
	expectedCRC := ptr["crc32c"].(string)

	// Direct client -> S3 fetch bypassing all CinderSearch nodes!
	directReq, _ := http.NewRequest(http.MethodGet, presignedURL, nil)
	directReq.Header.Set("Range", byteRange)
	directResp, err := http.DefaultClient.Do(directReq)
	if err != nil {
		t.Fatalf("direct s3 range fetch: %v", err)
	}
	rawDoc, _ := io.ReadAll(directResp.Body)
	_ = directResp.Body.Close()
	if directResp.StatusCode != http.StatusPartialContent {
		t.Fatalf("expected 206 Partial Content from direct S3 fetch, got %d", directResp.StatusCode)
	}
	if checksum.FormatHex(checksum.Sum32C(rawDoc)) != expectedCRC {
		t.Fatalf("direct S3 doc CRC32C mismatch: got %s want %s", checksum.FormatHex(checksum.Sum32C(rawDoc)), expectedCRC)
	}
	if !bytes.Contains(rawDoc, []byte("nvme cache hit")) {
		t.Fatalf("unexpected direct S3 document body: %s", string(rawDoc))
	}

	// 6. Corrupt a cached segment index file on node-1's NVMe SSD and verify auto-heal from S3!
	s3Key := strings.Replace(ptr["key"].(string), ".docs", ".idx", 1)
	corruptedAtLeastOne := false
	for _, n := range nodes {
		if err := n.NVMeCache().CorruptFileForTest(s3Key); err == nil {
			corruptedAtLeastOne = true
		}
	}
	if !corruptedAtLeastOne {
		t.Fatalf("expected at least one node to cache %s on NVMe", s3Key)
	}

	// Query again: corrupted NVMe cache entry is detected via CRC32C, purged, and healed from S3!
	healResp, err := http.Post(httpSrvs[0].URL+"/logs/_search", "application/json", strings.NewReader(zeroHopQuery))
	if err != nil {
		t.Fatalf("post-bitrot search: %v", err)
	}
	var healOut map[string]any
	_ = json.NewDecoder(healResp.Body).Decode(&healOut)
	_ = healResp.Body.Close()
	if int(healOut["hits"].(map[string]any)["total"].(map[string]any)["value"].(float64)) != 1 {
		t.Fatalf("expected query to succeed after NVMe bit-rot auto-heal, got %+v", healOut)
	}

	var totalHealed int64
	for _, n := range nodes {
		totalHealed += n.NVMeCache().Stats().BitRotHealed
	}
	if totalHealed < 1 {
		t.Fatalf("expected BitRotHealed >= 1 after NVMe corruption, got %d", totalHealed)
	}
}

func TestShardVersioningDeletesAndWALRecovery(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-rec", "sk-rec", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-recovery",
		AccessKey: "ak-rec",
		SecretKey: "sk-rec",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	nvmeDir := t.TempDir()
	nvmeCache, err := cache.NewNVMeCache(t.TempDir(), 32*1024*1024)
	if err != nil {
		t.Fatalf("new nvme cache: %v", err)
	}

	sh, err := cluster.OpenShard(ctx, "orders", 0, true, nvmeDir, nvmeCache, s3Client)
	if err != nil {
		t.Fatalf("open shard: %v", err)
	}

	// 1. Index v1 of doc-1 and doc-keep, then flush seg-000001 to S3.
	if _, err := sh.IndexLocal("doc-1", []byte(`{"status":"open","step":1}`)); err != nil {
		t.Fatalf("index v1: %v", err)
	}
	if _, err := sh.IndexLocal("doc-keep", []byte(`{"status":"archived","step":10}`)); err != nil {
		t.Fatalf("index doc-keep: %v", err)
	}
	if ref, err := sh.FlushToS3(ctx); err != nil || ref == nil {
		t.Fatalf("flush seg-1: ref=%v err=%v", ref, err)
	}

	// 2. Update doc-1 to v2 in WAL (not flushed yet); verify NRT segment suppresses v1.
	if _, err := sh.IndexLocal("doc-1", []byte(`{"status":"closed","step":2}`)); err != nil {
		t.Fatalf("index v2: %v", err)
	}
	segs, latestSeq, err := sh.ActiveSegments(ctx)
	if err != nil {
		t.Fatalf("active segments: %v", err)
	}
	openHits := 0
	for _, seg := range segs {
		hits, err := query.EvaluateSegment(seg, map[string]any{"term": map[string]any{"status": "open"}})
		if err != nil {
			t.Fatalf("eval open: %v", err)
		}
		for _, h := range hits {
			if seg.Docs[h.DocIdx].Seq >= latestSeq[h.DocID] {
				openHits++
			}
		}
	}
	if openHits != 0 {
		t.Fatalf("expected stale v1 status=open to be suppressed, got %d hits", openHits)
	}

	// 3. Delete doc-1, flush tombstone segment seg-000002, then write doc-wal (unflushed).
	if _, err := sh.DeleteLocal("doc-1"); err != nil {
		t.Fatalf("delete doc-1: %v", err)
	}
	if _, err := sh.FlushToS3(ctx); err != nil {
		t.Fatalf("flush tombstone seg-2: %v", err)
	}
	if _, err := sh.IndexLocal("doc-wal", []byte(`{"status":"pending","step":3}`)); err != nil {
		t.Fatalf("index doc-wal: %v", err)
	}
	_ = sh.Close()

	// 4. Crash recovery simulation: open shard against a fresh NVMeCache (cold cache) + existing NVMe WAL + S3 manifest.
	freshCache, err := cache.NewNVMeCache(t.TempDir(), 32*1024*1024)
	if err != nil {
		t.Fatalf("new fresh cache: %v", err)
	}
	reopened, err := cluster.OpenShard(ctx, "orders", 0, true, nvmeDir, freshCache, s3Client)
	if err != nil {
		t.Fatalf("reopen shard: %v", err)
	}
	defer reopened.Close()

	// doc-1 must remain deleted via S3 tombstone seg-2.
	if _, found, _, _, _, err := reopened.GetDocByID(ctx, "doc-1"); err != nil || found {
		t.Fatalf("expected doc-1 deleted after recovery, found=%v err=%v", found, err)
	}
	// doc-keep must hydrate from S3 seg-1 via fresh NVMe cache.
	rawKeep, found, _, _, _, err := reopened.GetDocByID(ctx, "doc-keep")
	if err != nil || !found || !bytes.Contains(rawKeep, []byte("archived")) {
		t.Fatalf("expected doc-keep recovered from S3, found=%v err=%v raw=%s", found, err, string(rawKeep))
	}
	// doc-wal must replay from NVMe WAL.
	rawWAL, found, _, _, _, err := reopened.GetDocByID(ctx, "doc-wal")
	if err != nil || !found || !bytes.Contains(rawWAL, []byte("pending")) {
		t.Fatalf("expected doc-wal replayed from WAL, found=%v err=%v raw=%s", found, err, string(rawWAL))
	}
}

func TestClusterReplicaFailoverOnPrimaryOutage(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-fo", "sk-fo", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-failover",
		AccessKey: "ak-fo",
		SecretKey: "sk-fo",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	nodes := make([]*cluster.Node, 3)
	httpSrvs := make([]*httptest.Server, 3)
	peers := make([]sharding.NodeInfo, 3)
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("node-%d", i+1)
		n, err := cluster.NewNode(cluster.NodeConfig{
			NodeID:            id,
			NVMeDir:           t.TempDir(),
			NVMeMaxBytes:      32 * 1024 * 1024,
			FlushInterval:     30 * time.Second,
			ReplicateInterval: 10 * time.Millisecond,
			ScrubInterval:     30 * time.Second,
			S3:                s3Client,
		})
		if err != nil {
			t.Fatalf("new node: %v", err)
		}
		nodes[i] = n
		httpSrvs[i] = httptest.NewServer(api.NewHandler(n))
		peers[i] = sharding.NodeInfo{
			ID:      id,
			Address: strings.TrimPrefix(httpSrvs[i].URL, "http://"),
		}
	}
	for _, n := range nodes {
		n.UpdatePeers(peers)
	}

	if err := nodes[0].CreateIndex(ctx, cluster.IndexMetadata{
		Name:     "events",
		Settings: cluster.IndexSettings{NumberOfShards: 3, NumberOfReplicas: 1},
	}, true); err != nil {
		t.Fatalf("create index: %v", err)
	}

	for i := 0; i < 12; i++ {
		docID := fmt.Sprintf("ev-%02d", i)
		payload := fmt.Sprintf(`{"seq":%d,"msg":"failover check %d"}`, i, i)
		if _, _, _, _, err := nodes[0].IndexDoc(ctx, "events", docID, []byte(payload), ""); err != nil {
			t.Fatalf("index %s: %v", docID, err)
		}
	}

	// Wait until async WAL replication delivers all 12 records across replicas.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var batches int64
		for _, n := range nodes {
			batches += n.Stats().ReplicationBatchesSent
		}
		if batches >= 3 {
			// Verify replica searches see all 12 docs after refresh.
			_ = nodes[0].RefreshIndex(ctx, "events", false, true)
			res, err := nodes[1].Search(ctx, []string{"events"}, query.SearchRequest{
				Query: map[string]any{"match_all": map[string]any{}},
			}, cluster.RetrievalDropIn)
			if err == nil {
				total := int(res["hits"].(map[string]any)["total"].(map[string]any)["value"].(int))
				if total == 12 {
					break
				}
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Terminate node-1 (primary for at least one shard) and close its HTTP listener.
	httpSrvs[0].Close()
	_ = nodes[0].Close()
	defer func() {
		httpSrvs[1].Close()
		httpSrvs[2].Close()
		_ = nodes[1].Close()
		_ = nodes[2].Close()
	}()

	// Search from surviving node-2: failed primary shards must transparently fail over to replicas.
	res, err := nodes[1].Search(ctx, []string{"events"}, query.SearchRequest{
		Query: map[string]any{"match_all": map[string]any{}},
	}, cluster.RetrievalDropIn)
	if err != nil {
		t.Fatalf("search after primary outage failed: %v", err)
	}
	total := res["hits"].(map[string]any)["total"].(map[string]any)["value"].(int)
	if total != 12 {
		t.Fatalf("expected all 12 hits via replica failover, got %d (shards=%+v)", total, res["_shards"])
	}
}

func TestBackgroundFlushAndNVMeScrubLoops(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-bg", "sk-bg", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-bg",
		AccessKey: "ak-bg",
		SecretKey: "sk-bg",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	n, err := cluster.NewNode(cluster.NodeConfig{
		NodeID:            "node-bg",
		NVMeDir:           t.TempDir(),
		NVMeMaxBytes:      32 * 1024 * 1024,
		FlushInterval:     40 * time.Millisecond,
		ReplicateInterval: 40 * time.Millisecond,
		ScrubInterval:     40 * time.Millisecond,
		S3:                s3Client,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	defer n.Close()

	if err := n.CreateIndex(ctx, cluster.IndexMetadata{
		Name:     "bgidx",
		Settings: cluster.IndexSettings{NumberOfShards: 1, NumberOfReplicas: 0},
	}, false); err != nil {
		t.Fatalf("create index: %v", err)
	}

	if _, _, _, _, err := n.IndexDoc(ctx, "bgidx", "d-1", []byte(`{"service":"worker","ok":true}`), ""); err != nil {
		t.Fatalf("index doc: %v", err)
	}

	// Wait for background flushLoop to persist to S3 automatically.
	var idxKey string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		st := n.Stats()
		if st.S3SegmentFlushes >= 1 && len(st.Shards) == 1 && st.Shards[0].SegmentCount >= 1 {
			idxKey = st.Shards[0].Segments[0].IdxKey
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if idxKey == "" {
		t.Fatalf("background flushLoop did not flush segment to S3 in time: %+v", n.Stats())
	}

	// Corrupt cached segment index on NVMe and verify background scrubLoop purges it without a query.
	if err := n.NVMeCache().CorruptFileForTest(idxKey); err != nil {
		t.Fatalf("corrupt cached idx: %v", err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n.NVMeCache().Stats().BitRotHealed >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if n.NVMeCache().Stats().BitRotHealed < 1 {
		t.Fatalf("background scrubLoop did not detect NVMe bit-rot in time: %+v", n.NVMeCache().Stats())
	}
}

func TestConcurrentIngestFlushAndSearchRaceSafe(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-race", "sk-race", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-race",
		AccessKey: "ak-race",
		SecretKey: "sk-race",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	n, err := cluster.NewNode(cluster.NodeConfig{
		NodeID:            "node-race",
		NVMeDir:           t.TempDir(),
		NVMeMaxBytes:      32 * 1024 * 1024,
		FlushInterval:     25 * time.Millisecond,
		ReplicateInterval: 25 * time.Millisecond,
		ScrubInterval:     30 * time.Millisecond,
		S3:                s3Client,
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	defer n.Close()

	if err := n.CreateIndex(ctx, cluster.IndexMetadata{
		Name:     "racetest",
		Settings: cluster.IndexSettings{NumberOfShards: 3, NumberOfReplicas: 0},
	}, false); err != nil {
		t.Fatalf("create index: %v", err)
	}

	var wg sync.WaitGroup
	// 4 concurrent writers
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 15; i++ {
				id := fmt.Sprintf("w%d-%d", worker, i)
				body := fmt.Sprintf(`{"worker":%d,"seq":%d,"message":"concurrent write test"}`, worker, i)
				if _, _, _, _, err := n.IndexDoc(ctx, "racetest", id, []byte(body), ""); err != nil {
					t.Errorf("concurrent index %s: %v", id, err)
				}
			}
		}(w)
	}
	// 2 concurrent searchers + flushers
	for s := 0; s < 2; s++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				_, err := n.Search(ctx, []string{"racetest"}, query.SearchRequest{
					Query: map[string]any{"match": map[string]any{"message": "concurrent"}},
				}, cluster.RetrievalHybrid)
				if err != nil {
					t.Errorf("concurrent search: %v", err)
				}
				_ = n.RefreshIndex(ctx, "racetest", i%3 == 0, false)
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	wg.Wait()

	if err := n.RefreshIndex(ctx, "racetest", true, false); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	res, err := n.Search(ctx, []string{"racetest"}, query.SearchRequest{
		Query: map[string]any{"match_all": map[string]any{}},
		Size:  func() *int { v := 100; return &v }(),
	}, cluster.RetrievalDropIn)
	if err != nil {
		t.Fatalf("final search: %v", err)
	}
	if got := res["hits"].(map[string]any)["total"].(map[string]any)["value"].(int); got != 60 {
		t.Fatalf("expected 60 total docs after concurrent ingest, got %d", got)
	}
}

func TestCoordinatorAndReaderNodeRolesAndPreferenceRouting(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-roles", "sk-roles", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-roles",
		AccessKey: "ak-roles",
		SecretKey: "sk-roles",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	specs := []struct {
		id    string
		roles []string
	}{
		{id: "data-1", roles: []string{sharding.RoleData}},
		{id: "data-2", roles: []string{sharding.RoleData}},
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
			FlushInterval:     30 * time.Second,
			ReplicateInterval: 20 * time.Millisecond,
			ScrubInterval:     30 * time.Second,
			S3:                s3Client,
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

	data1, data2, coord, reader := nodes[0], nodes[1], nodes[2], nodes[3]
	coordURL := httpSrvs[2].URL
	readerURL := httpSrvs[3].URL

	// 1. Create index via stateless coordinator node (2 shards, 1 replica -> hosted only on data-1 and data-2).
	createReq, _ := http.NewRequest(http.MethodPut, coordURL+"/telemetry", strings.NewReader(`{"settings":{"number_of_shards":2,"number_of_replicas":1}}`))
	createResp, err := http.DefaultClient.Do(createReq)
	if err != nil || createResp.StatusCode != http.StatusOK {
		t.Fatalf("create index via coordinator failed: err=%v resp=%v", err, createResp)
	}
	_ = createResp.Body.Close()

	// Verify coordinator holds 0 shards, reader holds 2 read-only shards with 0 WAL ops,
	// and all primaries/replicas belong exclusively to data nodes.
	if st := coord.Stats(); len(st.Shards) != 0 {
		t.Fatalf("expected coordinator to own 0 shards, got %+v", st.Shards)
	}
	rStats := reader.Stats()
	if len(rStats.Shards) != 2 {
		t.Fatalf("expected reader to open 2 read-only S3 shard views, got %+v", rStats.Shards)
	}
	for _, sh := range rStats.Shards {
		if !sh.ReadOnly || sh.IsPrimary || sh.WALLength != 0 {
			t.Fatalf("expected read-only stateless shard on reader, got %+v", sh)
		}
	}
	if len(data1.Stats().Shards) != 2 || len(data2.Stats().Shards) != 2 {
		t.Fatalf("expected data-1 and data-2 to hold 2 shards each, got %d and %d",
			len(data1.Stats().Shards), len(data2.Stats().Shards))
	}

	// 2. Bulk ingest via coordinator (routes writes to data-1 / data-2 primaries; zero WAL writes on coord/reader).
	bulkBody := strings.Join([]string{
		`{"index":{"_id":"t-1"}}`,
		`{"host":"api-1","region":"us-east","latency_ms":14,"msg":"role routing healthy"}`,
		`{"index":{"_id":"t-2"}}`,
		`{"host":"api-2","region":"us-west","latency_ms":95,"msg":"role routing slow"}`,
		"",
	}, "\n")
	bResp, err := http.Post(coordURL+"/telemetry/_bulk", "application/x-ndjson", strings.NewReader(bulkBody))
	if err != nil || bResp.StatusCode != http.StatusOK {
		t.Fatalf("bulk via coordinator failed: err=%v resp=%v", err, bResp)
	}
	_ = bResp.Body.Close()

	// 3. Before flush: GET /{index}/_doc/{id} on reader must forward to data primary and return NRT doc;
	// default search on coordinator must find both NRT WAL docs.
	gResp, err := http.Get(readerURL + "/telemetry/_doc/t-1")
	if err != nil || gResp.StatusCode != http.StatusOK {
		t.Fatalf("NRT get via reader failed: err=%v resp=%v", err, gResp)
	}
	var gotDoc map[string]any
	_ = json.NewDecoder(gResp.Body).Decode(&gotDoc)
	_ = gResp.Body.Close()
	if gotDoc["found"] != true {
		t.Fatalf("expected NRT doc found via reader primary forward, got %+v", gotDoc)
	}

	sResp, err := http.Post(coordURL+"/telemetry/_search", "application/json", strings.NewReader(`{"query":{"match":{"msg":"routing"}}}`))
	if err != nil {
		t.Fatalf("NRT search via coordinator: %v", err)
	}
	var nrtOut map[string]any
	_ = json.NewDecoder(sResp.Body).Decode(&nrtOut)
	_ = sResp.Body.Close()
	if int(nrtOut["hits"].(map[string]any)["total"].(map[string]any)["value"].(float64)) != 2 {
		t.Fatalf("expected 2 NRT hits via coordinator, got %+v", nrtOut)
	}

	// 4. Flush segments from data nodes to S3 via coordinator, then query with ?preference=reader
	// and verify the stateless reader hydrates from S3 into its own NVMe cache.
	fResp, err := http.Post(coordURL+"/telemetry/_refresh?flush=true", "application/json", nil)
	if err != nil || fResp.StatusCode != http.StatusOK {
		t.Fatalf("flush via coordinator failed: err=%v resp=%v", err, fResp)
	}
	_ = fResp.Body.Close()

	if reader.NVMeCache().Stats().Entries != 0 {
		t.Fatalf("expected reader NVMe cache empty before reader-routed search, got %+v", reader.NVMeCache().Stats())
	}

	rSearchResp, err := http.Post(coordURL+"/telemetry/_search?preference=reader", "application/json", strings.NewReader(`{"query":{"match":{"msg":"routing"}}}`))
	if err != nil || rSearchResp.StatusCode != http.StatusOK {
		t.Fatalf("preference=reader search failed: err=%v resp=%v", err, rSearchResp)
	}
	var readerOut map[string]any
	_ = json.NewDecoder(rSearchResp.Body).Decode(&readerOut)
	_ = rSearchResp.Body.Close()
	if int(readerOut["hits"].(map[string]any)["total"].(map[string]any)["value"].(float64)) != 2 {
		t.Fatalf("expected 2 hits via preference=reader, got %+v", readerOut)
	}
	if reader.NVMeCache().Stats().Entries == 0 {
		t.Fatalf("expected preference=reader search to route to reader and populate reader NVMe cache")
	}

	// Verify reader also honors X-Cinder-Preference header + direct SearchWithPreference.
	prefRes, err := coord.SearchWithPreference(ctx, []string{"telemetry"}, query.SearchRequest{
		Query: map[string]any{"term": map[string]any{"host": "api-1"}},
	}, cluster.RetrievalDropIn, true)
	if err != nil {
		t.Fatalf("SearchWithPreference(preferReader=true): %v", err)
	}
	if prefRes["hits"].(map[string]any)["total"].(map[string]any)["value"].(int) != 1 {
		t.Fatalf("expected 1 hit for host=api-1 via reader preference, got %+v", prefRes)
	}
}
