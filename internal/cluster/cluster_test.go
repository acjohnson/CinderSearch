package cluster_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cindersearch/cindersearch/internal/api"
	"github.com/cindersearch/cindersearch/internal/checksum"
	"github.com/cindersearch/cindersearch/internal/cluster"
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
	_ = context.Background()
}
