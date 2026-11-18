package api_test

import (
	"bytes"
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

func newTestCluster(t *testing.T, nodeCount int, nvmeMaxBytes int64) ([]*cluster.Node, []*httptest.Server, *s3store.MockServer, *s3store.Client) {
	t.Helper()

	s3Srv := s3store.NewMockServer("ak-api", "sk-api", "us-east-1")
	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-api-test",
		AccessKey: "ak-api",
		SecretKey: "sk-api",
	})
	if err != nil {
		s3Srv.Close()
		t.Fatalf("new s3 client: %v", err)
	}

	nodes := make([]*cluster.Node, nodeCount)
	httpSrvs := make([]*httptest.Server, nodeCount)
	peers := make([]sharding.NodeInfo, nodeCount)

	for i := 0; i < nodeCount; i++ {
		nodeID := "node-" + string(rune('1'+i))
		n, err := cluster.NewNode(cluster.NodeConfig{
			NodeID:            nodeID,
			NVMeDir:           t.TempDir(),
			NVMeMaxBytes:      nvmeMaxBytes,
			FlushInterval:     30 * time.Second,
			ReplicateInterval: 15 * time.Millisecond,
			ScrubInterval:     30 * time.Second,
			S3:                s3Client,
		})
		if err != nil {
			t.Fatalf("new node %s: %v", nodeID, err)
		}
		nodes[i] = n
		httpSrvs[i] = httptest.NewServer(api.NewHandler(n))
		peers[i] = sharding.NodeInfo{
			ID:      nodeID,
			Address: strings.TrimPrefix(httpSrvs[i].URL, "http://"),
		}
	}

	for _, n := range nodes {
		n.UpdatePeers(peers)
	}

	t.Cleanup(func() {
		for i := 0; i < nodeCount; i++ {
			_ = nodes[i].Close()
			httpSrvs[i].Close()
		}
		s3Srv.Close()
	})

	return nodes, httpSrvs, s3Srv, s3Client
}

func doJSONReq(t *testing.T, method, url, body string, headers map[string]string) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		t.Fatalf("new request %s %s: %v", method, url, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request %s %s: %v", method, url, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body %s %s: %v", method, url, err)
	}
	var out map[string]any
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("unmarshal response from %s %s (%d): %v (raw=%s)", method, url, resp.StatusCode, err, string(raw))
		}
	}
	return resp.StatusCode, out
}

func TestESClusterInfoHealthCatAndIndexLifecycle(t *testing.T) {
	_, srvs, _, _ := newTestCluster(t, 2, 16*1024*1024)

	// 1. Root cluster banner
	code, root := doJSONReq(t, http.MethodGet, srvs[0].URL+"/", "", nil)
	if code != http.StatusOK || root["cluster_name"] != "cindersearch-cluster" {
		t.Fatalf("unexpected root banner: code=%d body=%+v", code, root)
	}
	ver := root["version"].(map[string]any)
	if ver["number"] != "8.11.0" || ver["engine"] != "cindersearch-go" {
		t.Fatalf("unexpected version object: %+v", ver)
	}

	// 2. Create index on node-1 with explicit shards/replicas & mappings
	code, created := doJSONReq(t, http.MethodPut, srvs[0].URL+"/events", `{
		"settings": {"number_of_shards": 2, "number_of_replicas": 1},
		"mappings": {"properties": {"service": {"type": "keyword"}}}
	}`, nil)
	if code != http.StatusOK || created["acknowledged"] != true {
		t.Fatalf("create index failed: code=%d body=%+v", code, created)
	}

	// 3. Verify GET /{index} works from peer node-2 (broadcast sync)
	code, gotIdx := doJSONReq(t, http.MethodGet, srvs[1].URL+"/events", "", nil)
	if code != http.StatusOK || gotIdx["events"] == nil {
		t.Fatalf("expected synced index metadata on node-2: code=%d body=%+v", code, gotIdx)
	}

	// 4. Cluster health & _cat endpoints
	code, health := doJSONReq(t, http.MethodGet, srvs[0].URL+"/_cluster/health", "", nil)
	if code != http.StatusOK || health["status"] != "green" || int(health["number_of_nodes"].(float64)) != 2 {
		t.Fatalf("unexpected cluster health: code=%d body=%+v", code, health)
	}

	resp, err := http.Get(srvs[0].URL + "/_cat/indices")
	if err != nil {
		t.Fatalf("get _cat/indices: %v", err)
	}
	var catIndices []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&catIndices)
	_ = resp.Body.Close()
	if len(catIndices) != 1 || catIndices[0]["index"] != "events" {
		t.Fatalf("unexpected _cat/indices: %+v", catIndices)
	}

	resp, err = http.Get(srvs[0].URL + "/_cat/shards")
	if err != nil {
		t.Fatalf("get _cat/shards: %v", err)
	}
	var catShards []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&catShards)
	_ = resp.Body.Close()
	if len(catShards) != 4 { // 2 shards * (1 primary + 1 replica)
		t.Fatalf("expected 4 shard placements, got %d: %+v", len(catShards), catShards)
	}

	// 5. Stats endpoint
	code, stats := doJSONReq(t, http.MethodGet, srvs[0].URL+"/_cinder/stats", "", nil)
	if code != http.StatusOK || stats["nvme_cache"] == nil {
		t.Fatalf("unexpected _cinder/stats: code=%d body=%+v", code, stats)
	}

	// 6. Delete index and verify 404 across peers
	code, del := doJSONReq(t, http.MethodDelete, srvs[0].URL+"/events", "", nil)
	if code != http.StatusOK || del["acknowledged"] != true {
		t.Fatalf("delete index failed: code=%d body=%+v", code, del)
	}
	code, _ = doJSONReq(t, http.MethodGet, srvs[1].URL+"/events", "", nil)
	if code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete on peer node, got %d", code)
	}
}

func TestESDocCRUDUpdateBulkAndCrossNodeCRC(t *testing.T) {
	// 3 nodes, 0 replicas so coordinator forwarding on GET is guaranteed on non-owner nodes.
	_, srvs, _, _ := newTestCluster(t, 3, 16*1024*1024)

	code, _ := doJSONReq(t, http.MethodPut, srvs[0].URL+"/orders", `{"settings":{"number_of_shards":3,"number_of_replicas":0}}`, nil)
	if code != http.StatusOK {
		t.Fatalf("create orders index failed: %d", code)
	}

	// 1. POST /orders/_doc (auto-generated ID -> 201 Created)
	code, autoRes := doJSONReq(t, http.MethodPost, srvs[0].URL+"/orders/_doc", `{"customer":"alice","total":42.5}`, nil)
	if code != http.StatusCreated || autoRes["result"] != "created" || autoRes["_id"] == "" {
		t.Fatalf("auto-id index failed: code=%d body=%+v", code, autoRes)
	}
	autoID := autoRes["_id"].(string)

	// 2. PUT /orders/_doc/ord-100 on node-1, GET from all 3 nodes verifying intact _crc32c
	docPayload := `{"customer":"bob","status":"pending","total":199.0}`
	expectedCRC := checksum.FormatHex(checksum.Sum32C([]byte(docPayload)))
	code, putRes := doJSONReq(t, http.MethodPut, srvs[0].URL+"/orders/_doc/ord-100", docPayload, nil)
	if code != http.StatusOK || putRes["_id"] != "ord-100" {
		t.Fatalf("put doc failed: code=%d body=%+v", code, putRes)
	}

	for i, srv := range srvs {
		code, got := doJSONReq(t, http.MethodGet, srv.URL+"/orders/_doc/ord-100", "", nil)
		if code != http.StatusOK || got["found"] != true {
			t.Fatalf("node %d get doc failed: code=%d body=%+v", i, code, got)
		}
		if got["_crc32c"] != expectedCRC {
			t.Fatalf("node %d _crc32c mismatch: got %v want %s", i, got["_crc32c"], expectedCRC)
		}
	}

	// 3. Partial update via POST /orders/_update/ord-100
	code, updRes := doJSONReq(t, http.MethodPost, srvs[1].URL+"/orders/_update/ord-100", `{"doc":{"status":"shipped","carrier":"ups"}}`, nil)
	if code != http.StatusOK || updRes["result"] != "updated" {
		t.Fatalf("partial update failed: code=%d body=%+v", code, updRes)
	}
	code, afterUpd := doJSONReq(t, http.MethodGet, srvs[2].URL+"/orders/_doc/ord-100", "", nil)
	if code != http.StatusOK {
		t.Fatalf("get after update failed: %d", code)
	}
	src := afterUpd["_source"].(map[string]any)
	if src["customer"] != "bob" || src["status"] != "shipped" || src["carrier"] != "ups" {
		t.Fatalf("partial update merge failed: %+v", src)
	}

	// 4. Bulk mixed ops across auto-created index "metrics" and "orders"
	bulkNDJSON := strings.Join([]string{
		`{"create":{"_index":"metrics","_id":"m-1"}}`,
		`{"host":"web-1","cpu":0.75}`,
		`{"update":{"_index":"orders","_id":"ord-100"}}`,
		`{"doc":{"priority":"high"}}`,
		`{"delete":{"_index":"orders","_id":"` + autoID + `"}}`,
		"",
	}, "\n")
	code, bulkRes := doJSONReq(t, http.MethodPost, srvs[2].URL+"/_bulk", bulkNDJSON, nil)
	if code != http.StatusOK || bulkRes["errors"] != false {
		t.Fatalf("bulk failed: code=%d body=%+v", code, bulkRes)
	}
	if len(bulkRes["items"].([]any)) != 3 {
		t.Fatalf("expected 3 bulk items, got %+v", bulkRes["items"])
	}

	// Verify deleted doc returns 404 found=false
	code, missing := doJSONReq(t, http.MethodGet, srvs[0].URL+"/orders/_doc/"+autoID, "", nil)
	if code != http.StatusNotFound || missing["found"] != false {
		t.Fatalf("expected 404 found=false after bulk delete, got code=%d body=%+v", code, missing)
	}

	// Verify bulk update merged into ord-100
	code, afterBulkUpd := doJSONReq(t, http.MethodGet, srvs[0].URL+"/orders/_doc/ord-100", "", nil)
	if code != http.StatusOK {
		t.Fatalf("get ord-100 after bulk update: %d", code)
	}
	if afterBulkUpd["_source"].(map[string]any)["priority"] != "high" {
		t.Fatalf("expected bulk update field priority=high, got %+v", afterBulkUpd["_source"])
	}

	// 5. Explicit DELETE /{index}/_doc/{id}
	code, delDoc := doJSONReq(t, http.MethodDelete, srvs[1].URL+"/metrics/_doc/m-1", "", nil)
	if code != http.StatusOK || delDoc["result"] != "deleted" {
		t.Fatalf("delete doc failed: code=%d body=%+v", code, delDoc)
	}
	code, _ = doJSONReq(t, http.MethodGet, srvs[0].URL+"/metrics/_doc/m-1", "", nil)
	if code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete doc, got %d", code)
	}
}

func TestESSearchEndpointsAndRetrievalModes(t *testing.T) {
	// Tight NVMe cache budget (512 bytes) to verify hybrid mode behavior when cached vs evicted.
	nodes, srvs, _, _ := newTestCluster(t, 1, 512)

	code, _ := doJSONReq(t, http.MethodPut, srvs[0].URL+"/idx_a", `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`, nil)
	if code != http.StatusOK {
		t.Fatalf("create idx_a: %d", code)
	}
	code, _ = doJSONReq(t, http.MethodPut, srvs[0].URL+"/idx_b", `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`, nil)
	if code != http.StatusOK {
		t.Fatalf("create idx_b: %d", code)
	}

	_, _ = doJSONReq(t, http.MethodPut, srvs[0].URL+"/idx_a/_doc/a1", `{"service":"auth","msg":"token issued quickly","latency":12}`, nil)
	_, _ = doJSONReq(t, http.MethodPut, srvs[0].URL+"/idx_a/_doc/a2", `{"service":"auth","msg":"token expired error","latency":88}`, nil)
	_, _ = doJSONReq(t, http.MethodPut, srvs[0].URL+"/idx_b/_doc/b1", `{"service":"billing","msg":"invoice token verified","latency":45}`, nil)

	// Refresh without flush (NRT) + flush to S3
	code, _ = doJSONReq(t, http.MethodPost, srvs[0].URL+"/idx_a/_refresh?flush=true", "", nil)
	if code != http.StatusOK {
		t.Fatalf("flush idx_a: %d", code)
	}
	code, _ = doJSONReq(t, http.MethodPost, srvs[0].URL+"/idx_b/_refresh?flush=true", "", nil)
	if code != http.StatusOK {
		t.Fatalf("flush idx_b: %d", code)
	}

	// 1. GET /_search?q=...&size=...&from=... across both indices
	code, qGet := doJSONReq(t, http.MethodGet, srvs[0].URL+"/_search?q=token&size=2&from=1", "", nil)
	if code != http.StatusOK {
		t.Fatalf("GET /_search failed: %d body=%+v", code, qGet)
	}
	hitsObj := qGet["hits"].(map[string]any)
	if int(hitsObj["total"].(map[string]any)["value"].(float64)) != 3 {
		t.Fatalf("expected 3 total hits across idx_a and idx_b, got %+v", hitsObj["total"])
	}
	if len(hitsObj["hits"].([]any)) != 2 {
		t.Fatalf("expected page size 2 from GET /_search, got %d", len(hitsObj["hits"].([]any)))
	}

	// 2. Multi-index POST /idx_a,idx_b/_search with X-Cinder-Retrieval: presigned header
	code, hdrPresigned := doJSONReq(t, http.MethodPost, srvs[0].URL+"/idx_a,idx_b/_search", `{"query":{"term":{"_id":"b1"}}}`, map[string]string{
		"X-Cinder-Retrieval": "presigned",
	})
	if code != http.StatusOK {
		t.Fatalf("presigned header search failed: %d", code)
	}
	hit0 := hdrPresigned["hits"].(map[string]any)["hits"].([]any)[0].(map[string]any)
	if _, hasSrc := hit0["_source"]; hasSrc {
		t.Fatalf("expected _source omitted with X-Cinder-Retrieval: presigned")
	}
	if hit0["_s3_pointer"] == nil {
		t.Fatalf("expected _s3_pointer in presigned response")
	}

	// 3. Hybrid mode while hot in NVMe cache: both _s3_pointer (nvme_cached=true) AND _source present
	code, hotHybrid := doJSONReq(t, http.MethodPost, srvs[0].URL+"/idx_a/_search?retrieval=hybrid", `{"query":{"term":{"_id":"a1"}}}`, nil)
	if code != http.StatusOK {
		t.Fatalf("hybrid hot search failed: %d", code)
	}
	hotHit := hotHybrid["hits"].(map[string]any)["hits"].([]any)[0].(map[string]any)
	if hotHit["_source"] == nil || hotHit["_s3_pointer"] == nil {
		t.Fatalf("expected both _source and _s3_pointer in hot hybrid mode: %+v", hotHit)
	}
	if hotHit["_s3_pointer"].(map[string]any)["nvme_cached"] != true {
		t.Fatalf("expected nvme_cached=true on hot hybrid hit: %+v", hotHit["_s3_pointer"])
	}

	// 4. Evict idx_a .docs from NVMe cache by putting > 512 bytes of unrelated data,
	// then verify hybrid mode returns nvme_cached=false and omits _source!
	bigBlob := bytes.Repeat([]byte("z"), 480)
	_ = nodes[0].NVMeCache().Put("evict-1", bigBlob, 0)
	_ = nodes[0].NVMeCache().Put("evict-2", bigBlob, 0)

	code, coldHybrid := doJSONReq(t, http.MethodPost, srvs[0].URL+"/idx_a/_search?retrieval=hybrid", `{"query":{"term":{"_id":"a1"}}}`, nil)
	if code != http.StatusOK {
		t.Fatalf("hybrid cold search failed: %d", code)
	}
	coldHit := coldHybrid["hits"].(map[string]any)["hits"].([]any)[0].(map[string]any)
	if _, hasSrc := coldHit["_source"]; hasSrc {
		t.Fatalf("expected _source omitted in cold hybrid mode when .docs evicted from NVMe")
	}
	if coldHit["_s3_pointer"].(map[string]any)["nvme_cached"] != false {
		t.Fatalf("expected nvme_cached=false after NVMe eviction, got %+v", coldHit["_s3_pointer"])
	}

	// 5. Error handling: missing index and unsupported query DSL clause
	code, errResp := doJSONReq(t, http.MethodPost, srvs[0].URL+"/does_not_exist/_search", `{"query":{"match_all":{}}}`, nil)
	if code != http.StatusNotFound || errResp["error"] == nil {
		t.Fatalf("expected 404 ES error for missing index, got %d body=%+v", code, errResp)
	}
	code, badDSL := doJSONReq(t, http.MethodPost, srvs[0].URL+"/idx_a/_search", `{"query":{"fuzzy_unsupported":{"msg":"tok"}}}`, nil)
	if code != http.StatusBadRequest || badDSL["error"] == nil {
		t.Fatalf("expected 400 ES error for unsupported query clause, got %d body=%+v", code, badDSL)
	}
}
