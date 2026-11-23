package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func newTestCluster(t testing.TB, nodeCount int, nvmeMaxBytes int64) ([]*cluster.Node, []*httptest.Server, *s3store.MockServer, *s3store.Client) {
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

func doJSONReq(t testing.TB, method, url, body string, headers map[string]string) (int, map[string]any) {
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
	rolesArr, ok := root["roles"].([]any)
	if !ok || len(rolesArr) != 1 || rolesArr[0] != sharding.RoleData {
		t.Fatalf("unexpected root roles: %+v", root["roles"])
	}
	ver := root["version"].(map[string]any)
	if ver["number"] != "8.11.0" || ver["engine"] != "cindersearch-go" {
		t.Fatalf("unexpected version object: %+v", ver)
	}

	// Verify pretty-printed JSON by default (2-space indent) and compact with ?pretty=false
	rawPrettyResp, err := http.Get(srvs[0].URL + "/")
	if err != nil {
		t.Fatalf("get pretty /: %v", err)
	}
	rawPrettyBytes, _ := io.ReadAll(rawPrettyResp.Body)
	_ = rawPrettyResp.Body.Close()
	if !strings.Contains(string(rawPrettyBytes), "\n  \"cluster_name\":") {
		t.Fatalf("expected pretty-printed 2-space JSON by default, got:\n%s", string(rawPrettyBytes))
	}

	rawCompactResp, err := http.Get(srvs[0].URL + "/?pretty=false")
	if err != nil {
		t.Fatalf("get compact /: %v", err)
	}
	rawCompactBytes, _ := io.ReadAll(rawCompactResp.Body)
	_ = rawCompactResp.Body.Close()
	if strings.Contains(strings.TrimSpace(string(rawCompactBytes)), "\n") {
		t.Fatalf("expected single-line JSON with ?pretty=false, got:\n%s", string(rawCompactBytes))
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

	// 4. Cluster health, _nodes, & _cat endpoints
	code, health := doJSONReq(t, http.MethodGet, srvs[0].URL+"/_cluster/health", "", nil)
	if code != http.StatusOK || health["status"] != "green" ||
		int(health["number_of_nodes"].(float64)) != 2 ||
		int(health["number_of_data_nodes"].(float64)) != 2 {
		t.Fatalf("unexpected cluster health: code=%d body=%+v", code, health)
	}

	code, nodesInfo := doJSONReq(t, http.MethodGet, srvs[0].URL+"/_nodes", "", nil)
	if code != http.StatusOK || int(nodesInfo["_nodes"].(map[string]any)["total"].(float64)) != 2 {
		t.Fatalf("unexpected _nodes response: code=%d body=%+v", code, nodesInfo)
	}

	resp, err := http.Get(srvs[0].URL + "/_cat/nodes")
	if err != nil {
		t.Fatalf("get _cat/nodes: %v", err)
	}
	var catNodes []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&catNodes)
	_ = resp.Body.Close()
	if len(catNodes) != 2 || catNodes[0]["node.role"] != "d" {
		t.Fatalf("unexpected _cat/nodes: %+v", catNodes)
	}

	resp, err = http.Get(srvs[0].URL + "/_cat/indices")
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

	// 5. Explicit DELETE /{index}/_doc/{id} on auto-created index (with 1 async replica);
	// verify replica node converges to 404 after async WAL replication delivers the tombstone.
	code, delDoc := doJSONReq(t, http.MethodDelete, srvs[1].URL+"/metrics/_doc/m-1", "", nil)
	if code != http.StatusOK || delDoc["result"] != "deleted" {
		t.Fatalf("delete doc failed: code=%d body=%+v", code, delDoc)
	}
	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		code, _ = doJSONReq(t, http.MethodGet, srvs[0].URL+"/metrics/_doc/m-1", "", nil)
		if code == http.StatusNotFound {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if code != http.StatusNotFound {
		t.Fatalf("expected 404 after delete doc propagated to replica, got %d", code)
	}
}

func TestESSearchEndpointsAndRetrievalModes(t *testing.T) {
	// Bounded 32KB NVMe cache budget to verify hybrid mode behavior when hot vs evicted.
	nodes, srvs, _, _ := newTestCluster(t, 1, 32*1024)

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

	// 4. Evict idx_a .docs from the 32KB NVMe cache by writing 4x10KB entries,
	// then verify hybrid mode returns nvme_cached=false and omits _source!
	bigBlob := bytes.Repeat([]byte("z"), 10*1024)
	_ = nodes[0].NVMeCache().Put("evict-1", bigBlob, 0)
	_ = nodes[0].NVMeCache().Put("evict-2", bigBlob, 0)
	_ = nodes[0].NVMeCache().Put("evict-3", bigBlob, 0)
	_ = nodes[0].NVMeCache().Put("evict-4", bigBlob, 0)

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

func makeBulkBenchPayload(n int) []byte {
	services := []string{"checkout", "search", "payments", "inventory", "auth"}
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		fmt.Fprintf(&buf, "{\"index\":{\"_id\":\"doc-%d\"}}\n", i)
		fmt.Fprintf(&buf, "{\"service\":%q,\"level\":\"info\",\"latency_ms\":%d,\"message\":\"s3 range nvme segment lookup worker %d\"}\n",
			services[i%len(services)], (i%500)+1, i%16)
	}
	return buf.Bytes()
}

func seedBenchSearchCluster(b *testing.B, docs int) (*httptest.Server, *http.Client) {
	b.Helper()
	_, srvs, _, _ := newTestCluster(b, 1, 128*1024*1024)
	code, _ := doJSONReq(b, http.MethodPut, srvs[0].URL+"/bench?pretty=false", `{"settings":{"number_of_shards":3,"number_of_replicas":0}}`, nil)
	if code != http.StatusOK {
		b.Fatalf("create bench index: %d", code)
	}

	client := srvs[0].Client()
	payload := makeBulkBenchPayload(docs)
	resp, err := client.Post(srvs[0].URL+"/bench/_bulk?pretty=false", "application/x-ndjson", bytes.NewReader(payload))
	if err != nil {
		b.Fatalf("seed bulk: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		b.Fatalf("seed bulk status %d", resp.StatusCode)
	}

	fResp, err := client.Post(srvs[0].URL+"/bench/_refresh?flush=true&pretty=false", "application/json", nil)
	if err != nil {
		b.Fatalf("seed flush: %v", err)
	}
	_, _ = io.Copy(io.Discard, fResp.Body)
	_ = fResp.Body.Close()
	if fResp.StatusCode >= 300 {
		b.Fatalf("seed flush status %d", fResp.StatusCode)
	}
	return srvs[0], client
}

func BenchmarkESBulkIngestHTTP(b *testing.B) {
	_, srvs, _, _ := newTestCluster(b, 1, 128*1024*1024)
	code, _ := doJSONReq(b, http.MethodPut, srvs[0].URL+"/bench?pretty=false", `{"settings":{"number_of_shards":3,"number_of_replicas":0}}`, nil)
	if code != http.StatusOK {
		b.Fatalf("create bench index: %d", code)
	}

	client := srvs[0].Client()
	payload := makeBulkBenchPayload(100)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Post(srvs[0].URL+"/bench/_bulk?pretty=false", "application/x-ndjson", bytes.NewReader(payload))
		if err != nil {
			b.Fatalf("post _bulk: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			b.Fatalf("_bulk status %d", resp.StatusCode)
		}
	}
	b.StopTimer()
}

func BenchmarkESSearchDropInHTTP(b *testing.B) {
	srv, client := seedBenchSearchCluster(b, 500)
	queryBody := []byte(`{"query":{"bool":{"must":[{"match_phrase":{"message":"nvme segment"}}],"filter":[{"range":{"latency_ms":{"gte":100}}}]}},"size":10,"aggs":{"by_service":{"terms":{"field":"service"}},"stats":{"stats":{"field":"latency_ms"}}}}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Post(srv.URL+"/bench/_search?pretty=false", "application/json", bytes.NewReader(queryBody))
		if err != nil {
			b.Fatalf("post _search: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			b.Fatalf("_search status %d", resp.StatusCode)
		}
	}
	b.StopTimer()
}

func BenchmarkESSearchPresignedZeroHopHTTP(b *testing.B) {
	srv, client := seedBenchSearchCluster(b, 500)
	queryBody := []byte(`{"query":{"term":{"_id":"doc-42"}},"size":1}`)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Post(srv.URL+"/bench/_search?retrieval=presigned&pretty=false", "application/json", bytes.NewReader(queryBody))
		if err != nil {
			b.Fatalf("post presigned _search: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			b.Fatalf("presigned _search status %d", resp.StatusCode)
		}
	}
	b.StopTimer()
}

func TestESv8CompatHeadersHeadCreateConflictMappingCountAliasesAndWildcards(t *testing.T) {
	_, srvs, _, _ := newTestCluster(t, 2, 16*1024*1024)

	// 1. Official ES v8 client handshake headers & content-type negotiation
	v8Req, _ := http.NewRequest(http.MethodGet, srvs[0].URL+"/", nil)
	v8Req.Header.Set("Accept", "application/vnd.elasticsearch+json; compatible-with=8")
	v8Resp, err := http.DefaultClient.Do(v8Req)
	if err != nil {
		t.Fatalf("GET / with ES v8 Accept header: %v", err)
	}
	_ = v8Resp.Body.Close()
	if v8Resp.Header.Get("X-Elastic-Product") != "Elasticsearch" {
		t.Fatalf("expected X-Elastic-Product: Elasticsearch, got %q", v8Resp.Header.Get("X-Elastic-Product"))
	}
	if !strings.Contains(v8Resp.Header.Get("Content-Type"), "application/vnd.elasticsearch+json; compatible-with=8") {
		t.Fatalf("expected ES v8 compatible Content-Type, got %q", v8Resp.Header.Get("Content-Type"))
	}

	// 2. HEAD /{index} before & after creation (with inline alias + mapping)
	headMissing, err := http.Head(srvs[0].URL + "/logs-app")
	if err != nil || headMissing.StatusCode != http.StatusNotFound {
		t.Fatalf("expected HEAD /logs-app 404 before create, got err=%v resp=%v", err, headMissing)
	}
	_ = headMissing.Body.Close()

	code, _ := doJSONReq(t, http.MethodPut, srvs[0].URL+"/logs-app", `{
		"settings": {"number_of_shards": 2, "number_of_replicas": 0},
		"mappings": {"properties": {"env": {"type": "keyword"}}},
		"aliases": {"logs-live": {}}
	}`, nil)
	if code != http.StatusOK {
		t.Fatalf("create logs-app failed: %d", code)
	}

	for _, target := range []string{"/logs-app", "/logs-live"} {
		hResp, err := http.Head(srvs[1].URL + target)
		if err != nil || hResp.StatusCode != http.StatusOK {
			t.Fatalf("expected HEAD %s 200 on peer node, got err=%v resp=%v", target, err, hResp)
		}
		if hResp.Header.Get("X-Elastic-Product") != "Elasticsearch" {
			t.Fatalf("expected X-Elastic-Product header on HEAD %s", target)
		}
		_ = hResp.Body.Close()
	}

	// 3. GET/PUT /{index}/_mapping merge & peer sync
	code, _ = doJSONReq(t, http.MethodPut, srvs[0].URL+"/logs-app/_mapping", `{"properties":{"service":{"type":"keyword"}}}`, nil)
	if code != http.StatusOK {
		t.Fatalf("PUT _mapping failed: %d", code)
	}
	code, mapOut := doJSONReq(t, http.MethodGet, srvs[1].URL+"/logs-app/_mapping", "", nil)
	if code != http.StatusOK {
		t.Fatalf("GET _mapping failed: %d body=%+v", code, mapOut)
	}
	props := mapOut["logs-app"].(map[string]any)["mappings"].(map[string]any)["properties"].(map[string]any)
	if props["env"] == nil || props["service"] == nil {
		t.Fatalf("expected merged mapping properties env and service on peer, got %+v", props)
	}

	// 4. PUT /{index}/_create/{id}, ?op_type=create 409 Conflict (local + cross-node), and HEAD /{index}/_doc/{id}
	code, created := doJSONReq(t, http.MethodPut, srvs[0].URL+"/logs-app/_create/doc-1", `{"env":"prod","service":"api","msg":"first"}`, nil)
	if code != http.StatusCreated || created["result"] != "created" {
		t.Fatalf("expected 201 on first _create, got %d body=%+v", code, created)
	}
	code, dup1 := doJSONReq(t, http.MethodPut, srvs[0].URL+"/logs-app/_create/doc-1", `{"env":"prod","service":"api","msg":"dup"}`, nil)
	if code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on duplicate _create, got %d body=%+v", code, dup1)
	}
	code, dup2 := doJSONReq(t, http.MethodPut, srvs[1].URL+"/logs-app/_doc/doc-1?op_type=create", `{"env":"prod","service":"api","msg":"dup2"}`, nil)
	if code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on peer ?op_type=create, got %d body=%+v", code, dup2)
	}

	for i, srv := range srvs {
		hDoc, err := http.Head(srv.URL + "/logs-app/_doc/doc-1")
		if err != nil || hDoc.StatusCode != http.StatusOK {
			t.Fatalf("node %d HEAD existing doc failed: err=%v resp=%v", i, err, hDoc)
		}
		_ = hDoc.Body.Close()

		hMiss, err := http.Head(srv.URL + "/logs-app/_doc/does-not-exist")
		if err != nil || hMiss.StatusCode != http.StatusNotFound {
			t.Fatalf("node %d HEAD missing doc expected 404: err=%v resp=%v", i, err, hMiss)
		}
		_ = hMiss.Body.Close()
	}

	// 5. _bulk create 409 Conflict + malformed JSON item 400 + valid create 201
	bulkPayload := strings.Join([]string{
		`{"create":{"_index":"logs-app","_id":"doc-1"}}`,
		`{"env":"prod","msg":"conflict in bulk"}`,
		`{"create":{"_index":"logs-app","_id":"doc-2"}}`,
		`{"env":"prod","service":"web","msg":"second"}`,
		`{"index":{"_index":"logs-app","_id":"bad-1"}}`,
		`not-valid-json`,
		"",
	}, "\n")
	code, bulkRes := doJSONReq(t, http.MethodPost, srvs[0].URL+"/_bulk", bulkPayload, nil)
	if code != http.StatusOK || bulkRes["errors"] != true {
		t.Fatalf("expected 200 with errors=true for mixed _bulk, got %d body=%+v", code, bulkRes)
	}
	items := bulkRes["items"].([]any)
	if len(items) != 3 {
		t.Fatalf("expected 3 bulk items, got %d: %+v", len(items), items)
	}
	if int(items[0].(map[string]any)["create"].(map[string]any)["status"].(float64)) != http.StatusConflict {
		t.Fatalf("expected bulk item 0 status 409, got %+v", items[0])
	}
	if int(items[1].(map[string]any)["create"].(map[string]any)["status"].(float64)) != http.StatusCreated {
		t.Fatalf("expected bulk item 1 status 201, got %+v", items[1])
	}
	if int(items[2].(map[string]any)["index"].(map[string]any)["status"].(float64)) != http.StatusBadRequest {
		t.Fatalf("expected bulk item 2 status 400, got %+v", items[2])
	}

	// 6. Additional indices + atomic POST /_aliases + /{index}/_alias/{name} lifecycle
	_, _ = doJSONReq(t, http.MethodPut, srvs[0].URL+"/logs-debug", `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`, nil)
	_, _ = doJSONReq(t, http.MethodPut, srvs[0].URL+"/logs-debug/_doc/dbg-1", `{"env":"dev","msg":"debug trace"}`, nil)
	_, _ = doJSONReq(t, http.MethodPut, srvs[0].URL+"/metrics-1", `{"settings":{"number_of_shards":1,"number_of_replicas":0}}`, nil)
	_, _ = doJSONReq(t, http.MethodPut, srvs[0].URL+"/metrics-1/_doc/m-1", `{"host":"n1","cpu":0.4}`, nil)

	code, _ = doJSONReq(t, http.MethodPost, srvs[0].URL+"/_aliases", `{
		"actions": [
			{"add": {"index": "logs-*", "alias": "all-logs"}},
			{"remove": {"index": "logs-debug", "alias": "all-logs"}}
		]
	}`, nil)
	if code != http.StatusOK {
		t.Fatalf("POST /_aliases failed: %d", code)
	}

	code, _ = doJSONReq(t, http.MethodPut, srvs[0].URL+"/metrics-1/_alias/m-read", "", nil)
	if code != http.StatusOK {
		t.Fatalf("PUT /metrics-1/_alias/m-read failed: %d", code)
	}
	hAlias, err := http.Head(srvs[1].URL + "/_alias/m-read")
	if err != nil || hAlias.StatusCode != http.StatusOK {
		t.Fatalf("expected HEAD /_alias/m-read 200 on peer, got err=%v resp=%v", err, hAlias)
	}
	_ = hAlias.Body.Close()

	code, aliasMap := doJSONReq(t, http.MethodGet, srvs[1].URL+"/_alias/all-logs", "", nil)
	if code != http.StatusOK || aliasMap["logs-app"] == nil || aliasMap["logs-debug"] != nil {
		t.Fatalf("expected all-logs to point only to logs-app after atomic add/remove, got %d body=%+v", code, aliasMap)
	}

	code, _ = doJSONReq(t, http.MethodDelete, srvs[0].URL+"/metrics-1/_alias/m-read", "", nil)
	if code != http.StatusOK {
		t.Fatalf("DELETE /metrics-1/_alias/m-read failed: %d", code)
	}
	hAliasDel, err := http.Head(srvs[1].URL + "/_alias/m-read")
	if err != nil || hAliasDel.StatusCode != http.StatusNotFound {
		t.Fatalf("expected HEAD /_alias/m-read 404 after delete, got err=%v resp=%v", err, hAliasDel)
	}
	_ = hAliasDel.Body.Close()

	// 7. Wildcards, exclusions, _all, alias expansion, and _count
	assertCount := func(path string, want int) {
		t.Helper()
		c, out := doJSONReq(t, http.MethodGet, srvs[0].URL+path, "", nil)
		if c != http.StatusOK {
			t.Fatalf("GET %s failed: code=%d body=%+v", path, c, out)
		}
		if got := int(out["count"].(float64)); got != want {
			t.Fatalf("GET %s count mismatch: got %d want %d (body=%+v)", path, got, want, out)
		}
	}

	assertCount("/logs-*/_count", 3)
	assertCount("/logs-*,-logs-debug/_count", 2)
	assertCount("/_all/_count", 4)
	assertCount("/_count", 4)
	assertCount("/logs-live/_count", 2)
	assertCount("/no-such-*/_count", 0)

	code, aliasSearch := doJSONReq(t, http.MethodPost, srvs[1].URL+"/logs-live/_search", `{"query":{"term":{"env":"prod"}}}`, nil)
	if code != http.StatusOK {
		t.Fatalf("alias search failed: %d body=%+v", code, aliasSearch)
	}
	if got := int(aliasSearch["hits"].(map[string]any)["total"].(map[string]any)["value"].(float64)); got != 2 {
		t.Fatalf("expected 2 hits from alias logs-live, got %d", got)
	}

	code, zeroWild := doJSONReq(t, http.MethodPost, srvs[0].URL+"/nonexistent-*/_search", `{"query":{"match_all":{}}}`, nil)
	if code != http.StatusOK {
		t.Fatalf("unmatched wildcard search expected 200, got %d body=%+v", code, zeroWild)
	}
	if got := int(zeroWild["hits"].(map[string]any)["total"].(map[string]any)["value"].(float64)); got != 0 {
		t.Fatalf("expected 0 hits for unmatched wildcard, got %d", got)
	}
}

func FuzzESBulkNDJSON(f *testing.F) {
	s3Srv := s3store.NewMockServer("ak-fz", "sk-fz", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-fuzz",
		AccessKey: "ak-fz",
		SecretKey: "sk-fz",
	})
	if err != nil {
		f.Fatalf("new s3 client: %v", err)
	}

	node, err := cluster.NewNode(cluster.NodeConfig{
		NodeID:        "node-fuzz",
		NVMeDir:       f.TempDir(),
		NVMeMaxBytes:  8 * 1024 * 1024,
		FlushInterval: 60 * time.Second,
		ScrubInterval: 60 * time.Second,
		S3:            s3Client,
	})
	if err != nil {
		f.Fatalf("new node: %v", err)
	}
	defer node.Close()

	handler := api.NewHandler(node)

	f.Add([]byte("{\"index\":{\"_id\":\"1\"}}\n{\"msg\":\"ok\"}\n"))
	f.Add([]byte("{\"create\":{\"_index\":\"fuzz\",\"_id\":\"1\"}}\n{\"a\":1}\n"))
	f.Add([]byte("{\"delete\":{\"_index\":\"fuzz\",\"_id\":\"1\"}}\n"))
	f.Add([]byte("{\"update\":{\"_index\":\"fuzz\",\"_id\":\"1\"}}\n{\"doc\":{\"x\":2}}\n"))
	f.Add([]byte("{\"index\":{}}\n"))
	f.Add([]byte("not json\n{}"))
	f.Add([]byte("{\"index\":{\"_id\":\"1\"}}\nnull\n"))

	f.Fuzz(func(t *testing.T, payload []byte) {
		req := httptest.NewRequest(http.MethodPost, "/fuzzidx/_bulk?pretty=false", bytes.NewReader(payload))
		req.Header.Set("Content-Type", "application/x-ndjson")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code == 0 {
			t.Fatalf("expected non-zero HTTP status code")
		}
	})
}
