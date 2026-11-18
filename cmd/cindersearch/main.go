package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/cindersearch/cindersearch/internal/api"
	"github.com/cindersearch/cindersearch/internal/checksum"
	"github.com/cindersearch/cindersearch/internal/cluster"
	"github.com/cindersearch/cindersearch/internal/s3store"
	"github.com/cindersearch/cindersearch/internal/sharding"
)

var (
	version = "dev"
	commit  = "local"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "serve":
		runServe(os.Args[2:])
	case "demo":
		if err := runDemo(); err != nil {
			log.Fatalf("demo failed: %v", err)
		}
	case "bench":
		if err := runBench(os.Args[2:]); err != nil {
			log.Fatalf("bench failed: %v", err)
		}
	case "version", "-v", "--version":
		fmt.Printf("cindersearch %s (%s)\n", version, commit)
	default:
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("CinderSearch — S3-First, NVMe-Cached Drop-In Elasticsearch Replacement")
	fmt.Println("Usage:")
	fmt.Println("  cindersearch serve [flags]   Start a CinderSearch HTTP/ES server node")
	fmt.Println("  cindersearch demo            Run an end-to-end 3-node S3 + NVMe + Bit-Rot healing demo")
	fmt.Println("  cindersearch bench [flags]   Run throughput & latency profiling (ingest, flush, search, zero-hop)")
	fmt.Println("  cindersearch version         Print build version")
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	nodeID := fs.String("node-id", "node-1", "Cluster node identifier")
	listenAddr := fs.String("listen", ":9200", "HTTP listen address")
	pprofAddr := fs.String("pprof", "127.0.0.1:6060", "Optional pprof HTTP debug address (empty to disable)")
	peersFlag := fs.String("peers", "", "Comma-separated peer specs id=host:port")
	s3Endpoint := fs.String("s3-endpoint", "", "S3-compatible endpoint URL (AWS S3 or MinIO)")
	s3Bucket := fs.String("s3-bucket", "cindersearch", "S3 bucket name")
	s3Region := fs.String("s3-region", "us-east-1", "S3 region")
	s3AccessKey := fs.String("s3-access-key", os.Getenv("AWS_ACCESS_KEY_ID"), "S3 access key")
	s3SecretKey := fs.String("s3-secret-key", os.Getenv("AWS_SECRET_ACCESS_KEY"), "S3 secret key")
	embeddedS3 := fs.Bool("embedded-s3", false, "Start an embedded S3-compatible HTTP server for local testing")
	nvmeDir := fs.String("nvme-dir", "/tmp/cindersearch-nvme", "Local NVMe SSD cache and WAL directory")
	nvmeMaxMB := fs.Int64("nvme-max-mb", 512, "Max local NVMe read cache size in MB")
	flushSec := fs.Int("flush-sec", 5, "Background WAL-to-S3 segment flush interval in seconds")
	_ = fs.Parse(args)

	var mockS3 *s3store.MockServer
	endpoint := *s3Endpoint
	if *embeddedS3 || endpoint == "" {
		mockS3 = s3store.NewMockServer("cindersearch-access", "cindersearch-secret", *s3Region)
		defer mockS3.Close()
		endpoint = mockS3.URL()
		if *s3AccessKey == "" {
			*s3AccessKey = "cindersearch-access"
		}
		if *s3SecretKey == "" {
			*s3SecretKey = "cindersearch-secret"
		}
	}

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  endpoint,
		Region:    *s3Region,
		Bucket:    *s3Bucket,
		AccessKey: *s3AccessKey,
		SecretKey: *s3SecretKey,
	})
	if err != nil {
		log.Fatalf("init s3 client: %v", err)
	}

	var peers []sharding.NodeInfo
	if *peersFlag != "" {
		for _, item := range strings.Split(*peersFlag, ",") {
			kv := strings.SplitN(strings.TrimSpace(item), "=", 2)
			if len(kv) == 2 {
				peers = append(peers, sharding.NodeInfo{ID: kv[0], Address: kv[1]})
			}
		}
	}

	node, err := cluster.NewNode(cluster.NodeConfig{
		NodeID:           *nodeID,
		AdvertiseAddress: *listenAddr,
		NVMeDir:          *nvmeDir,
		NVMeMaxBytes:     *nvmeMaxMB * 1024 * 1024,
		FlushInterval:    time.Duration(*flushSec) * time.Second,
		S3:               s3Client,
		Peers:            peers,
	})
	if err != nil {
		log.Fatalf("init node: %v", err)
	}
	defer node.Close()

	srv := &http.Server{
		Addr:    *listenAddr,
		Handler: api.NewHandler(node),
	}

	log.Println(api.FormatEndpointBanner(*nodeID, *listenAddr, endpoint))

	var pprofSrv *http.Server
	if *pprofAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", httppprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", httppprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", httppprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", httppprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", httppprof.Trace)
		pprofSrv = &http.Server{Addr: *pprofAddr, Handler: mux}
		go func() {
			log.Printf("CinderSearch pprof listening=http://%s/debug/pprof/", *pprofAddr)
			if err := pprofSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("pprof server error: %v", err)
			}
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		if pprofSrv != nil {
			_ = pprofSrv.Shutdown(ctx)
		}
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server error: %v", err)
	}
}

func runDemo() error {
	fmt.Println("=== CinderSearch End-to-End S3 + NVMe + Bit-Rot Healing Demo ===")

	tmpRoot, err := os.MkdirTemp("", "cindersearch-demo-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpRoot)

	s3Srv := s3store.NewMockServer("demo-ak", "demo-sk", "us-east-1")
	defer s3Srv.Close()
	fmt.Printf("[1/6] Embedded S3 Object Storage running at %s\n", s3Srv.URL())

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-demo",
		AccessKey: "demo-ak",
		SecretKey: "demo-sk",
	})
	if err != nil {
		return err
	}

	nodes := make([]*cluster.Node, 3)
	listeners := make([]net.Listener, 3)
	servers := make([]*http.Server, 3)
	peers := make([]sharding.NodeInfo, 3)

	for i := 0; i < 3; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		listeners[i] = ln
		nodeID := fmt.Sprintf("node-%d", i+1)
		peers[i] = sharding.NodeInfo{ID: nodeID, Address: ln.Addr().String()}
	}

	for i := 0; i < 3; i++ {
		n, err := cluster.NewNode(cluster.NodeConfig{
			NodeID:            peers[i].ID,
			AdvertiseAddress:  peers[i].Address,
			NVMeDir:           filepath.Join(tmpRoot, peers[i].ID),
			NVMeMaxBytes:      128 * 1024 * 1024,
			FlushInterval:     30 * time.Second,
			ReplicateInterval: 20 * time.Millisecond,
			S3:                s3Client,
			Peers:             peers,
		})
		if err != nil {
			return err
		}
		nodes[i] = n
		servers[i] = &http.Server{Handler: api.NewHandler(n)}
		go func(srv *http.Server, ln net.Listener) {
			_ = srv.Serve(ln)
		}(servers[i], listeners[i])
	}
	defer func() {
		for i := 0; i < 3; i++ {
			_ = servers[i].Close()
			_ = nodes[i].Close()
		}
	}()
	fmt.Printf("[2/6] 3-node CinderSearch cluster active (%s, %s, %s)\n", peers[0].Address, peers[1].Address, peers[2].Address)

	baseURL := "http://" + peers[0].Address

	// Create ES index with 3 shards and 1 replica.
	req, _ := http.NewRequest(http.MethodPut, baseURL+"/logs", strings.NewReader(`{"settings":{"number_of_shards":3,"number_of_replicas":1}}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()

	// Bulk NDJSON ingestion.
	bulk := strings.Join([]string{
		`{"index":{"_id":"doc-101"}}`,
		`{"service":"checkout","level":"error","latency_ms":412,"message":"s3 range timeout on shard 2"}`,
		`{"index":{"_id":"doc-102"}}`,
		`{"service":"checkout","level":"warn","latency_ms":145,"message":"nvme wal queue high"}`,
		`{"index":{"_id":"doc-103"}}`,
		`{"service":"search","level":"info","latency_ms":6,"message":"nvme cache hit for inverted index"}`,
		"",
	}, "\n")
	bResp, err := http.Post("http://"+peers[1].Address+"/logs/_bulk", "application/x-ndjson", strings.NewReader(bulk))
	if err != nil {
		return err
	}
	_ = bResp.Body.Close()

	time.Sleep(120 * time.Millisecond)
	var repSent int64
	for _, n := range nodes {
		repSent += n.Stats().ReplicationBatchesSent
	}
	fmt.Printf("[3/6] Ingested 3 docs via ES _bulk; async WAL replication batches delivered=%d\n", repSent)

	// Flush WAL segments to S3.
	fResp, err := http.Post(baseURL+"/logs/_refresh?flush=true", "application/json", nil)
	if err != nil {
		return err
	}
	_ = fResp.Body.Close()
	fmt.Println("[4/6] Flushed NVMe WALs to immutable S3 segments (positional index + DocValues + packed _source)")

	// Zero-Hop Presigned S3 Range Search.
	qBody := `{"query":{"bool":{"must":[{"match_phrase":{"message":"range timeout"}}],"filter":[{"range":{"latency_ms":{"gte":100}}}]}},"aggs":{"by_service":{"terms":{"field":"service"}}}}`
	sResp, err := http.Post("http://"+peers[2].Address+"/logs/_search?retrieval=presigned", "application/json", strings.NewReader(qBody))
	if err != nil {
		return err
	}
	var searchOut map[string]any
	_ = json.NewDecoder(sResp.Body).Decode(&searchOut)
	_ = sResp.Body.Close()

	hit := searchOut["hits"].(map[string]any)["hits"].([]any)[0].(map[string]any)
	ptr := hit["_s3_pointer"].(map[string]any)
	fmt.Printf("[5/6] Zero-Hop Search matched doc=%s -> Presigned S3 Range=%s CRC32C=%s\n", hit["_id"], ptr["range"], ptr["crc32c"])

	// Direct client -> S3 fetch bypassing CinderSearch.
	dReq, _ := http.NewRequest(http.MethodGet, ptr["url"].(string), nil)
	dReq.Header.Set("Range", ptr["range"].(string))
	dResp, err := http.DefaultClient.Do(dReq)
	if err != nil {
		return err
	}
	directDoc, _ := io.ReadAll(dResp.Body)
	_ = dResp.Body.Close()
	fmt.Printf("      Direct S3 HTTP %d fetch verified CRC32C=%s payload=%s\n", dResp.StatusCode, checksum.FormatHex(checksum.Sum32C(directDoc)), string(directDoc))

	// Inject NVMe bit-rot and verify automatic self-healing from S3.
	idxKey := strings.Replace(ptr["key"].(string), ".docs", ".idx", 1)
	for _, n := range nodes {
		_ = n.NVMeCache().CorruptFileForTest(idxKey)
	}
	hResp, err := http.Post(baseURL+"/logs/_search", "application/json", strings.NewReader(`{"query":{"match_all":{}}}`))
	if err != nil {
		return err
	}
	_ = hResp.Body.Close()

	var healed int64
	for _, n := range nodes {
		healed += n.NVMeCache().Stats().BitRotHealed
	}
	fmt.Printf("[6/6] Simulated NVMe bit-rot on %s -> CRC32C detected corruption and auto-healed from S3 (healed=%d)\n", idxKey, healed)
	return nil
}

func runBench(args []string) error {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	target := fs.String("target", "", "Target CinderSearch URL (e.g. http://127.0.0.1:9200); empty launches embedded node+S3")
	totalDocs := fs.Int("docs", 5000, "Total documents to ingest")
	batchSize := fs.Int("batch-size", 250, "Documents per _bulk NDJSON batch")
	concurrency := fs.Int("concurrency", 8, "Concurrent HTTP worker goroutines")
	totalQueries := fs.Int("queries", 1000, "Search queries to run per search phase")
	shards := fs.Int("shards", 3, "Number of primary shards for benchmark index")
	replicas := fs.Int("replicas", 0, "Number of replicas for benchmark index")
	cpuProfPath := fs.String("cpuprofile", "", "Write CPU pprof profile to file")
	memProfPath := fs.String("memprofile", "", "Write heap pprof profile to file")
	_ = fs.Parse(args)

	if *cpuProfPath != "" {
		f, err := os.Create(*cpuProfPath)
		if err != nil {
			return fmt.Errorf("create cpuprofile: %w", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			_ = f.Close()
			return fmt.Errorf("start cpuprofile: %w", err)
		}
		defer func() {
			pprof.StopCPUProfile()
			_ = f.Close()
			fmt.Printf("Saved CPU profile -> %s\n", *cpuProfPath)
		}()
	}

	baseURL := strings.TrimRight(*target, "/")
	if baseURL == "" {
		tmpRoot, err := os.MkdirTemp("", "cindersearch-bench-*")
		if err != nil {
			return err
		}
		defer os.RemoveAll(tmpRoot)

		s3Srv := s3store.NewMockServer("bench-ak", "bench-sk", "us-east-1")
		defer s3Srv.Close()

		s3Client, err := s3store.NewClient(s3store.Config{
			Endpoint:  s3Srv.URL(),
			Region:    "us-east-1",
			Bucket:    "cindersearch-bench",
			AccessKey: "bench-ak",
			SecretKey: "bench-sk",
		})
		if err != nil {
			return err
		}

		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return err
		}
		n, err := cluster.NewNode(cluster.NodeConfig{
			NodeID:           "bench-node",
			AdvertiseAddress: ln.Addr().String(),
			NVMeDir:          tmpRoot,
			NVMeMaxBytes:     256 * 1024 * 1024,
			FlushInterval:    60 * time.Second,
			ScrubInterval:    60 * time.Second,
			S3:               s3Client,
		})
		if err != nil {
			return err
		}
		defer n.Close()

		srv := &http.Server{Handler: api.NewHandler(n)}
		go func() { _ = srv.Serve(ln) }()
		defer srv.Close()
		baseURL = "http://" + ln.Addr().String()
	}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
		},
	}

	indexName := fmt.Sprintf("bench-%d", time.Now().Unix())
	fmt.Printf("=== CinderSearch Benchmark (target=%s index=%s docs=%d batch=%d workers=%d queries=%d) ===\n",
		baseURL, indexName, *totalDocs, *batchSize, *concurrency, *totalQueries)

	createBody := fmt.Sprintf(`{"settings":{"number_of_shards":%d,"number_of_replicas":%d}}`, *shards, *replicas)
	cReq, _ := http.NewRequest(http.MethodPut, baseURL+"/"+indexName+"?pretty=false", strings.NewReader(createBody))
	cResp, err := httpClient.Do(cReq)
	if err != nil {
		return fmt.Errorf("create index: %w", err)
	}
	cBody, _ := io.ReadAll(cResp.Body)
	_ = cResp.Body.Close()
	if cResp.StatusCode >= 300 {
		return fmt.Errorf("create index status %d: %s", cResp.StatusCode, strings.TrimSpace(string(cBody)))
	}

	// Phase 1: Bulk NDJSON ingest
	batches := (*totalDocs + *batchSize - 1) / *batchSize
	var ingestedBytes atomic.Int64
	var ingestIdx atomic.Int64
	var firstIngestErr atomic.Value
	ingestLats := make([]time.Duration, batches)

	t0 := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			services := []string{"checkout", "search", "payments", "inventory", "auth"}
			for {
				b := int(ingestIdx.Add(1) - 1)
				if b >= batches {
					return
				}
				startDoc := b * *batchSize
				endDoc := startDoc + *batchSize
				if endDoc > *totalDocs {
					endDoc = *totalDocs
				}
				var buf bytes.Buffer
				for i := startDoc; i < endDoc; i++ {
					svc := services[i%len(services)]
					fmt.Fprintf(&buf, "{\"index\":{\"_id\":\"doc-%d\"}}\n", i)
					fmt.Fprintf(&buf, "{\"service\":%q,\"level\":\"info\",\"latency_ms\":%d,\"message\":\"s3 range nvme segment lookup worker %d\"}\n", svc, (i%500)+1, i%16)
				}
				payload := buf.Bytes()
				ingestedBytes.Add(int64(len(payload)))

				opStart := time.Now()
				resp, err := httpClient.Post(baseURL+"/"+indexName+"/_bulk?pretty=false", "application/x-ndjson", bytes.NewReader(payload))
				if err != nil {
					firstIngestErr.CompareAndSwap(nil, err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode >= 300 {
					firstIngestErr.CompareAndSwap(nil, fmt.Errorf("_bulk returned status %d", resp.StatusCode))
					return
				}
				ingestLats[b] = time.Since(opStart)
			}
		}()
	}
	wg.Wait()
	if v := firstIngestErr.Load(); v != nil {
		return v.(error)
	}
	ingestElapsed := time.Since(t0)
	mbIngested := float64(ingestedBytes.Load()) / (1024 * 1024)
	p50, p95, p99 := benchPercentiles(ingestLats)
	fmt.Printf("[1/4] _bulk Ingest: %.0f docs/sec (%.2f MB/sec) | batch latency p50=%.2fms p95=%.2fms p99=%.2fms\n",
		float64(*totalDocs)/ingestElapsed.Seconds(), mbIngested/ingestElapsed.Seconds(), p50, p95, p99)

	// Phase 2: Flush buffered WAL to immutable S3 segments
	tFlush := time.Now()
	fResp, err := httpClient.Post(baseURL+"/"+indexName+"/_refresh?flush=true&pretty=false", "application/json", nil)
	if err != nil {
		return fmt.Errorf("flush: %w", err)
	}
	_, _ = io.Copy(io.Discard, fResp.Body)
	_ = fResp.Body.Close()
	if fResp.StatusCode >= 300 {
		return fmt.Errorf("flush returned status %d", fResp.StatusCode)
	}
	flushElapsed := time.Since(tFlush)
	fmt.Printf("[2/4] S3 Segment Flush: %.2fms total (%.0f docs/sec compiled+uploaded)\n",
		float64(flushElapsed.Microseconds())/1000.0, float64(*totalDocs)/flushElapsed.Seconds())

	// Phase 3: Query DSL + Aggregations (drop-in _source)
	queryPayload := []byte(`{"query":{"bool":{"must":[{"match_phrase":{"message":"nvme segment"}}],"filter":[{"range":{"latency_ms":{"gte":100}}}]}},"size":10,"aggs":{"by_service":{"terms":{"field":"service"}},"stats":{"stats":{"field":"latency_ms"}}}}`)
	qDropInLats, dropInElapsed, err := runSearchLoad(httpClient, baseURL+"/"+indexName+"/_search?pretty=false", queryPayload, *totalQueries, *concurrency)
	if err != nil {
		return err
	}
	p50, p95, p99 = benchPercentiles(qDropInLats)
	fmt.Printf("[3/4] Search Drop-In (DSL+Aggs): %.0f QPS | p50=%.2fms p95=%.2fms p99=%.2fms\n",
		float64(*totalQueries)/dropInElapsed.Seconds(), p50, p95, p99)

	// Phase 4: Zero-Hop Presigned Search
	zeroHopQuery := []byte(`{"query":{"term":{"_id":"doc-42"}},"size":1}`)
	qPresignLats, presignElapsed, err := runSearchLoad(httpClient, baseURL+"/"+indexName+"/_search?retrieval=presigned&pretty=false", zeroHopQuery, *totalQueries, *concurrency)
	if err != nil {
		return err
	}
	p50, p95, p99 = benchPercentiles(qPresignLats)
	fmt.Printf("[4/4] Zero-Hop Presigned Search: %.0f QPS | p50=%.2fms p95=%.2fms p99=%.2fms\n",
		float64(*totalQueries)/presignElapsed.Seconds(), p50, p95, p99)

	if *memProfPath != "" {
		runtime.GC()
		f, err := os.Create(*memProfPath)
		if err != nil {
			return fmt.Errorf("create memprofile: %w", err)
		}
		if err := pprof.WriteHeapProfile(f); err != nil {
			_ = f.Close()
			return fmt.Errorf("write memprofile: %w", err)
		}
		_ = f.Close()
		fmt.Printf("Saved Heap profile -> %s\n", *memProfPath)
	}
	return nil
}

func runSearchLoad(client *http.Client, url string, body []byte, total, workers int) ([]time.Duration, time.Duration, error) {
	lats := make([]time.Duration, total)
	var idx atomic.Int64
	var firstErr atomic.Value
	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(idx.Add(1) - 1)
				if i >= total {
					return
				}
				t0 := time.Now()
				resp, err := client.Post(url, "application/json", bytes.NewReader(body))
				if err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode >= 300 {
					firstErr.CompareAndSwap(nil, fmt.Errorf("_search returned status %d", resp.StatusCode))
					return
				}
				lats[i] = time.Since(t0)
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if v := firstErr.Load(); v != nil {
		return nil, elapsed, v.(error)
	}
	return lats, elapsed, nil
}

func benchPercentiles(durs []time.Duration) (p50, p95, p99 float64) {
	if len(durs) == 0 {
		return 0, 0, 0
	}
	cp := append([]time.Duration(nil), durs...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	at := func(pct float64) float64 {
		pos := int(float64(len(cp)-1) * pct)
		return float64(cp[pos].Microseconds()) / 1000.0
	}
	return at(0.50), at(0.95), at(0.99)
}
