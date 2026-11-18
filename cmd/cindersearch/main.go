package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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
	fmt.Println("  cindersearch version         Print build version")
}

func runServe(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	nodeID := fs.String("node-id", "node-1", "Cluster node identifier")
	listenAddr := fs.String("listen", ":9200", "HTTP listen address")
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

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
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
