package cluster_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cindersearch/cindersearch/internal/api"
	"github.com/cindersearch/cindersearch/internal/cache"
	"github.com/cindersearch/cindersearch/internal/cluster"
	"github.com/cindersearch/cindersearch/internal/query"
	"github.com/cindersearch/cindersearch/internal/s3store"
	"github.com/cindersearch/cindersearch/internal/sharding"
)

type dynamicNodeHandle struct {
	node *cluster.Node
	srv  *http.Server
	ln   net.Listener
	addr string
}

func startDynamicNode(t *testing.T, nodeID string, roles []string, s3Client *s3store.Client, disc cluster.DiscoveryConfig) *dynamicNodeHandle {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for %s: %v", nodeID, err)
	}
	addr := ln.Addr().String()

	n, err := cluster.NewNode(cluster.NodeConfig{
		NodeID:            nodeID,
		Roles:             roles,
		AdvertiseAddress:  addr,
		NVMeDir:           t.TempDir(),
		NVMeMaxBytes:      16 * 1024 * 1024,
		FlushInterval:     30 * time.Second,
		ReplicateInterval: 15 * time.Millisecond,
		ScrubInterval:     30 * time.Second,
		Discovery:         disc,
		S3:                s3Client,
	})
	if err != nil {
		_ = ln.Close()
		t.Fatalf("new dynamic node %s: %v", nodeID, err)
	}

	srv := &http.Server{Handler: api.NewHandler(n)}
	go func() {
		_ = srv.Serve(ln)
	}()

	h := &dynamicNodeHandle{node: n, srv: srv, ln: ln, addr: addr}
	t.Cleanup(func() {
		_ = srv.Close()
		_ = n.Close()
	})
	return h
}

func waitUntil(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", timeout, desc)
}

func TestDynamicS3MembershipJoinScaleOutAndReaderSync(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-dyn", "sk-dyn", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-dynamic-join",
		AccessKey: "ak-dyn",
		SecretKey: "sk-dyn",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	disc := cluster.DiscoveryConfig{
		Mode:              cluster.DiscoveryModeS3,
		HeartbeatInterval: 40 * time.Millisecond,
		LeaseTTL:          300 * time.Millisecond,
		SyncInterval:      40 * time.Millisecond,
	}

	// Boot 2 data nodes and 1 coordinator with ZERO static --peers.
	d1 := startDynamicNode(t, "data-1", []string{sharding.RoleData}, s3Client, disc)
	d2 := startDynamicNode(t, "data-2", []string{sharding.RoleData}, s3Client, disc)
	coord := startDynamicNode(t, "coord-1", []string{sharding.RoleCoordinator}, s3Client, disc)

	waitUntil(t, 3*time.Second, "initial 3-node S3 discovery", func() bool {
		return len(coord.node.Ring().Nodes()) == 3 &&
			len(coord.node.Ring().DataNodes()) == 2 &&
			len(d1.node.Ring().DataNodes()) == 2 &&
			len(d2.node.Ring().DataNodes()) == 2
	})

	// Create index & ingest 6 documents through coordinator, then flush to S3.
	if err := coord.node.CreateIndex(ctx, cluster.IndexMetadata{
		Name:     "catalog",
		Settings: cluster.IndexSettings{NumberOfShards: 2, NumberOfReplicas: 1},
	}, true); err != nil {
		t.Fatalf("create catalog index: %v", err)
	}
	for i := 1; i <= 6; i++ {
		id := fmt.Sprintf("sku-%d", i)
		doc := fmt.Sprintf(`{"title":"espresso grinder %d","price":%d}`, i, 100+i*10)
		if _, err := coord.node.IndexDocMeta(ctx, "catalog", id, []byte(doc), "", ""); err != nil {
			t.Fatalf("index %s: %v", id, err)
		}
	}
	if err := coord.node.RefreshIndex(ctx, "catalog", true, true); err != nil {
		t.Fatalf("flush catalog to S3: %v", err)
	}

	// Dynamically scale out by adding a 3rd data node and a stateless reader node (zero static peers).
	d3 := startDynamicNode(t, "data-3", []string{sharding.RoleData}, s3Client, disc)
	reader := startDynamicNode(t, "reader-1", []string{sharding.RoleReader}, s3Client, disc)

	waitUntil(t, 3*time.Second, "scale-out discovery of data-3 and reader-1", func() bool {
		return len(coord.node.Ring().Nodes()) == 5 &&
			len(coord.node.Ring().DataNodes()) == 3 &&
			len(coord.node.Ring().ReaderNodes()) == 1 &&
			d3.node.HasIndexOrAlias("catalog") &&
			reader.node.HasIndexOrAlias("catalog")
	})

	// Verify reader automatically synced index metadata from S3 and serves all 6 docs with preference=reader.
	waitUntil(t, 3*time.Second, "reader-1 serves all 6 flushed catalog docs", func() bool {
		resp, err := coord.node.SearchWithPreference(ctx, []string{"catalog"}, query.SearchRequest{
			Query: map[string]any{"match": map[string]any{"title": "espresso"}},
		}, cluster.RetrievalDropIn, true)
		return err == nil && resp.Hits.Total.Value == 6
	})

	// Verify writes still succeed after ring rebalanced to 3 data nodes.
	res, err := coord.node.IndexDocMeta(ctx, "catalog", "sku-7", []byte(`{"title":"espresso tamp","price":45}`), "", "")
	if err != nil {
		t.Fatalf("write sku-7 after scale-out: %v", err)
	}
	if res.PrimaryTerm < 1 || res.ShardsTotal != 2 {
		t.Fatalf("unexpected write meta after scale-out: %+v", res)
	}
}

func TestPrimaryCrashAutoPromotionAndWriteFailover(t *testing.T) {
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

	disc := cluster.DiscoveryConfig{
		Mode:              cluster.DiscoveryModeS3,
		HeartbeatInterval: 35 * time.Millisecond,
		LeaseTTL:          160 * time.Millisecond,
		SyncInterval:      35 * time.Millisecond,
	}

	d1 := startDynamicNode(t, "data-1", []string{sharding.RoleData}, s3Client, disc)
	d2 := startDynamicNode(t, "data-2", []string{sharding.RoleData}, s3Client, disc)
	coord := startDynamicNode(t, "coord-1", []string{sharding.RoleCoordinator}, s3Client, disc)

	waitUntil(t, 3*time.Second, "2 data nodes + coordinator ready", func() bool {
		return len(coord.node.Ring().DataNodes()) == 2 &&
			len(d1.node.Ring().DataNodes()) == 2 &&
			len(d2.node.Ring().DataNodes()) == 2
	})

	if err := coord.node.CreateIndex(ctx, cluster.IndexMetadata{
		Name:     "orders",
		Settings: cluster.IndexSettings{NumberOfShards: 1, NumberOfReplicas: 1},
	}, true); err != nil {
		t.Fatalf("create orders index: %v", err)
	}

	// Ingest 2 documents into WAL (without flushing to S3) and wait for async WAL replication.
	for _, doc := range []struct {
		id   string
		body string
	}{
		{"ord-1", `{"customer":"alice","amount":99}`},
		{"ord-2", `{"customer":"bob","amount":149}`},
	} {
		if _, err := coord.node.IndexDocMeta(ctx, "orders", doc.id, []byte(doc.body), "", ""); err != nil {
			t.Fatalf("index %s: %v", doc.id, err)
		}
	}

	// Determine which node is primary and which is replica.
	placement, err := coord.node.Ring().PlacementForShard("orders", 0, 1)
	if err != nil {
		t.Fatalf("placement for orders[0]: %v", err)
	}
	primaryH := d1
	survivorH := d2
	if placement.Primary.ID == "data-2" {
		primaryH = d2
		survivorH = d1
	}

	// Wait for async WAL replication to deliver both records to the replica.
	waitUntil(t, 2*time.Second, "async WAL replication to replica", func() bool {
		got, err := survivorH.node.GetDocMeta(ctx, "orders", "ord-2", "")
		return err == nil && got.Found
	})

	// Simulate an ungraceful crash of the primary (no lease deletion!).
	_ = primaryH.srv.Close()
	primaryH.node.SimulateCrash()

	// Wait for LeaseTTL expiration and automated replica-to-primary promotion.
	waitUntil(t, 3*time.Second, "surviving replica promoted to primary", func() bool {
		return survivorH.node.Stats().PromotionsCompleted >= 1 &&
			survivorH.node.PrimaryTermForShard("orders", 0) >= 2 &&
			len(coord.node.Ring().DataNodes()) == 1
	})

	// Verify previously replicated docs remain intact.
	for _, id := range []string{"ord-1", "ord-2"} {
		got, err := coord.node.GetDocMeta(ctx, "orders", id, "")
		if err != nil || !got.Found {
			t.Fatalf("expected %s readable after primary crash, got found=%v err=%v", id, got.Found, err)
		}
	}

	// Verify new writes succeed on the promoted primary with primary_term >= 2.
	writeRes, err := coord.node.IndexDocMeta(ctx, "orders", "ord-3", []byte(`{"customer":"carol","amount":210}`), "", "")
	if err != nil {
		t.Fatalf("write ord-3 after failover: %v", err)
	}
	if writeRes.PrimaryTerm < 2 {
		t.Fatalf("expected primary_term >= 2 after promotion, got %+v", writeRes)
	}

	// Verify /_cluster/health reports yellow with 1 active primary and 1 unassigned replica.
	health, err := coord.node.ClusterHealth(nil, "cluster")
	if err != nil {
		t.Fatalf("cluster health: %v", err)
	}
	if health["status"] != "yellow" ||
		health["active_primary_shards"] != 1 ||
		health["active_shards"] != 1 ||
		health["unassigned_shards"] != 1 {
		t.Fatalf("expected yellow health with 1 unassigned replica, got %+v", health)
	}
}

func TestStalePrimaryFencingOnManifestCommit(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-fence", "sk-fence", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-fencing",
		AccessKey: "ak-fence",
		SecretKey: "sk-fence",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	cacheA, err := cache.NewNVMeCache(t.TempDir(), 4*1024*1024)
	if err != nil {
		t.Fatalf("cacheA: %v", err)
	}
	cacheB, err := cache.NewNVMeCache(t.TempDir(), 4*1024*1024)
	if err != nil {
		t.Fatalf("cacheB: %v", err)
	}

	// Node A opens shard 0 as primary (term 1) and flushes d1 to S3.
	shardA, err := cluster.OpenShard(ctx, "fenced", 0, true, t.TempDir(), cacheA, s3Client)
	if err != nil {
		t.Fatalf("open shardA: %v", err)
	}
	defer shardA.Close()
	shardA.SetOwnerNodeID("node-a")

	if _, err := shardA.IndexLocal("d1", []byte(`{"v":1}`)); err != nil {
		t.Fatalf("shardA index d1: %v", err)
	}
	if _, err := shardA.FlushToS3(ctx); err != nil {
		t.Fatalf("shardA initial flush: %v", err)
	}

	// Node B opens shard 0 as replica and promotes to primary (term 2).
	shardB, err := cluster.OpenShard(ctx, "fenced", 0, false, t.TempDir(), cacheB, s3Client)
	if err != nil {
		t.Fatalf("open shardB: %v", err)
	}
	defer shardB.Close()
	shardB.SetOwnerNodeID("node-b")

	newTerm, err := shardB.PromoteToPrimary(ctx, "node-b")
	if err != nil {
		t.Fatalf("shardB promote: %v", err)
	}
	if newTerm != 2 || !shardB.IsPrimary() || shardB.PrimaryTerm() != 2 {
		t.Fatalf("expected shardB promoted to term 2, got term=%d isPrimary=%v", newTerm, shardB.IsPrimary())
	}

	// Stale partitioned Node A (still at term 1) writes locally and attempts to commit to S3 -> MUST BE FENCED.
	if _, err := shardA.IndexLocal("d-stale", []byte(`{"v":999}`)); err != nil {
		t.Fatalf("shardA stale local index: %v", err)
	}
	if _, err := shardA.FlushToS3(ctx); !errors.Is(err, cluster.ErrStalePrimaryTerm) {
		t.Fatalf("expected ErrStalePrimaryTerm from stale primary flush, got %v", err)
	}
	if shardA.IsPrimary() {
		t.Fatalf("expected stale primary shardA to demote itself upon fencing")
	}

	// Verify S3 manifest still belongs to node-b at term 2 (never overwritten by stale node-a).
	manRaw, _, err := s3Client.GetObject(ctx, "fenced/shard-0/manifest.json")
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var man cluster.ShardManifest
	if err := json.Unmarshal(manRaw, &man); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if man.PrimaryTerm != 2 || man.PrimaryNode != "node-b" {
		t.Fatalf("expected fenced manifest PrimaryTerm=2 PrimaryNode=node-b, got %+v", man)
	}

	// Active term-2 primary Node B continues flushing cleanly.
	if _, err := shardB.IndexLocal("d2", []byte(`{"v":2}`)); err != nil {
		t.Fatalf("shardB index d2: %v", err)
	}
	if ref, err := shardB.FlushToS3(ctx); err != nil || ref == nil {
		t.Fatalf("expected term-2 primary shardB flush to succeed, ref=%+v err=%v", ref, err)
	}
}

func TestGracefulDrainFlushesWALAndRemovesLease(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-dr", "sk-dr", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-drain",
		AccessKey: "ak-dr",
		SecretKey: "sk-dr",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	// Deliberately long LeaseTTL (10s) to prove graceful Drain removes lease immediately without waiting for TTL.
	disc := cluster.DiscoveryConfig{
		Mode:              cluster.DiscoveryModeS3,
		HeartbeatInterval: 100 * time.Millisecond,
		LeaseTTL:          10 * time.Second,
		SyncInterval:      50 * time.Millisecond,
	}

	d1 := startDynamicNode(t, "data-1", []string{sharding.RoleData}, s3Client, disc)
	d2 := startDynamicNode(t, "data-2", []string{sharding.RoleData}, s3Client, disc)
	coord := startDynamicNode(t, "coord-1", []string{sharding.RoleCoordinator}, s3Client, disc)

	waitUntil(t, 3*time.Second, "drain test cluster discovered", func() bool {
		return len(coord.node.Ring().DataNodes()) == 2 &&
			len(d1.node.Ring().DataNodes()) == 2 &&
			len(d2.node.Ring().DataNodes()) == 2
	})

	if err := coord.node.CreateIndex(ctx, cluster.IndexMetadata{
		Name:     "drain_idx",
		Settings: cluster.IndexSettings{NumberOfShards: 1, NumberOfReplicas: 1},
	}, true); err != nil {
		t.Fatalf("create drain_idx: %v", err)
	}

	// Write 2 documents (kept in WAL, not explicitly flushed).
	if _, err := coord.node.IndexDocMeta(ctx, "drain_idx", "d1", []byte(`{"msg":"before drain 1"}`), "", ""); err != nil {
		t.Fatalf("index d1: %v", err)
	}
	if _, err := coord.node.IndexDocMeta(ctx, "drain_idx", "d2", []byte(`{"msg":"before drain 2"}`), "", ""); err != nil {
		t.Fatalf("index d2: %v", err)
	}

	placement, err := coord.node.Ring().PlacementForShard("drain_idx", 0, 1)
	if err != nil {
		t.Fatalf("placement: %v", err)
	}
	primaryH := d1
	survivorH := d2
	if placement.Primary.ID == "data-2" {
		primaryH = d2
		survivorH = d1
	}

	drainRes, err := primaryH.node.Drain(ctx)
	if err != nil {
		t.Fatalf("drain primary: %v", err)
	}
	if !drainRes.Draining || drainRes.ShardsFlushed < 1 || drainRes.SegmentsFlushed < 1 || !drainRes.LeaseRemoved {
		t.Fatalf("unexpected drain result: %+v", drainRes)
	}

	// Surviving node and coordinator promote & converge immediately (far faster than the 10s LeaseTTL).
	waitUntil(t, 2*time.Second, "immediate promotion after drain lease removal", func() bool {
		return survivorH.node.PrimaryTermForShard("drain_idx", 0) >= 2 &&
			len(coord.node.Ring().DataNodes()) == 1
	})

	// Verify both pre-drain WAL docs were flushed to S3 and are readable via the promoted primary.
	for _, id := range []string{"d1", "d2"} {
		got, err := coord.node.GetDocMeta(ctx, "drain_idx", id, "")
		if err != nil || !got.Found {
			t.Fatalf("expected %s persisted across graceful drain, got found=%v err=%v", id, got.Found, err)
		}
	}

	// Verify new write succeeds cleanly on promoted primary.
	res, err := coord.node.IndexDocMeta(ctx, "drain_idx", "d3", []byte(`{"msg":"after drain"}`), "", "")
	if err != nil || res.PrimaryTerm < 2 {
		t.Fatalf("expected post-drain write to succeed at term >= 2, res=%+v err=%v", res, err)
	}
}

func TestConcurrentMembershipFailoverAndWritesRaceSafe(t *testing.T) {
	ctx := context.Background()
	s3Srv := s3store.NewMockServer("ak-race", "sk-race", "us-east-1")
	defer s3Srv.Close()

	s3Client, err := s3store.NewClient(s3store.Config{
		Endpoint:  s3Srv.URL(),
		Region:    "us-east-1",
		Bucket:    "cindersearch-race-failover",
		AccessKey: "ak-race",
		SecretKey: "sk-race",
	})
	if err != nil {
		t.Fatalf("new s3 client: %v", err)
	}

	disc := cluster.DiscoveryConfig{
		Mode:              cluster.DiscoveryModeS3,
		HeartbeatInterval: 40 * time.Millisecond,
		LeaseTTL:          200 * time.Millisecond,
		SyncInterval:      40 * time.Millisecond,
	}

	d1 := startDynamicNode(t, "data-1", []string{sharding.RoleData}, s3Client, disc)
	d2 := startDynamicNode(t, "data-2", []string{sharding.RoleData}, s3Client, disc)
	d3 := startDynamicNode(t, "data-3", []string{sharding.RoleData}, s3Client, disc)
	coord := startDynamicNode(t, "coord-1", []string{sharding.RoleCoordinator}, s3Client, disc)

	waitUntil(t, 3*time.Second, "race test initial 4-node cluster", func() bool {
		return len(coord.node.Ring().DataNodes()) == 3 &&
			len(d1.node.Ring().DataNodes()) == 3 &&
			len(d2.node.Ring().DataNodes()) == 3 &&
			len(d3.node.Ring().DataNodes()) == 3
	})

	if err := coord.node.CreateIndex(ctx, cluster.IndexMetadata{
		Name:     "events",
		Settings: cluster.IndexSettings{NumberOfShards: 3, NumberOfReplicas: 1},
	}, true); err != nil {
		t.Fatalf("create events index: %v", err)
	}

	var stop atomic.Bool
	var writesSucceeded atomic.Int64
	var wg sync.WaitGroup

	// Concurrent writers
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; !stop.Load() && i < 40; i++ {
				id := fmt.Sprintf("w%d-d%d", worker, i)
				if _, err := coord.node.IndexDocMeta(ctx, "events", id, []byte(`{"kind":"race","ok":true}`), "", ""); err == nil {
					writesSucceeded.Add(1)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(w)
	}

	// Concurrent readers & health checks
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			_, _ = coord.node.Search(ctx, []string{"events"}, query.SearchRequest{
				Query: map[string]any{"match_all": map[string]any{}},
			}, cluster.RetrievalDropIn)
			_, _ = coord.node.ClusterHealth(nil, "indices")
			_ = coord.node.ClusterState(nil)
			time.Sleep(10 * time.Millisecond)
		}
	}()

	// Trigger graceful drain of data-3 + scale-out of data-4 concurrently with active traffic.
	time.Sleep(40 * time.Millisecond)
	_, _ = d3.node.Drain(ctx)
	d4 := startDynamicNode(t, "data-4", []string{sharding.RoleData}, s3Client, disc)

	waitUntil(t, 3*time.Second, "data-4 joined while data-3 drained", func() bool {
		nodes := coord.node.Ring().DataNodes()
		hasD4 := false
		hasD3 := false
		for _, nd := range nodes {
			if nd.ID == "data-4" {
				hasD4 = true
			}
			if nd.ID == "data-3" {
				hasD3 = true
			}
		}
		return hasD4 && !hasD3 && d4.node.HasIndexOrAlias("events")
	})

	stop.Store(true)
	wg.Wait()

	if writesSucceeded.Load() == 0 {
		t.Fatalf("expected successful writes during concurrent membership churn")
	}
}
