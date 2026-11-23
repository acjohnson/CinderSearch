# CinderSearch (`cindersearch`)

**CinderSearch** is a concurrent, sharded full-text search and document engine in pure Go built as a drop-in **Elasticsearch / OpenSearch** API and Query DSL replacement, disaggregated onto **S3-compatible object storage** with **local NVMe SSD read/write caching**, hardware-accelerated **`CRC32C` checksumming**, **asynchronous WAL replication**, and **zero-hop presigned S3 byte-range document retrieval**.

## Architecture Highlights

- **Zero-Dependency Pure Go Core**: Written with the Go 1.22+ standard library (`net/http`, `hash/crc32`, `crypto/hmac`, `sync`), compiling in ~1 second with `make build`.
- **S3-First Authoritative Storage + NVMe Tiered Cache**:
  - **Write Path**: Incoming documents are `CRC32C`-checksummed on the fly, appended to a local NVMe Write-Ahead Log (WAL) for sub-millisecond acknowledgment, asynchronously shipped in batches to replica shards, and flushed in the background into immutable S3 segments.
  - **Read Path**: Inverted index terms, positional postings, and columnar DocValues are hydrated via HTTP `Range` requests from S3 and cached on local NVMe SSD (`LRU` eviction + background `CRC32C` bit-rot scrubber).
- **Zero-Hop Direct S3 Retrieval (Opt-In for ES Client Compatibility)**:
  - `retrieval=dropin` *(default)* returns standard Elasticsearch `_source` JSON for full compatibility with official Elasticsearch/OpenSearch clients.
  - Passing `?retrieval=presigned` (or `X-Cinder-Retrieval: presigned`) omits `_source` on S3-committed documents and returns `_s3_pointer: {url, range, crc32c}` pointing straight at the document's exact byte slice inside the S3 segment object—eliminating DB proxy hops on document fetches.
  - Passing `?retrieval=hybrid` returns `_s3_pointer` and also inlines `_source` only when hot in local NVMe cache.
- **Role-Based Cluster Topology (`data`, `coordinator`, `reader`)**:
  - `data` *(default)*: Owns shard primaries/replicas in the consistent hash ring, accepts NVMe WAL writes, replicates asynchronously, and flushes immutable segments to S3.
  - `coordinator`: Stateless HTTP router, Query DSL scatter-gather coordinator, and cross-shard aggregation/sort merger with zero local shards or WALs.
  - `reader`: Stateless S3 + NVMe read-cache search node (no WALs, no shard ownership) for horizontally scaling search QPS; supports `?preference=reader` / `X-Cinder-Preference: reader` routing while still forwarding `_doc` reads to `data` primaries for NRT consistency.
  - Unsupported legacy ES roles (`master`, `ml`, `ingest`, `data_hot`/`warm`/`cold`/`frozen`, `transform`, `remote_cluster_client`) are explicitly rejected—leaderless ring metadata + S3 tiering replace master-only and hot/warm/cold splits.
- **S3-Persisted Cluster Metadata & Cold-Boot Recovery**: Index settings, mappings, and aliases are persisted to `_cluster/indices/<name>.json` on S3 and automatically hydrated on node startup, enabling full cluster recovery even after total pod/NVMe loss.
- **Official ES v8 Client Handshake, Aliases & Wildcards**: Emits `X-Elastic-Product: Elasticsearch` and negotiates `application/vnd.elasticsearch+json; compatible-with=8`; supports `HEAD /{index}`, `HEAD /{index}/_doc/{id}`, `PUT /{index}/_create/{id}` / `?op_type=create` (`409 Conflict`), `GET/PUT /{index}/_mapping`, `GET/POST /_count`, atomic `POST /_aliases`, `/{index}/_alias/{name}`, and wildcard/exclusion index resolution (`logs-*,-logs-debug*`, `_all`).
- **Bit-Rot Protection**: Every WAL frame, replica stream, NVMe cache entry, S3 segment footer, and `_s3_pointer` carries a Castagnoli `CRC32C` digest. Corrupted NVMe cache entries are quarantined and automatically healed from S3.

## Quickstart

```bash
# Build bin/cindersearch
make build

# Run full unit & integration test suite (with -race)
make test

# Run Go fuzz tests across WAL, Query DSL, and _bulk NDJSON parsers
make fuzz

# Run build-tagged integration test against real AWS S3 or MinIO
CINDER_TEST_S3_ENDPOINT=http://127.0.0.1:9000 CINDER_TEST_S3_BUCKET=cindersearch \
  AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin make test-s3-integration

# Run the automated 3-node S3 + NVMe + Bit-Rot healing demo
make demo

# Start a single-node Elasticsearch-compatible server on :9200 with embedded S3
make run

# Profiling & Benchmarks (see BENCHMARKS.md for baselines)
make bench        # Go package microbenchmarks
make bench-live   # End-to-end _bulk, S3 flush, drop-in & presigned search benchmark
make profile-cpu  # Generate & inspect CPU pprof hotspots
make profile-mem  # Generate & inspect Heap alloc_space pprof hotspots
```

## Multi-Node Roles Topology

```bash
# Start a data node owning shards on :9201
./bin/cindersearch serve --node-id=data-1 --roles=data --listen=127.0.0.1:9201 \
  --peers=data-2=127.0.0.1:9202@data,coord-1=127.0.0.1:9200@coordinator,reader-1=127.0.0.1:9203@reader

# Start a stateless coordinator gateway on :9200 (0 shards, no WAL)
./bin/cindersearch serve --node-id=coord-1 --roles=coordinator --listen=127.0.0.1:9200 \
  --peers=data-1=127.0.0.1:9201@data,data-2=127.0.0.1:9202@data,reader-1=127.0.0.1:9203@reader

# Start a stateless S3+NVMe search reader on :9203 (0 WALs)
./bin/cindersearch serve --node-id=reader-1 --roles=reader --listen=127.0.0.1:9203 \
  --peers=data-1=127.0.0.1:9201@data,data-2=127.0.0.1:9202@data,coord-1=127.0.0.1:9200@coordinator
```

Nodes can hold multiple comma-separated roles (e.g., `--roles=data,reader` or `@data+reader` in `--peers`).

## Drop-In Elasticsearch Usage

Responses are pretty-printed JSON by default (matching Elasticsearch/OpenSearch); append `?pretty=false` for compact single-line JSON on high-throughput clients.

```bash
# Inspect cluster health, node roles, and _cat views
curl http://localhost:9200/
curl http://localhost:9200/_cluster/health
curl http://localhost:9200/_nodes
curl http://localhost:9200/_cat/nodes

# Create an index with 3 shards and 1 replica
curl -X PUT http://localhost:9200/logs -H 'Content-Type: application/json' -d '{
  "settings": { "number_of_shards": 3, "number_of_replicas": 1 }
}'

# Bulk ingest NDJSON documents
curl -X POST http://localhost:9200/logs/_bulk -H 'Content-Type: application/x-ndjson' --data-binary '
{"index":{"_id":"1"}}
{"service":"checkout","level":"error","latency_ms":412,"message":"s3 range timeout on shard 2"}
{"index":{"_id":"2"}}
{"service":"search","level":"info","latency_ms":8,"message":"nvme cache hit for inverted index"}
'

# Search using Elasticsearch Query DSL + Aggregations
curl -X POST http://localhost:9200/logs/_search -H 'Content-Type: application/json' -d '{
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
}'

# Route flushed search reads to stateless S3+NVMe reader nodes
curl -X POST 'http://localhost:9200/logs/_search?preference=reader' -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} }
}'

# Zero-Hop Direct S3 Retrieval (returns presigned Range URLs + CRC32C)
curl -X POST 'http://localhost:9200/logs/_search?retrieval=presigned' -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} }
}'
```

## Kubernetes Deployment (Helm Chart)

A production Helm chart lives in `charts/cindersearch` (with container image build via `Dockerfile`), supporting single-node dev, multi-node leaderless multi-master clusters, and disaggregated `data` + `coordinator` + `reader` pools with either a bundled open-source **MinIO** server (`standalone` or 4-node `distributed`) or an external **AWS S3 / R2 / Ceph RGW** bucket.

```bash
# Multi-node leaderless multi-master cluster + 4-node distributed MinIO
helm upgrade --install cindersearch ./charts/cindersearch \
  -n cindersearch --create-namespace \
  -f ./charts/cindersearch/examples/values-multimaster-minio.yaml

# Disaggregated (data + coordinator + reader) backed by real AWS S3
helm upgrade --install cindersearch ./charts/cindersearch \
  -n cindersearch --create-namespace \
  -f ./charts/cindersearch/examples/values-disaggregated-aws-s3.yaml

# Validate chart & run in-cluster smoke test
make helm-lint
helm test cindersearch -n cindersearch
```

See `charts/cindersearch/README.md` for full configuration details.
