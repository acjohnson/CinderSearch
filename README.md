# CinderSearch (`cindersearch`)

**CinderSearch** is a concurrent, sharded full-text search and document engine in pure Go built as a drop-in **Elasticsearch / OpenSearch** API and Query DSL replacement, disaggregated onto **S3-compatible object storage** with **local NVMe SSD read/write caching**, hardware-accelerated **`CRC32C` checksumming**, **asynchronous WAL replication**, and **zero-hop presigned S3 byte-range document retrieval**.

## Architecture Highlights

- **Zero-Dependency Pure Go Core**: Written with the Go 1.22+ standard library (`net/http`, `hash/crc32`, `crypto/hmac`, `sync`), compiling in ~1 second with `make build`.
- **S3-First Authoritative Storage + NVMe Tiered Cache**:
  - **Write Path**: Incoming documents are `CRC32C`-checksummed on the fly, appended to a local NVMe Write-Ahead Log (WAL) for sub-millisecond acknowledgment, asynchronously shipped in batches to replica shards, and flushed in the background into immutable S3 segments.
  - **Read Path**: Inverted index terms, positional postings, and columnar DocValues are hydrated via HTTP `Range` requests from S3 and cached on local NVMe SSD (`LRU` eviction + background `CRC32C` bit-rot scrubber).
- **Zero-Hop Direct S3 Retrieval**:
  - Default mode returns standard Elasticsearch `_source` JSON for drop-in client compatibility.
  - Passing `?retrieval=presigned` (or `X-Cinder-Retrieval: presigned`) returns `_s3_pointer: {url, range, crc32c}` pointing straight at the document's exact byte slice inside the S3 segment object—eliminating DB proxy hops on document fetches.
- **Bit-Rot Protection**: Every WAL frame, replica stream, NVMe cache entry, S3 segment footer, and `_s3_pointer` carries a Castagnoli `CRC32C` digest. Corrupted NVMe cache entries are quarantined and automatically healed from S3.

## Quickstart

```bash
# Build bin/cindersearch
make build

# Run full unit & integration test suite (with -race)
make test

# Run the automated 3-node S3 + NVMe + Bit-Rot healing demo
make demo

# Start a single-node Elasticsearch-compatible server on :9200 with embedded S3
make run
```

## Drop-In Elasticsearch Usage

```bash
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

# Zero-Hop Direct S3 Retrieval (returns presigned Range URLs + CRC32C)
curl -X POST 'http://localhost:9200/logs/_search?retrieval=presigned' -H 'Content-Type: application/json' -d '{
  "query": { "match_all": {} }
}'
```
