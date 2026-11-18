# CinderSearch Performance Baselines

Recorded baseline measurements captured before optimization passes so future changes can be compared apples-to-apples.

---

## Baseline #1 — Commit `ce7afc9` (2026-11-17)

- **Environment**: `linux/amd64`, `12th Gen Intel(R) Core(TM) i9-12900H` (`GOMAXPROCS=20`)
- **Commit / Build**: `ce7afc9`

### 1. Go Microbenchmarks (`make bench`)

```text
go test -run=^$ -bench=. -benchmem ./...
```

| Package | Benchmark | ns/op | Throughput | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: | ---: |
| `internal/api` | `BenchmarkESBulkIngestHTTP-20` | `980,910` | `13.27 MB/s` | `441,075` | `5,131` |
| `internal/api` | `BenchmarkESSearchDropInHTTP-20` | `7,048,807` | — | `11,055,910` | `55,784` |
| `internal/api` | `BenchmarkESSearchPresignedZeroHopHTTP-20` | `3,492,102` | — | `2,659,582` | `51,163` |
| `internal/cache` | `BenchmarkNVMeCachePut4KB-20` | `55,039` | `74.42 MB/s` | `1,385` | `19` |
| `internal/cache` | `BenchmarkNVMeCacheGetHit4KB-20` | `6,979` | `586.87 MB/s` | `5,368` | `6` |
| `internal/checksum` | `BenchmarkCRC32C1KB-20` | `54.81` | `18,681.40 MB/s` | `0` | `0` |
| `internal/query` | `BenchmarkQueryMatchPhraseAndRange1000Docs-20` | `714,404` | — | `676,392` | `1,093` |
| `internal/query` | `BenchmarkQueryTermsAndStatsAggs1000Docs-20` | `230,514` | — | `147,168` | `150` |
| `internal/segment` | `BenchmarkSegmentBuild100Docs-20` | `2,195,507` | `6.30 MB/s` | `1,344,581` | `19,594` |
| `internal/segment` | `BenchmarkSegmentUnmarshalIndex-20` | `1,235,425` | `43.50 MB/s` | `587,546` | `13,149` |
| `internal/wal` | `BenchmarkWALAppendBuffered-20` | `1,200` | `84.19 MB/s` | `846` | `3` |
| `internal/wal` | `BenchmarkWALAppendFsync-20` | `6,671,500` | `0.02 MB/s` | `627` | `3` |

### 2. End-to-End Load Benchmark (`make bench-live`: 5,000 docs, batch=250, workers=8, queries=1,000)

| Phase | Throughput | Latency (`p50` / `p95` / `p99`) |
| --- | --- | --- |
| `[1/4] _bulk Ingest` | `195,970 docs/sec` (`24.84 MB/sec`) | `8.58ms` / `12.45ms` / `12.45ms` |
| `[2/4] S3 Segment Flush` | `26,215 docs/sec` | `190.73ms` total |
| `[3/4] Search Drop-In (DSL+Aggs)` | `22 QPS` | `260.91ms` / `1163.38ms` / `1361.21ms` |
| `[4/4] Zero-Hop Presigned Search` | `90 QPS` | `47.61ms` / `257.05ms` / `296.82ms` |

### 3. Profile Runs (10,000 docs, batch=250, workers=8, queries=2,000)

#### `make profile-cpu` (`/tmp/cindersearch-cpu.pprof`)
- `_bulk Ingest`: `168,320 docs/sec` (`21.36 MB/sec`), batch latency `p50=10.69ms p95=14.66ms p99=15.54ms`
- `S3 Segment Flush`: `370.67ms` total (`26,978 docs/sec`)
- `Search Drop-In (DSL+Aggs)`: `4 QPS` (`p50=1831.87ms p95=2636.17ms p99=3475.47ms`)
- `Zero-Hop Presigned Search`: `52 QPS` (`p50=90.58ms p95=477.17ms p99=543.35ms`)
- CPU total samples: `7946.97s` across `486.32s` wall time:
  - `ExecuteLocalShardSearch`: `93.98%` cum (`7468.18s`)
  - `NVMeCache.Get` / `ReadDocSource` (`os.ReadFile`): `76.03%` cum (`6042.32s`)
  - `runtime.mallocgc`: `29.28%` cum (`2326.80s`)
  - `Shard.ActiveSegments` -> `segment.UnmarshalIndex` (`encoding/gob`): `16.60%` cum (`1319.19s`)

#### `make profile-mem` (`/tmp/cindersearch-mem.pprof`)
- `_bulk Ingest`: `200,990 docs/sec` (`25.50 MB/sec`), batch latency `p50=8.16ms p95=13.32ms p99=13.33ms`
- `S3 Segment Flush`: `376.93ms` total (`26,530 docs/sec`)
- `Search Drop-In (DSL+Aggs)`: `5 QPS` (`p50=1008.55ms p95=2749.42ms p99=3673.02ms`)
- `Zero-Hop Presigned Search`: `99 QPS` (`p50=78.85ms p95=102.28ms p99=115.04ms`)
- Total `alloc_space`: `5,527,013.35 MB` (`~5.53 TB`):
  - `os.readFileContents` (via `ReadDocSource` -> `NVMeCache.Get`): `5,326,060.83 MB` (`96.36%`)
  - `segment.UnmarshalIndex` (`encoding/gob`): `160,924.80 MB` (`2.91%`)

### Key Bottlenecks Identified from Baseline #1

1. **Per-hit full `.docs` file reads & eager pre-pagination `_source` loading** (`internal/cluster/node.go`, `internal/cluster/shard.go`, `internal/cache/nvme.go`):
   - `ExecuteLocalShardSearch` loads `_source` for **all** matched hits on every shard (e.g., ~8,000 docs) before coordinator top-`size` (10) truncation.
   - `Shard.ReadDocSource` calls `NVMeCache.Get(seg.DocsObjectKey)`, which executes `os.ReadFile` + full-segment CRC32C verification on the entire packed `.docs` file **for every matched hit** (~5.3 TB allocated across 2,000 queries).
   - `Shard.IsDocsCachedOnNVMe` also calls `NVMeCache.Get`, reading and discarding the entire `.docs` file just to check cache presence.
2. **Repeated `encoding/gob` deserialization on every query** (`Shard.ActiveSegments` -> `segment.UnmarshalIndex`):
   - Every query reads `.idx` from NVMe and runs `gob.Decode` on every segment instead of reusing cached parsed `*segment.Segment` instances in memory.
