import { NextResponse } from 'next/server';
import fs from 'node:fs/promises';
import path from 'node:path';

export interface RepoFileEntry {
  path: string;
  category: string;
  summary: string;
  lines: number;
  bytes: number;
  content: string;
}

const REPO_FILES: Array<{ path: string; category: string; summary: string }> = [
  { path: 'Makefile', category: 'Build & CLI', summary: 'Repeatable build, race-enabled test, bench, and 3-node demo targets' },
  { path: 'go.mod', category: 'Build & CLI', summary: 'Zero-external-dependency Go 1.22 module manifest' },
  { path: 'README.md', category: 'Build & CLI', summary: 'Quickstart, architecture overview, node roles, and ES curl examples' },
  { path: 'BENCHMARKS.md', category: 'Build & CLI', summary: 'Recorded microbenchmarks, live load baselines, and pprof bottlenecks' },
  { path: 'cmd/cindersearch/main.go', category: 'Build & CLI', summary: 'CLI entry point: serve (--roles, --pprof), demo, and bench profiling' },
  { path: 'internal/checksum/crc32c.go', category: 'Storage & Integrity', summary: 'Hardware-accelerated Castagnoli CRC32C matching S3 checksum headers' },
  { path: 'internal/sharding/ring.go', category: 'Cluster & Sharding', summary: 'Role-aware consistent hash ring distributing shards across data nodes' },
  { path: 'internal/wal/wal.go', category: 'Storage & Integrity', summary: 'CRC32C-framed NVMe Write-Ahead Log with safe tail truncation and compaction' },
  { path: 'internal/s3store/client.go', category: 'S3 & NVMe Tier', summary: 'Pure-Go S3 REST client, Range GETs, and AWS SigV4 presigned URL signer' },
  { path: 'internal/s3store/mock_server.go', category: 'S3 & NVMe Tier', summary: 'Embedded S3 HTTP server with 206 Range and SigV4 validation for local tests' },
  { path: 'internal/cache/nvme.go', category: 'S3 & NVMe Tier', summary: 'Bounded NVMe SSD LRU read cache with on-read & scrub bit-rot healing' },
  { path: 'internal/segment/segment.go', category: 'Search Engine', summary: 'Immutable segment builder: positional postings, DocValues, packed _source offsets' },
  { path: 'internal/query/dsl.go', category: 'Search Engine', summary: 'Elasticsearch Query DSL parser, BM25 scorer, sort, and bucket/metric aggs' },
  { path: 'internal/cluster/shard.go', category: 'Cluster & Sharding', summary: 'Shard engine unifying NVMe WAL buffer, NRT memtable, NVMe cache, and S3' },
  { path: 'internal/cluster/merge.go', category: 'Cluster & Sharding', summary: 'Lucene-style S3 TieredMergePolicy, deterministic tombstone GC, NVMe pre-warm & _forcemerge' },
  { path: 'internal/cluster/node.go', category: 'Cluster & Sharding', summary: 'Cluster coordinator, async WAL replication, S3 flusher, merge/GC loop & scatter-gather' },
  { path: 'internal/api/es_server.go', category: 'Elasticsearch API', summary: 'Drop-in Elasticsearch / OpenSearch HTTP REST router, _forcemerge, _segments & _bulk' },
  { path: 'internal/cluster/cluster_test.go', category: 'Tests', summary: '3-node race tests: cold-boot S3 recovery, fault injection, roles, zero-hop S3, bit-rot heal' },
  { path: 'internal/cluster/merge_test.go', category: 'Tests', summary: 'Tiered merge policy, anti-zombie tombstone GC, presigned URL grace window & race tests' },
  { path: 'internal/api/es_server_test.go', category: 'Tests', summary: 'ES v8 compat, HEAD, 409 create, _mapping, _count, _forcemerge, _segments, aliases & fuzz' },
  { path: 'cmd/cindersearch/main_test.go', category: 'Tests', summary: 'CLI --peers/@roles, --merge-* flag validation, percentile math & demo smoke tests' },
  { path: 'internal/s3store/s3_integration_test.go', category: 'Tests', summary: 'Build-tagged real AWS S3 / MinIO SigV4, CRC32C, and HTTP 206 Range integration test' },
  { path: 'Dockerfile', category: 'Kubernetes & Helm', summary: 'Multi-stage non-root Alpine container build for CinderSearch' },
  { path: 'charts/cindersearch/Chart.yaml', category: 'Kubernetes & Helm', summary: 'Helm v2 chart metadata for CinderSearch' },
  { path: 'charts/cindersearch/values.yaml', category: 'Kubernetes & Helm', summary: 'Complete Helm values: multi-master data, coordinator, reader, MinIO & external S3' },
  { path: 'charts/cindersearch/README.md', category: 'Kubernetes & Helm', summary: 'Kubernetes deployment guide for single, multi-master MinIO, and AWS S3 modes' },
];

export async function GET() {
  const rootDir = process.cwd();
  const entries: RepoFileEntry[] = [];

  for (const spec of REPO_FILES) {
    try {
      const fullPath = path.join(rootDir, spec.path);
      const content = await fs.readFile(fullPath, 'utf8');
      entries.push({
        path: spec.path,
        category: spec.category,
        summary: spec.summary,
        lines: content.split('\n').length,
        bytes: Buffer.byteLength(content, 'utf8'),
        content,
      });
    } catch {
      // Skip missing files gracefully
    }
  }

  return NextResponse.json({ files: entries });
}
