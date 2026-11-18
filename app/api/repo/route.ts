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
  { path: 'internal/cluster/node.go', category: 'Cluster & Sharding', summary: 'Cluster coordinator, async WAL replication loop, S3 flusher, scatter-gather' },
  { path: 'internal/api/es_server.go', category: 'Elasticsearch API', summary: 'Drop-in Elasticsearch / OpenSearch HTTP REST router and NDJSON _bulk parser' },
  { path: 'internal/cluster/cluster_test.go', category: 'Tests', summary: '3-node race integration test: _bulk, async WAL, zero-hop S3, bit-rot heal' },
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
