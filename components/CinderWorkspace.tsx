'use client';

import React, { useMemo, useState } from 'react';
import {
  Check,
  Copy,
  Database,
  FileCode2,
  HardDrive,
  Search,
  ShieldAlert,
  ShieldCheck,
  Terminal,
} from 'lucide-react';
import type { RepoFileEntry } from '@/app/api/repo/route';

type WorkspaceView = 'quickstart' | 'source' | 'tracer';
type RetrievalMode = 'dropin' | 'presigned' | 'hybrid';

interface CinderWorkspaceProps {
  initialFiles: RepoFileEntry[];
}

const MAKE_TARGETS = [
  { target: 'make build', desc: 'Compile zero-dependency Go binary to bin/cindersearch' },
  { target: 'make test', desc: 'Run unit, cold-boot, fault-injection & 3-node tests with -race' },
  { target: 'make fuzz', desc: 'Run Go fuzz passes over WAL, Query DSL, and _bulk NDJSON' },
  { target: 'make test-s3-integration', desc: 'Run build-tagged live AWS S3 / MinIO SigV4 & Range test' },
  { target: 'make demo', desc: 'Boot embedded S3 + 3-node cluster, test zero-hop Range & bit-rot heal' },
  { target: 'make run', desc: 'Start single-node Elasticsearch-compatible server on :9200' },
  { target: 'make bench', desc: 'Run Go microbenchmarks for CRC32C, NVMe, segment, and query HTTP' },
  { target: 'make bench-live', desc: 'Run end-to-end ingest, flush, drop-in & presigned search benchmark' },
  { target: 'make profile-cpu', desc: 'Capture CPU pprof profile & print top cumulative hotspots' },
  { target: 'make vet', desc: 'Run static analysis across all Go packages' },
];

const ES_COMPAT_MATRIX = [
  {
    surface: 'Index, Mapping & Cluster REST',
    endpoints: 'GET /, HEAD/PUT/GET/DELETE /{index}, GET/PUT /{index}/_mapping, GET/POST /_count, GET /_cluster/health, /_nodes, /_cat/{nodes,indices,shards}, POST /{index}/_refresh',
    notes: 'X-Elastic-Product: Elasticsearch & ES v8 media-type negotiation, S3-persisted cold-boot cluster metadata (_cluster/indices/*.json)',
  },
  {
    surface: 'Aliases & Wildcard Patterns',
    endpoints: 'POST /_aliases, GET /_aliases, GET/HEAD /_alias/{name}, PUT/DELETE/GET/HEAD /{index}/_alias/{name}, logs-*,-logs-debug*, _all',
    notes: 'Atomic add/remove alias swaps persisted to S3 and expanded across _search, _count, _mapping, and _refresh',
  },
  {
    surface: 'Node Roles & Routing',
    endpoints: '--roles=data|coordinator|reader, --peers=id=host:port[@role1+role2], ?preference=reader (X-Cinder-Preference)',
    notes: 'Only data nodes own shard WALs; coordinators are stateless routers; readers serve flushed S3+NVMe queries with automatic failover',
  },
  {
    surface: 'Document CRUD & Bulk',
    endpoints: 'PUT/POST /{index}/_doc/{id}, PUT /{index}/_create/{id} (?op_type=create), HEAD/GET/DELETE /{index}/_doc/{id}, POST /{index}/_update/{id}, POST /_bulk',
    notes: '409 Conflict on duplicate create, strict JSON object validation, NDJSON bulk streaming, CRC32C digests & sub-ms NVMe WAL ACK',
  },
  {
    surface: 'Full-Text Query DSL',
    endpoints: 'match, match_phrase (slop), multi_match, query_string, simple_query_string',
    notes: 'Positional inverted index per field + BM25 scoring across segments',
  },
  {
    surface: 'Term, Range & Compound',
    endpoints: 'bool (must, should, must_not, filter, minimum_should_match), term, terms, range, prefix, wildcard, exists, ids, constant_score',
    notes: 'Evaluated over columnar DocValues (float64, ISO-8601 dates, keywords)',
  },
  {
    surface: 'Aggregations & Sorting',
    endpoints: 'terms, histogram, date_histogram, min, max, sum, avg, stats, value_count, cardinality, sort, _source filtering',
    notes: 'Global cross-shard merge with nested sub-aggregations and from/size pagination',
  },
  {
    surface: 'Segment Merging & S3 GC',
    endpoints: 'POST /_forcemerge, POST /{index}/_forcemerge (?max_num_segments, ?only_expunge_deletes), GET /{index}/_segments, GET /_cat/segments',
    notes: 'Lucene-style TieredMergePolicy (cap 128 MiB, concurrency=1), deterministic base-0 tombstone purge, NVMe pre-warm & 10m S3 GC grace window',
  },
  {
    surface: 'Zero-Hop S3 Retrieval',
    endpoints: '?retrieval=dropin | presigned | hybrid (or X-Cinder-Retrieval header)',
    notes: 'Drop-in _source by default; opt-in SigV4 Presigned URL + Range: bytes=start-end + CRC32C digest',
  },
];

const CURL_SNIPPET = `# 1. Create a sharded index
curl -X PUT http://localhost:9200/logs -H 'Content-Type: application/json' -d '{
  "settings": { "number_of_shards": 3, "number_of_replicas": 1 }
}'

# 2. Bulk ingest NDJSON documents (buffered in NVMe WAL, replicated asynchronously)
curl -X POST http://localhost:9200/logs/_bulk -H 'Content-Type: application/x-ndjson' --data-binary '
{"index":{"_id":"evt-1"}}
{"service":"checkout","level":"error","latency_ms":412,"message":"s3 range timeout on shard 2"}
{"index":{"_id":"evt-2"}}
{"service":"search","level":"info","latency_ms":8,"message":"nvme cache hit for inverted index"}
'

# 3. Flush WAL to immutable S3 segments & query with Zero-Hop Presigned Range URLs
curl -X POST 'http://localhost:9200/logs/_refresh?flush=true'
curl -X POST 'http://localhost:9200/logs/_search?retrieval=presigned' -H 'Content-Type: application/json' -d '{
  "query": { "bool": { "must": [{ "match_phrase": { "message": "range timeout" } }] } },
  "aggs": { "by_service": { "terms": { "field": "service" } }, "latency": { "stats": { "field": "latency_ms" } } }
}'`;

// Castagnoli CRC32C table matching Go's crc32.MakeTable(crc32.Castagnoli)
const CRC32C_TABLE = (() => {
  const table = new Uint32Array(256);
  const poly = 0x82f63b78;
  for (let i = 0; i < 256; i++) {
    let crc = i;
    for (let j = 0; j < 8; j++) {
      crc = (crc & 1) !== 0 ? (crc >>> 1) ^ poly : crc >>> 1;
    }
    table[i] = crc >>> 0;
  }
  return table;
})();

function crc32cHex(input: string): string {
  const bytes = new TextEncoder().encode(input);
  let crc = 0xffffffff;
  for (let i = 0; i < bytes.length; i++) {
    crc = CRC32C_TABLE[(crc ^ bytes[i]) & 0xff] ^ (crc >>> 8);
  }
  return ((crc ^ 0xffffffff) >>> 0).toString(16).padStart(8, '0');
}

// FNV-1a 64-bit matching Go's internal/sharding/ring.go ShardForKey
function fnv1aShard(indexName: string, docID: string, numShards: number): number {
  const bytes = new TextEncoder().encode(`${indexName}\u0000${docID}`);
  let hash = BigInt('0xcbf29ce484222325');
  const prime = BigInt('0x100000001b3');
  const mask = BigInt('0xffffffffffffffff');
  for (let i = 0; i < bytes.length; i++) {
    hash ^= BigInt(bytes[i]);
    hash = (hash * prime) & mask;
  }
  return Number(hash % BigInt(Math.max(1, numShards)));
}

export default function CinderWorkspace({ initialFiles }: CinderWorkspaceProps) {
  const [view, setView] = useState<WorkspaceView>('quickstart');
  const [selectedPath, setSelectedPath] = useState<string>(
    initialFiles[0]?.path ?? 'Makefile'
  );
  const [fileFilter, setFileFilter] = useState<string>('');
  const [copiedId, setCopiedId] = useState<string | null>(null);

  // Interactive Path Tracer state
  const [traceIndex, setTraceIndex] = useState<string>('logs');
  const [traceDocID, setTraceDocID] = useState<string>('evt-101');
  const [traceShards, setTraceShards] = useState<number>(3);
  const [tracePayload, setTracePayload] = useState<string>(
    '{"service":"checkout","level":"error","latency_ms":412,"message":"s3 range timeout on shard 2"}'
  );
  const [retrievalMode, setRetrievalMode] = useState<RetrievalMode>('presigned');
  const [nvmeHot, setNvmeHot] = useState<boolean>(true);
  const [simulateBitRot, setSimulateBitRot] = useState<boolean>(false);

  const selectedFile = useMemo(
    () => initialFiles.find((f) => f.path === selectedPath) ?? initialFiles[0],
    [initialFiles, selectedPath]
  );

  const filteredFiles = useMemo(() => {
    const q = fileFilter.trim().toLowerCase();
    if (!q) return initialFiles;
    return initialFiles.filter(
      (f) =>
        f.path.toLowerCase().includes(q) ||
        f.category.toLowerCase().includes(q) ||
        f.summary.toLowerCase().includes(q)
    );
  }, [initialFiles, fileFilter]);

  const groupedFiles = useMemo(() => {
    const groups = new Map<string, RepoFileEntry[]>();
    for (const f of filteredFiles) {
      const list = groups.get(f.category) ?? [];
      list.push(f);
      groups.set(f.category, list);
    }
    return Array.from(groups.entries());
  }, [filteredFiles]);

  const totalGoLines = useMemo(
    () => initialFiles.reduce((acc, f) => acc + f.lines, 0),
    [initialFiles]
  );

  const shardId = useMemo(
    () => fnv1aShard(traceIndex || 'logs', traceDocID || 'doc-1', traceShards),
    [traceIndex, traceDocID, traceShards]
  );

  const primaryNode = `node-${(shardId % 3) + 1}`;
  const replicaNode = `node-${((shardId + 1) % 3) + 1}`;
  const expectedCRC = useMemo(() => crc32cHex(tracePayload), [tracePayload]);
  const corruptedPayload = useMemo(() => {
    if (!tracePayload) return ' ';
    const ch = tracePayload[0] === '{' ? '[' : '{';
    return ch + tracePayload.slice(1);
  }, [tracePayload]);
  const observedCRC = useMemo(
    () => (simulateBitRot ? crc32cHex(corruptedPayload) : expectedCRC),
    [simulateBitRot, corruptedPayload, expectedCRC]
  );
  const payloadBytes = useMemo(
    () => new TextEncoder().encode(tracePayload).length,
    [tracePayload]
  );

  const handleCopy = async (id: string, text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopiedId(id);
      setTimeout(() => setCopiedId((cur) => (cur === id ? null : cur)), 1500);
    } catch {
      // Clipboard blocked in restricted context
    }
  };

  return (
    <div className="min-h-screen flex flex-col bg-slate-950 text-slate-100">
      {/* Top Bar Contract: 3 zones separated by gap-8 */}
      <header className="h-14 border-b border-slate-800 bg-slate-950 px-6 flex items-center justify-between gap-8 shrink-0">
        <span className="font-display text-lg font-semibold tracking-tight text-slate-50 whitespace-nowrap shrink-0">
          CinderSearch
        </span>

        <nav className="flex items-center gap-6 text-sm font-medium">
          <button
            type="button"
            onClick={() => setView('quickstart')}
            className={`whitespace-nowrap shrink-0 py-1 border-b-2 transition-colors ${
              view === 'quickstart'
                ? 'border-blue-500 text-slate-50'
                : 'border-transparent text-slate-400 hover:text-slate-100'
            }`}
          >
            Quickstart
          </button>
          <button
            type="button"
            onClick={() => setView('source')}
            className={`whitespace-nowrap shrink-0 py-1 border-b-2 transition-colors ${
              view === 'source'
                ? 'border-blue-500 text-slate-50'
                : 'border-transparent text-slate-400 hover:text-slate-100'
            }`}
          >
            Source Explorer
          </button>
          <button
            type="button"
            onClick={() => setView('tracer')}
            className={`whitespace-nowrap shrink-0 py-1 border-b-2 transition-colors ${
              view === 'tracer'
                ? 'border-blue-500 text-slate-50'
                : 'border-transparent text-slate-400 hover:text-slate-100'
            }`}
          >
            Path Tracer
          </button>
        </nav>

        <button
          type="button"
          onClick={() => handleCopy('header-make', 'make build && make test && make demo')}
          className="inline-flex items-center gap-2 px-4 py-2 text-xs font-medium rounded-md bg-blue-600 hover:bg-blue-500 text-white transition-colors whitespace-nowrap shrink-0 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-500"
        >
          {copiedId === 'header-make' ? <Check className="w-3.5 h-3.5" /> : <Copy className="w-3.5 h-3.5" />}
          <span>{copiedId === 'header-make' ? 'Copied command' : 'Copy make test'}</span>
        </button>
      </header>

      {/* Main Split Layout: 260px Sidebar + Content Viewport */}
      <div className="flex-1 flex min-h-0">
        <aside className="w-[260px] border-r border-slate-800 bg-slate-900/40 flex flex-col shrink-0">
          <div className="p-4 border-b border-slate-800">
            <div className="flex items-center justify-between text-xs text-slate-400 mb-2.5">
              <span className="font-medium text-slate-300">Repository Files</span>
              <span className="font-mono tabular-nums">
                {initialFiles.length} files · {totalGoLines.toLocaleString()} lines
              </span>
            </div>
            <div className="relative">
              <Search className="w-3.5 h-3.5 text-slate-500 absolute left-2.5 top-1/2 -translate-y-1/2" />
              <input
                type="text"
                value={fileFilter}
                onChange={(e) => setFileFilter(e.target.value)}
                placeholder="Filter Go packages..."
                aria-label="Filter Go packages"
                className="w-full bg-slate-950 border border-slate-800 rounded-md pl-8 pr-3 py-1.5 text-xs text-slate-200 placeholder:text-slate-500 focus:outline-none focus:border-blue-500"
              />
            </div>
          </div>

          <div className="flex-1 overflow-y-auto p-3 space-y-5">
            {groupedFiles.length === 0 ? (
              <p className="text-xs text-slate-500 px-2 py-4">No matching files.</p>
            ) : (
              groupedFiles.map(([category, items]) => (
                <div key={category}>
                  <div className="px-2 mb-1.5 text-xs font-medium text-slate-400">
                    {category}
                  </div>
                  <div className="space-y-0.5">
                    {items.map((item) => {
                      const active = view === 'source' && selectedPath === item.path;
                      return (
                        <button
                          key={item.path}
                          type="button"
                          onClick={() => {
                            setSelectedPath(item.path);
                            setView('source');
                          }}
                          className={`w-full text-left px-2.5 py-2 rounded-md text-xs transition-colors flex items-center justify-between gap-2 ${
                            active
                              ? 'bg-slate-800 text-slate-50'
                              : 'text-slate-300 hover:bg-slate-800/60 hover:text-slate-100'
                          }`}
                        >
                          <span className="font-mono truncate">{item.path}</span>
                          <span className="font-mono tabular-nums text-[11px] text-slate-500 shrink-0">
                            {item.lines}L
                          </span>
                        </button>
                      );
                    })}
                  </div>
                </div>
              ))
            )}
          </div>
        </aside>

        <main className="flex-1 overflow-y-auto px-8 py-7">
          <div className="max-w-[1160px] mx-auto space-y-8">
            {view === 'quickstart' && (
              <>
                <section className="flex flex-col lg:flex-row lg:items-end justify-between gap-6 pb-6 border-b border-slate-800">
                  <div className="max-w-2xl">
                    <div className="text-xs text-slate-400 mb-2">
                      Pure Go 1.22 · S3 Object Storage · Local NVMe Tiered Cache · Castagnoli CRC32C
                    </div>
                    <h1 className="font-display text-3xl font-semibold tracking-tight text-slate-50 text-balance">
                      Drop-in Elasticsearch replacement disaggregated onto S3 and NVMe
                    </h1>
                    <p className="mt-2.5 text-sm leading-relaxed text-slate-300">
                      Clone the repository and build the standalone <span className="font-mono text-slate-100">cindersearch</span> binary
                      with zero external Go dependencies. Buffered writes acknowledge from the NVMe WAL, replicate
                      asynchronously across shard peers, flush into immutable S3 segments, and serve zero-hop presigned
                      byte-range queries.
                    </p>
                  </div>
                  <div className="flex items-center gap-3 shrink-0">
                    <button
                      type="button"
                      onClick={() => setView('source')}
                      className="px-4 py-2 text-xs font-medium rounded-md border border-slate-700 bg-slate-900 hover:bg-slate-800 text-slate-200 transition-colors whitespace-nowrap"
                    >
                      Inspect Go Source
                    </button>
                    <button
                      type="button"
                      onClick={() => setView('tracer')}
                      className="px-4 py-2 text-xs font-medium rounded-md bg-blue-600 hover:bg-blue-500 text-white transition-colors whitespace-nowrap"
                    >
                      Trace Request Path
                    </button>
                  </div>
                </section>

                {/* Repeatable Makefile Targets */}
                <section className="space-y-3">
                  <div className="flex items-center justify-between">
                    <h2 className="font-display text-lg font-semibold text-slate-100">
                      Repeatable Makefile Workflow
                    </h2>
                    <span className="text-xs text-slate-400">
                      Local builds &amp; race-enabled integration suite
                    </span>
                  </div>
                  <div className="border border-slate-800 rounded-lg bg-slate-900/50 divide-y divide-slate-800">
                    {MAKE_TARGETS.map((item) => (
                      <div
                        key={item.target}
                        className="px-4 py-3 flex flex-col sm:flex-row sm:items-center justify-between gap-3 hover:bg-slate-900/80 transition-colors"
                      >
                        <div className="flex items-center gap-3 min-w-0">
                          <Terminal className="w-4 h-4 text-blue-400 shrink-0" />
                          <code className="font-mono text-xs font-medium text-slate-100 whitespace-nowrap">
                            {item.target}
                          </code>
                          <span className="text-xs text-slate-400 truncate">{item.desc}</span>
                        </div>
                        <button
                          type="button"
                          onClick={() => handleCopy(item.target, item.target)}
                          className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded border border-slate-700 bg-slate-950 hover:bg-slate-800 text-slate-300 transition-colors whitespace-nowrap shrink-0 self-start sm:self-auto"
                        >
                          {copiedId === item.target ? (
                            <Check className="w-3.5 h-3.5 text-emerald-400" />
                          ) : (
                            <Copy className="w-3.5 h-3.5" />
                          )}
                          <span>{copiedId === item.target ? 'Copied' : 'Copy'}</span>
                        </button>
                      </div>
                    ))}
                  </div>
                </section>

                {/* Drop-In ES Compatibility Matrix */}
                <section className="space-y-3">
                  <div className="flex items-center justify-between">
                    <h2 className="font-display text-lg font-semibold text-slate-100">
                      Elasticsearch / OpenSearch Compatibility Matrix
                    </h2>
                    <span className="text-xs text-slate-400">
                      Implemented in pure Go stdlib
                    </span>
                  </div>
                  <div className="border border-slate-800 rounded-lg bg-slate-900/50 overflow-hidden">
                    <table className="w-full text-left border-collapse">
                      <thead>
                        <tr className="border-b border-slate-800 text-xs font-medium text-slate-400 bg-slate-900/80">
                          <th className="py-2.5 px-4">Subsystem</th>
                          <th className="py-2.5 px-4">Supported Endpoints &amp; DSL</th>
                          <th className="py-2.5 px-4">Engine Implementation</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-800 text-xs">
                        {ES_COMPAT_MATRIX.map((row) => (
                          <tr key={row.surface} className="hover:bg-slate-900/60">
                            <td className="py-3 px-4 font-medium text-slate-100 whitespace-nowrap align-top">
                              {row.surface}
                            </td>
                            <td className="py-3 px-4 font-mono text-slate-300 align-top">
                              {row.endpoints}
                            </td>
                            <td className="py-3 px-4 text-slate-400 align-top">
                              {row.notes}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </section>

                {/* Quickstart Curl Examples */}
                <section className="space-y-3">
                  <div className="flex items-center justify-between">
                    <h2 className="font-display text-lg font-semibold text-slate-100">
                      Drop-In API &amp; Zero-Hop S3 Example
                    </h2>
                    <button
                      type="button"
                      onClick={() => handleCopy('curl-snippet', CURL_SNIPPET)}
                      className="inline-flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium rounded border border-slate-700 bg-slate-900 hover:bg-slate-800 text-slate-200 transition-colors"
                    >
                      {copiedId === 'curl-snippet' ? (
                        <Check className="w-3.5 h-3.5 text-emerald-400" />
                      ) : (
                        <Copy className="w-3.5 h-3.5" />
                      )}
                      <span>{copiedId === 'curl-snippet' ? 'Copied' : 'Copy curl commands'}</span>
                    </button>
                  </div>
                  <pre className="p-4 rounded-lg border border-slate-800 bg-slate-900/70 font-mono text-xs leading-relaxed text-slate-200 overflow-x-auto">
                    {CURL_SNIPPET}
                  </pre>
                </section>
              </>
            )}

            {view === 'source' && selectedFile && (
              <section className="space-y-4">
                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4 pb-4 border-b border-slate-800">
                  <div>
                    <div className="text-xs text-slate-400 mb-1">
                      {selectedFile.category} · <span className="font-mono tabular-nums">{selectedFile.lines} lines</span> ·{' '}
                      <span className="font-mono tabular-nums">{selectedFile.bytes.toLocaleString()} bytes</span>
                    </div>
                    <h1 className="font-display text-xl font-semibold text-slate-50 font-mono">
                      {selectedFile.path}
                    </h1>
                    <p className="text-xs text-slate-400 mt-1">{selectedFile.summary}</p>
                  </div>
                  <button
                    type="button"
                    onClick={() => handleCopy(`file-${selectedFile.path}`, selectedFile.content)}
                    className="inline-flex items-center gap-2 px-4 py-2 text-xs font-medium rounded-md border border-slate-700 bg-slate-900 hover:bg-slate-800 text-slate-100 transition-colors shrink-0 self-start"
                  >
                    {copiedId === `file-${selectedFile.path}` ? (
                      <Check className="w-3.5 h-3.5 text-emerald-400" />
                    ) : (
                      <Copy className="w-3.5 h-3.5" />
                    )}
                    <span>{copiedId === `file-${selectedFile.path}` ? 'Copied file' : 'Copy source'}</span>
                  </button>
                </div>

                <div className="border border-slate-800 rounded-lg bg-slate-900/60 overflow-hidden">
                  <div className="px-4 py-2 border-b border-slate-800 bg-slate-900 flex items-center gap-2 text-xs text-slate-400">
                    <FileCode2 className="w-3.5 h-3.5 text-blue-400" />
                    <span className="font-mono">{selectedFile.path}</span>
                  </div>
                  <pre className="p-4 font-mono text-xs leading-relaxed text-slate-200 overflow-x-auto max-h-[680px]">
                    {selectedFile.content}
                  </pre>
                </div>
              </section>
            )}

            {view === 'tracer' && (
              <section className="space-y-6">
                <div className="pb-4 border-b border-slate-800">
                  <h1 className="font-display text-2xl font-semibold text-slate-50">
                    Interactive Shard, CRC32C &amp; Zero-Hop S3 Path Tracer
                  </h1>
                  <p className="mt-1 text-sm text-slate-400">
                    Computes the exact FNV-1a consistent hash shard and Castagnoli CRC32C digest used by CinderSearch&apos;s
                    Go engine across the NVMe WAL, async replica stream, S3 segment, and zero-hop Range fetch.
                  </p>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-12 gap-6">
                  <div className="lg:col-span-5 border border-slate-800 rounded-lg bg-slate-900/50 p-5 space-y-4">
                    <h2 className="font-display text-sm font-semibold text-slate-100">
                      Document &amp; Query Parameters
                    </h2>

                    <div className="grid grid-cols-2 gap-3">
                      <div>
                        <label className="block text-xs text-slate-400 mb-1">Index Name</label>
                        <input
                          type="text"
                          value={traceIndex}
                          onChange={(e) => setTraceIndex(e.target.value)}
                          className="w-full bg-slate-950 border border-slate-800 rounded px-3 py-1.5 text-xs font-mono text-slate-100 focus:outline-none focus:border-blue-500"
                        />
                      </div>
                      <div>
                        <label className="block text-xs text-slate-400 mb-1">Document _id</label>
                        <input
                          type="text"
                          value={traceDocID}
                          onChange={(e) => setTraceDocID(e.target.value)}
                          className="w-full bg-slate-950 border border-slate-800 rounded px-3 py-1.5 text-xs font-mono text-slate-100 focus:outline-none focus:border-blue-500"
                        />
                      </div>
                    </div>

                    <div>
                      <label className="block text-xs text-slate-400 mb-1">
                        Primary Shards ({traceShards})
                      </label>
                      <input
                        type="range"
                        min={1}
                        max={12}
                        value={traceShards}
                        onChange={(e) => setTraceShards(Number(e.target.value))}
                        className="w-full accent-blue-500"
                      />
                    </div>

                    <div>
                      <label className="block text-xs text-slate-400 mb-1">JSON _source Payload</label>
                      <textarea
                        rows={4}
                        value={tracePayload}
                        onChange={(e) => setTracePayload(e.target.value)}
                        className="w-full bg-slate-950 border border-slate-800 rounded p-2.5 text-xs font-mono text-slate-100 focus:outline-none focus:border-blue-500"
                      />
                    </div>

                    <div>
                      <label className="block text-xs text-slate-400 mb-1.5">Retrieval Mode</label>
                      <div className="grid grid-cols-3 gap-1 p-1 bg-slate-950 border border-slate-800 rounded-md">
                        {(['dropin', 'presigned', 'hybrid'] as const).map((m) => (
                          <button
                            key={m}
                            type="button"
                            onClick={() => setRetrievalMode(m)}
                            className={`py-1.5 px-2 text-xs font-medium rounded transition-colors ${
                              retrievalMode === m
                                ? 'bg-blue-600 text-white'
                                : 'text-slate-400 hover:text-slate-200'
                            }`}
                          >
                            {m}
                          </button>
                        ))}
                      </div>
                    </div>

                    <div className="flex flex-col gap-2 pt-2 border-t border-slate-800">
                      <label className="flex items-center justify-between text-xs text-slate-300 cursor-pointer">
                        <span>Segment hot in local NVMe LRU cache</span>
                        <input
                          type="checkbox"
                          checked={nvmeHot}
                          onChange={(e) => setNvmeHot(e.target.checked)}
                          className="accent-blue-500"
                        />
                      </label>
                      <label className="flex items-center justify-between text-xs text-slate-300 cursor-pointer">
                        <span>Inject 1-bit corruption in NVMe cache</span>
                        <input
                          type="checkbox"
                          checked={simulateBitRot}
                          onChange={(e) => setSimulateBitRot(e.target.checked)}
                          className="accent-red-500"
                        />
                      </label>
                    </div>
                  </div>

                  <div className="lg:col-span-7 space-y-4">
                    {/* Write & Replication Trace */}
                    <div className="border border-slate-800 rounded-lg bg-slate-900/50 p-5 space-y-3">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2 text-sm font-semibold text-slate-100">
                          <HardDrive className="w-4 h-4 text-blue-400" />
                          <span>1. Write Path, Sharding &amp; Async Replication</span>
                        </div>
                        <span className="font-mono tabular-nums text-xs text-emerald-400">
                          CRC32C 0x{expectedCRC}
                        </span>
                      </div>
                      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 text-xs">
                        <div className="p-3 rounded border border-slate-800 bg-slate-950">
                          <div className="text-slate-400 mb-1">FNV-1a Shard Route</div>
                          <div className="font-mono text-slate-100 tabular-nums">
                            {traceIndex || 'logs'}[shard-{shardId}]
                          </div>
                          <div className="text-slate-500 mt-1">Primary: {primaryNode}</div>
                        </div>
                        <div className="p-3 rounded border border-slate-800 bg-slate-950">
                          <div className="text-slate-400 mb-1">NVMe WAL Commit</div>
                          <div className="font-mono text-emerald-400 tabular-nums">
                            ACK &lt;1ms ({payloadBytes} B)
                          </div>
                          <div className="text-slate-500 mt-1">Async ship -&gt; {replicaNode}</div>
                        </div>
                        <div className="p-3 rounded border border-slate-800 bg-slate-950">
                          <div className="text-slate-400 mb-1">S3 Segment Target</div>
                          <div className="font-mono text-slate-100 truncate">
                            shard-{shardId}/seg-000001.docs
                          </div>
                          <div className="text-slate-500 mt-1">Range: bytes=0-{Math.max(0, payloadBytes - 1)}</div>
                        </div>
                      </div>
                    </div>

                    {/* Bit-Rot Integrity Check */}
                    <div className="border border-slate-800 rounded-lg bg-slate-900/50 p-5 space-y-2.5">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2 text-sm font-semibold text-slate-100">
                          {simulateBitRot ? (
                            <ShieldAlert className="w-4 h-4 text-red-400" />
                          ) : (
                            <ShieldCheck className="w-4 h-4 text-emerald-400" />
                          )}
                          <span>2. NVMe Integrity &amp; Bit-Rot Scrubber</span>
                        </div>
                        <span
                          className={`text-xs font-medium ${
                            simulateBitRot ? 'text-red-400' : 'text-emerald-400'
                          }`}
                        >
                          {simulateBitRot ? 'Checksum Mismatch — Healing from S3' : 'Verified Intact'}
                        </span>
                      </div>
                      <p className="text-xs text-slate-400 leading-relaxed">
                        {simulateBitRot ? (
                          <>
                            NVMe file returned digest <span className="font-mono text-red-400">0x{observedCRC}</span>,
                            failing comparison against manifest digest{' '}
                            <span className="font-mono text-emerald-400">0x{expectedCRC}</span>. CinderSearch purges the
                            corrupted NVMe entry and re-hydrates clean bytes from S3 before serving.
                          </>
                        ) : (
                          <>
                            On-read and scrubber verification match{' '}
                            <span className="font-mono text-emerald-400">0x{observedCRC}</span>.{' '}
                            {nvmeHot
                              ? 'Index postings and document block served directly from local NVMe cache.'
                              : 'NVMe cache miss; hydrating verified segment slice from S3.'}
                          </>
                        )}
                      </p>
                    </div>

                    {/* Simulated Search Envelope */}
                    <div className="border border-slate-800 rounded-lg bg-slate-900/50 p-5 space-y-3">
                      <div className="flex items-center justify-between">
                        <div className="flex items-center gap-2 text-sm font-semibold text-slate-100">
                          <Database className="w-4 h-4 text-blue-400" />
                          <span>3. Returned Elasticsearch Response ({retrievalMode} mode)</span>
                        </div>
                        <span className="text-xs text-slate-400">
                          {retrievalMode === 'presigned'
                            ? '0 DB proxy hops for _source'
                            : retrievalMode === 'hybrid' && nvmeHot
                              ? 'NVMe inline hit + S3 pointer'
                              : 'Standard drop-in ES JSON'}
                        </span>
                      </div>
                      <pre className="p-3.5 rounded border border-slate-800 bg-slate-950 font-mono text-xs text-slate-200 overflow-x-auto">
                        {JSON.stringify(
                          {
                            _index: traceIndex || 'logs',
                            _id: traceDocID || 'evt-101',
                            _shard: shardId,
                            _score: 1.4285,
                            _crc32c: expectedCRC,
                            ...(retrievalMode !== 'dropin'
                              ? {
                                  _s3_pointer: {
                                    url: `https://s3.us-east-1.amazonaws.com/cindersearch/${traceIndex || 'logs'}/shard-${shardId}/seg-000001.docs?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Expires=300`,
                                    range: `bytes=0-${Math.max(0, payloadBytes - 1)}`,
                                    crc32c: expectedCRC,
                                    nvme_cached: nvmeHot && !simulateBitRot,
                                  },
                                }
                              : {}),
                            ...(retrievalMode === 'dropin' || (retrievalMode === 'hybrid' && nvmeHot && !simulateBitRot)
                              ? {
                                  _source: (() => {
                                    try {
                                      return JSON.parse(tracePayload);
                                    } catch {
                                      return tracePayload;
                                    }
                                  })(),
                                }
                              : {}),
                          },
                          null,
                          2
                        )}
                      </pre>
                    </div>
                  </div>
                </div>
              </section>
            )}
          </div>
        </main>
      </div>
    </div>
  );
}
