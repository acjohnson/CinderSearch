package query

import (
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	"github.com/cindersearch/cindersearch/internal/segment"
)

func TestElasticsearchQueryDSLAndAggregations(t *testing.T) {
	docs := []segment.InputDoc{
		{DocID: "1", Seq: 1, RawJSON: []byte(`{"service":"checkout","level":"error","latency_ms":412,"message":"s3 range timeout on shard 2","created_at":"2026-11-17T10:15:00Z","meta":{"region":"us-east","secret":"omit-me"}}`)},
		{DocID: "2", Seq: 2, RawJSON: []byte(`{"service":"checkout","level":"warn","latency_ms":140,"message":"nvme write queue spike","created_at":"2026-11-17T10:45:00Z","meta":{"region":"us-west","secret":"omit-me"}}`)},
		{DocID: "3", Seq: 3, RawJSON: []byte(`{"service":"search","level":"info","latency_ms":8,"message":"nvme cache hit for inverted index","created_at":"2026-11-17T11:05:00Z"}`)},
	}

	seg, _, _, err := segment.Build("seg-1", "logs", 0, docs, "k.idx", "k.docs")
	if err != nil {
		t.Fatalf("build segment: %v", err)
	}

	// 1. bool + match_phrase + numeric range filter.
	hits, err := EvaluateSegment(seg, map[string]any{
		"bool": map[string]any{
			"must": []any{
				map[string]any{"match_phrase": map[string]any{"message": "range timeout"}},
			},
			"filter": []any{
				map[string]any{"range": map[string]any{"latency_ms": map[string]any{"gte": float64(100)}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("eval bool+match_phrase: %v", err)
	}
	if len(hits) != 1 || hits[0].DocID != "1" {
		t.Fatalf("expected only doc 1, got %+v", hits)
	}

	// 2. Aggregations: terms + nested avg + top-level stats + date_histogram.
	allHits, err := EvaluateSegment(seg, map[string]any{"match_all": map[string]any{}})
	if err != nil {
		t.Fatalf("eval match_all: %v", err)
	}
	refs := make([]DocRef, 0, len(allHits))
	for _, h := range allHits {
		refs = append(refs, DocRef{Seg: seg, DocIdx: h.DocIdx, Score: h.Score})
	}

	aggs, err := ComputeAggregations(refs, map[string]any{
		"by_service": map[string]any{
			"terms": map[string]any{"field": "service"},
			"aggs": map[string]any{
				"avg_latency": map[string]any{"avg": map[string]any{"field": "latency_ms"}},
			},
		},
		"latency_stats": map[string]any{
			"stats": map[string]any{"field": "latency_ms"},
		},
		"hourly": map[string]any{
			"date_histogram": map[string]any{"field": "created_at", "fixed_interval": "1h"},
		},
	})
	if err != nil {
		t.Fatalf("compute aggs: %v", err)
	}
	statsMap := aggs["latency_stats"].(map[string]any)
	if statsMap["count"].(int) != 3 || statsMap["max"].(float64) != 412 {
		t.Fatalf("unexpected latency_stats: %+v", statsMap)
	}
}

func TestDSLAllQueryClausesAndOperators(t *testing.T) {
	docs := []segment.InputDoc{
		{DocID: "d1", Seq: 1, RawJSON: []byte(`{"service":"checkout-api","title":"S3 range fetch","message":"direct s3 byte range timeout","latency_ms":412,"created_at":"2026-11-17T10:15:00Z","tag":"critical"}`)},
		{DocID: "d2", Seq: 2, RawJSON: []byte(`{"service":"checkout-web","title":"NVMe flush","message":"nvme write queue spike detected","latency_ms":140,"created_at":"2026-11-17T10:45:00Z","tag":"warning"}`)},
		{DocID: "d3", Seq: 3, RawJSON: []byte(`{"service":"search-core","title":"Cache hit","message":"nvme cache hit for inverted index","latency_ms":8,"created_at":"2026-11-17T11:05:00Z"}`)},
	}

	seg, _, _, err := segment.Build("seg-dsl", "logs", 0, docs, "k.idx", "k.docs")
	if err != nil {
		t.Fatalf("build segment: %v", err)
	}

	assertIDs := func(name string, q map[string]any, want ...string) {
		t.Helper()
		hits, err := EvaluateSegment(seg, q)
		if err != nil {
			t.Fatalf("%s error: %v", name, err)
		}
		got := make([]string, 0, len(hits))
		for _, h := range hits {
			got = append(got, h.DocID)
		}
		sort.Strings(got)
		sort.Strings(want)
		if len(got) != len(want) {
			t.Fatalf("%s mismatch: got %v, want %v", name, got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("%s mismatch: got %v, want %v", name, got, want)
			}
		}
	}

	// match_none & constant_score
	assertIDs("match_none", map[string]any{"match_none": map[string]any{}})
	assertIDs("constant_score", map[string]any{
		"constant_score": map[string]any{
			"filter": map[string]any{"term": map[string]any{"service": "search-core"}},
			"boost":  3.5,
		},
	}, "d3")

	// match operator: and vs or
	assertIDs("match_and", map[string]any{
		"match": map[string]any{"message": map[string]any{"query": "nvme spike", "operator": "and"}},
	}, "d2")
	assertIDs("match_or", map[string]any{
		"match": map[string]any{"message": map[string]any{"query": "nvme spike", "operator": "or"}},
	}, "d2", "d3")

	// match_phrase with slop=0 vs slop=1 ("direct byte" skips "s3")
	assertIDs("phrase_slop_0", map[string]any{
		"match_phrase": map[string]any{"message": map[string]any{"query": "direct byte", "slop": 0}},
	})
	assertIDs("phrase_slop_1", map[string]any{
		"match_phrase": map[string]any{"message": map[string]any{"query": "direct byte", "slop": 1}},
	}, "d1")

	// multi_match with field wildcard & boost
	assertIDs("multi_match", map[string]any{
		"multi_match": map[string]any{
			"query":  "nvme",
			"fields": []any{"title^2", "mess*"},
			"type":   "most_fields",
		},
	}, "d2", "d3")

	// query_string & simple_query_string
	assertIDs("query_string", map[string]any{
		"query_string": map[string]any{"query": `+message:nvme -service:search-core`},
	}, "d2")
	assertIDs("simple_query_string_phrase", map[string]any{
		"simple_query_string": map[string]any{"query": `+"inverted index"`},
	}, "d3")

	// terms, prefix, wildcard, exists, ids, date range, bool should/must_not
	assertIDs("terms", map[string]any{
		"terms": map[string]any{"service": []any{"checkout-api", "search-core"}},
	}, "d1", "d3")
	assertIDs("prefix", map[string]any{"prefix": map[string]any{"service": "checkout-"}}, "d1", "d2")
	assertIDs("wildcard", map[string]any{"wildcard": map[string]any{"service": "*web"}}, "d2")
	assertIDs("exists", map[string]any{"exists": map[string]any{"field": "tag"}}, "d1", "d2")
	assertIDs("ids", map[string]any{"ids": map[string]any{"values": []any{"d1", "d3"}}}, "d1", "d3")
	assertIDs("date_range", map[string]any{
		"range": map[string]any{
			"created_at": map[string]any{
				"gte": "2026-11-17T10:30:00Z",
				"lt":  "2026-11-17T11:00:00Z",
			},
		},
	}, "d2")
	assertIDs("bool_should_msm", map[string]any{
		"bool": map[string]any{
			"should": []any{
				map[string]any{"prefix": map[string]any{"service": "checkout"}},
				map[string]any{"range": map[string]any{"latency_ms": map[string]any{"gt": float64(200)}}},
			},
			"minimum_should_match": 2,
		},
	}, "d1")
	assertIDs("bool_must_not", map[string]any{
		"bool": map[string]any{
			"must_not": []any{map[string]any{"prefix": map[string]any{"service": "checkout"}}},
		},
	}, "d3")
}

func TestDSLSortSourceFilterPaginationAndAllMetricAggs(t *testing.T) {
	docs := []segment.InputDoc{
		{DocID: "a", Seq: 1, RawJSON: []byte(`{"group":"g1","latency":10,"meta":{"region":"us","secret":"s1"}}`)},
		{DocID: "b", Seq: 2, RawJSON: []byte(`{"group":"g1","latency":50,"meta":{"region":"eu","secret":"s2"}}`)},
		{DocID: "c", Seq: 3, RawJSON: []byte(`{"group":"g2","latency":30,"meta":{"region":"ap","secret":"s3"}}`)},
	}
	seg, _, _, err := segment.Build("seg-agg", "metrics", 0, docs, "k.idx", "k.docs")
	if err != nil {
		t.Fatalf("build segment: %v", err)
	}

	// EffectiveSize honors explicit 0
	zero := 0
	if (SearchRequest{Size: &zero}).EffectiveSize() != 0 {
		t.Fatalf("expected EffectiveSize() == 0 when explicit 0")
	}
	if (SearchRequest{}).EffectiveSize() != 10 {
		t.Fatalf("expected default EffectiveSize() == 10")
	}

	// _source filtering (includes + excludes nested)
	spec := ParseSourceSpec(map[string]any{
		"includes": []any{"group", "meta.*"},
		"excludes": []any{"meta.secret"},
	})
	filtered := FilterSource(docs[0].RawJSON, spec)
	var obj map[string]any
	if err := json.Unmarshal(filtered, &obj); err != nil {
		t.Fatalf("unmarshal filtered: %v", err)
	}
	if obj["group"] != "g1" || obj["latency"] != nil {
		t.Fatalf("unexpected top-level filtered fields: %+v", obj)
	}
	meta := obj["meta"].(map[string]any)
	if meta["region"] != "us" || meta["secret"] != nil {
		t.Fatalf("unexpected nested filtered fields: %+v", meta)
	}

	if FilterSource(docs[0].RawJSON, ParseSourceSpec(false)) != nil {
		t.Fatalf("expected nil _source when _source=false")
	}

	// Sort by numeric field desc
	refs := []DocRef{
		{Seg: seg, DocIdx: 0, Score: 1},
		{Seg: seg, DocIdx: 1, Score: 1},
		{Seg: seg, DocIdx: 2, Score: 1},
	}
	sorts := ParseSortSpecs([]any{map[string]any{"latency": "desc"}})
	sort.Slice(refs, func(i, j int) bool {
		return CompareDocRefs(refs[i], refs[j], sorts) < 0
	})
	if seg.Docs[refs[0].DocIdx].DocID != "b" || seg.Docs[refs[2].DocIdx].DocID != "a" {
		t.Fatalf("unexpected latency desc order: %v %v %v", seg.Docs[refs[0].DocIdx].DocID, seg.Docs[refs[1].DocIdx].DocID, seg.Docs[refs[2].DocIdx].DocID)
	}

	// Metric + Histogram + Cardinality aggregations
	aggs, err := ComputeAggregations(refs, map[string]any{
		"hist": map[string]any{
			"histogram": map[string]any{"field": "latency", "interval": float64(25)},
			"aggs": map[string]any{
				"bucket_max": map[string]any{"max": map[string]any{"field": "latency"}},
			},
		},
		"min_lat":  map[string]any{"min": map[string]any{"field": "latency"}},
		"max_lat":  map[string]any{"max": map[string]any{"field": "latency"}},
		"sum_lat":  map[string]any{"sum": map[string]any{"field": "latency"}},
		"avg_lat":  map[string]any{"avg": map[string]any{"field": "latency"}},
		"cnt_lat":  map[string]any{"value_count": map[string]any{"field": "latency"}},
		"uniq_grp": map[string]any{"cardinality": map[string]any{"field": "group"}},
	})
	if err != nil {
		t.Fatalf("compute aggs: %v", err)
	}
	if aggs["min_lat"].(map[string]any)["value"].(float64) != 10 {
		t.Fatalf("min mismatch: %+v", aggs["min_lat"])
	}
	if aggs["max_lat"].(map[string]any)["value"].(float64) != 50 {
		t.Fatalf("max mismatch: %+v", aggs["max_lat"])
	}
	if aggs["sum_lat"].(map[string]any)["value"].(float64) != 90 {
		t.Fatalf("sum mismatch: %+v", aggs["sum_lat"])
	}
	if aggs["avg_lat"].(map[string]any)["value"].(float64) != 30 {
		t.Fatalf("avg mismatch: %+v", aggs["avg_lat"])
	}
	if aggs["cnt_lat"].(map[string]any)["value"].(int) != 3 {
		t.Fatalf("value_count mismatch: %+v", aggs["cnt_lat"])
	}
	if aggs["uniq_grp"].(map[string]any)["value"].(int) != 2 {
		t.Fatalf("cardinality mismatch: %+v", aggs["uniq_grp"])
	}
	histBuckets := aggs["hist"].(map[string]any)["buckets"].([]map[string]any)
	if len(histBuckets) != 3 {
		t.Fatalf("expected 3 histogram buckets (0, 25, 50), got %+v", histBuckets)
	}

	// Empty refs stats must return count=0 without panic
	emptyAggs, err := ComputeAggregations(nil, map[string]any{
		"s": map[string]any{"stats": map[string]any{"field": "latency"}},
	})
	if err != nil || emptyAggs["s"].(map[string]any)["count"].(int) != 0 {
		t.Fatalf("unexpected empty stats agg: %+v err=%v", emptyAggs, err)
	}
}

func buildBenchQuerySegment(b *testing.B, n int) *segment.Segment {
	b.Helper()
	services := []string{"checkout", "search", "payments", "inventory", "auth"}
	docs := make([]segment.InputDoc, n)
	for i := 0; i < n; i++ {
		raw := []byte(fmt.Sprintf(
			`{"service":%q,"level":"info","latency_ms":%d,"message":"s3 range nvme segment lookup worker %d","created_at":"2026-11-17T10:15:00Z"}`,
			services[i%len(services)], (i%500)+1, i%16,
		))
		docs[i] = segment.InputDoc{
			DocID:   fmt.Sprintf("doc-%d", i),
			Seq:     uint64(i + 1),
			RawJSON: raw,
		}
	}
	seg, _, _, err := segment.Build("seg-qbench", "logs", 0, docs, "logs/shard-0/seg-qbench.idx", "logs/shard-0/seg-qbench.docs")
	if err != nil {
		b.Fatalf("build query bench segment: %v", err)
	}
	return seg
}

func BenchmarkQueryMatchPhraseAndRange1000Docs(b *testing.B) {
	seg := buildBenchQuerySegment(b, 1000)
	q := map[string]any{
		"bool": map[string]any{
			"must": []any{
				map[string]any{"match_phrase": map[string]any{"message": "nvme segment"}},
			},
			"filter": []any{
				map[string]any{"range": map[string]any{"latency_ms": map[string]any{"gte": float64(100)}}},
			},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		hits, err := EvaluateSegment(seg, q)
		if err != nil || len(hits) == 0 {
			b.Fatalf("evaluate segment: err=%v hits=%d", err, len(hits))
		}
	}
}

func BenchmarkQueryTermsAndStatsAggs1000Docs(b *testing.B) {
	seg := buildBenchQuerySegment(b, 1000)
	hits, err := EvaluateSegment(seg, map[string]any{"match_all": map[string]any{}})
	if err != nil {
		b.Fatalf("evaluate match_all: %v", err)
	}
	refs := make([]DocRef, len(hits))
	for i, h := range hits {
		refs[i] = DocRef{Seg: seg, DocIdx: h.DocIdx, Score: h.Score}
	}
	aggsSpec := map[string]any{
		"by_service": map[string]any{
			"terms": map[string]any{"field": "service"},
			"aggs": map[string]any{
				"avg_latency": map[string]any{"avg": map[string]any{"field": "latency_ms"}},
			},
		},
		"latency_stats": map[string]any{
			"stats": map[string]any{"field": "latency_ms"},
		},
		"hourly": map[string]any{
			"date_histogram": map[string]any{"field": "created_at", "fixed_interval": "1h"},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := ComputeAggregations(refs, aggsSpec)
		if err != nil || len(out) == 0 {
			b.Fatalf("compute aggregations: err=%v len=%d", err, len(out))
		}
	}
}
