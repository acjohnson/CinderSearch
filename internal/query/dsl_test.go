package query

import (
	"testing"

	"github.com/cindersearch/cindersearch/internal/segment"
)

func TestElasticsearchQueryDSLAndAggregations(t *testing.T) {
	docs := []segment.InputDoc{
		{DocID: "1", Seq: 1, RawJSON: []byte(`{"service":"checkout","level":"error","latency_ms":412,"message":"s3 range timeout on shard 2","created_at":"2026-11-17T10:15:00Z"}`)},
		{DocID: "2", Seq: 2, RawJSON: []byte(`{"service":"checkout","level":"warn","latency_ms":140,"message":"nvme write queue spike","created_at":"2026-11-17T10:45:00Z"}`)},
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
