package query

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cindersearch/cindersearch/internal/segment"
)

// SourceSpec controls _source inclusion/exclusion on returned Elasticsearch hits.
type SourceSpec struct {
	Enabled  bool
	Includes []string
	Excludes []string
}

// SortSpec defines a single Elasticsearch sort field and direction.
type SortSpec struct {
	Field string
	Desc  bool
}

// SearchRequest represents a parsed Elasticsearch _search JSON payload.
type SearchRequest struct {
	Query        map[string]any         `json:"query"`
	From         int                    `json:"from"`
	Size         *int                   `json:"size"`
	Sort         any                    `json:"sort"`
	Source       any                    `json:"_source"`
	Aggs         map[string]any         `json:"aggs"`
	Aggregations map[string]any         `json:"aggregations"`
	TrackTotal   any                    `json:"track_total_hits"`
}

// EffectiveSize returns the requested hit count (defaulting to 10 like Elasticsearch,
// while honoring explicit size: 0 for aggregation-only queries).
func (r SearchRequest) EffectiveSize() int {
	if r.Size == nil {
		return 10
	}
	if *r.Size < 0 {
		return 0
	}
	return *r.Size
}

// MergedAggs combines "aggs" and "aggregations" aliases just like Elasticsearch.
func (r SearchRequest) MergedAggs() map[string]any {
	if len(r.Aggs) > 0 {
		return r.Aggs
	}
	return r.Aggregations
}

// ParseSourceSpec normalizes Elasticsearch's polymorphic _source parameter.
func ParseSourceSpec(raw any) SourceSpec {
	spec := SourceSpec{Enabled: true}
	if raw == nil {
		return spec
	}
	switch v := raw.(type) {
	case bool:
		spec.Enabled = v
	case string:
		spec.Includes = []string{v}
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok {
				spec.Includes = append(spec.Includes, s)
			}
		}
	case map[string]any:
		if inc, ok := v["includes"]; ok {
			spec.Includes = toStringSlice(inc)
		}
		if exc, ok := v["excludes"]; ok {
			spec.Excludes = toStringSlice(exc)
		}
	}
	return spec
}

// ParseSortSpecs normalizes Elasticsearch's polymorphic sort clause.
func ParseSortSpecs(raw any) []SortSpec {
	if raw == nil {
		return []SortSpec{{Field: "_score", Desc: true}}
	}
	var specs []SortSpec
	parseItem := func(item any) {
		switch v := item.(type) {
		case string:
			if v == "_score" {
				specs = append(specs, SortSpec{Field: "_score", Desc: true})
			} else {
				specs = append(specs, SortSpec{Field: v, Desc: false})
			}
		case map[string]any:
			for field, cfg := range v {
				desc := field == "_score"
				switch c := cfg.(type) {
				case string:
					desc = strings.EqualFold(c, "desc")
				case map[string]any:
					if ord, ok := c["order"].(string); ok {
						desc = strings.EqualFold(ord, "desc")
					}
				}
				specs = append(specs, SortSpec{Field: field, Desc: desc})
			}
		}
	}

	switch v := raw.(type) {
	case []any:
		for _, item := range v {
			parseItem(item)
		}
	default:
		parseItem(v)
	}
	if len(specs) == 0 {
		specs = append(specs, SortSpec{Field: "_score", Desc: true})
	}
	return specs
}

// FilterSource applies _source includes/excludes wildcards to a raw JSON document.
func FilterSource(raw []byte, spec SourceSpec) json.RawMessage {
	if !spec.Enabled {
		return nil
	}
	if len(spec.Includes) == 0 && len(spec.Excludes) == 0 {
		return json.RawMessage(raw)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return json.RawMessage(raw)
	}
	filtered := filterMap("", doc, spec)
	out, err := json.Marshal(filtered)
	if err != nil {
		return json.RawMessage(raw)
	}
	return json.RawMessage(out)
}

func filterMap(prefix string, m map[string]any, spec SourceSpec) map[string]any {
	res := make(map[string]any)
	for k, v := range m {
		full := k
		if prefix != "" {
			full = prefix + "." + k
		}
		if matchesAnyWildcard(full, spec.Excludes) {
			continue
		}
		if len(spec.Includes) > 0 && !matchesAnyWildcardOrParent(full, spec.Includes) {
			continue
		}
		if sub, ok := v.(map[string]any); ok {
			filteredSub := filterMap(full, sub, spec)
			if len(filteredSub) > 0 {
				res[k] = filteredSub
			}
		} else {
			res[k] = v
		}
	}
	return res
}

func matchesAnyWildcard(path string, patterns []string) bool {
	for _, p := range patterns {
		if wildcardMatch(p, path) {
			return true
		}
	}
	return false
}

func matchesAnyWildcardOrParent(path string, patterns []string) bool {
	for _, p := range patterns {
		if wildcardMatch(p, path) || strings.HasPrefix(p, path+".") {
			return true
		}
	}
	return false
}

// SegmentHit represents a matched document inside a Segment prior to cluster merge.
type SegmentHit struct {
	DocIdx uint32
	DocID  string
	Score  float64
}

// EvaluateSegment runs an Elasticsearch Query DSL AST against a single Segment,
// returning scored hits and (optionally) segment-local matched DocIndices for aggs.
func EvaluateSegment(seg *segment.Segment, queryMap map[string]any) ([]SegmentHit, error) {
	if seg == nil || seg.LiveDocs == 0 {
		return nil, nil
	}
	if len(queryMap) == 0 {
		queryMap = map[string]any{"match_all": map[string]any{}}
	}

	scores, err := evalNode(seg, queryMap)
	if err != nil {
		return nil, err
	}

	hits := make([]SegmentHit, 0, len(scores))
	for docIdx, score := range scores {
		if int(docIdx) >= len(seg.Docs) {
			continue
		}
		meta := seg.Docs[docIdx]
		if meta.Deleted {
			continue
		}
		hits = append(hits, SegmentHit{
			DocIdx: docIdx,
			DocID:  meta.DocID,
			Score:  score,
		})
	}
	return hits, nil
}

func evalNode(seg *segment.Segment, node map[string]any) (map[uint32]float64, error) {
	for key, val := range node {
		switch key {
		case "match_all":
			boost := extractBoost(val, 1.0)
			res := make(map[uint32]float64, len(seg.Docs))
			for i := range seg.Docs {
				if !seg.Docs[i].Deleted {
					res[uint32(i)] = boost
				}
			}
			return res, nil

		case "match_none":
			return map[uint32]float64{}, nil

		case "bool":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("bool query must be an object")
			}
			return evalBool(seg, cfg)

		case "constant_score":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("constant_score query must be an object")
			}
			boost := extractBoost(cfg, 1.0)
			filterNode, ok := cfg["filter"].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("constant_score requires filter object")
			}
			m, err := evalNode(seg, filterNode)
			if err != nil {
				return nil, err
			}
			for id := range m {
				m[id] = boost
			}
			return m, nil

		case "match":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("match query must be an object")
			}
			return evalMatch(seg, cfg)

		case "match_phrase":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("match_phrase query must be an object")
			}
			return evalMatchPhrase(seg, cfg)

		case "multi_match":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("multi_match query must be an object")
			}
			return evalMultiMatch(seg, cfg)

		case "query_string", "simple_query_string":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("%s query must be an object", key)
			}
			return evalQueryString(seg, cfg)

		case "term":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("term query must be an object")
			}
			return evalTerm(seg, cfg)

		case "terms":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("terms query must be an object")
			}
			return evalTerms(seg, cfg)

		case "range":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("range query must be an object")
			}
			return evalRange(seg, cfg)

		case "prefix":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("prefix query must be an object")
			}
			return evalPrefix(seg, cfg)

		case "wildcard":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("wildcard query must be an object")
			}
			return evalWildcard(seg, cfg)

		case "exists":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("exists query must be an object")
			}
			return evalExists(seg, cfg)

		case "ids":
			cfg, ok := val.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("ids query must be an object")
			}
			return evalIDs(seg, cfg)

		default:
			return nil, fmt.Errorf("unsupported query clause %q", key)
		}
	}
	return map[uint32]float64{}, nil
}

func evalBool(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	mustClauses := toClauseSlice(cfg["must"])
	filterClauses := toClauseSlice(cfg["filter"])
	shouldClauses := toClauseSlice(cfg["should"])
	mustNotClauses := toClauseSlice(cfg["must_not"])

	hasPositive := len(mustClauses) > 0 || len(filterClauses) > 0
	minShould := 0
	if v, ok := toInt(cfg["minimum_should_match"]); ok {
		minShould = v
	} else if !hasPositive && len(shouldClauses) > 0 {
		minShould = 1
	}

	var candidates map[uint32]float64
	initWith := func(m map[uint32]float64, addScore bool) {
		if candidates == nil {
			candidates = make(map[uint32]float64, len(m))
			for id, s := range m {
				if addScore {
					candidates[id] = s
				} else {
					candidates[id] = 0
				}
			}
			return
		}
		for id := range candidates {
			s, ok := m[id]
			if !ok {
				delete(candidates, id)
			} else if addScore {
				candidates[id] += s
			}
		}
	}

	for _, c := range mustClauses {
		m, err := evalNode(seg, c)
		if err != nil {
			return nil, err
		}
		initWith(m, true)
	}
	for _, c := range filterClauses {
		m, err := evalNode(seg, c)
		if err != nil {
			return nil, err
		}
		initWith(m, false)
	}

	if candidates == nil && len(shouldClauses) > 0 {
		// Pure should (or only must_not + should): collect union first.
		candidates = make(map[uint32]float64)
		shouldMatchCount := make(map[uint32]int)
		for _, c := range shouldClauses {
			m, err := evalNode(seg, c)
			if err != nil {
				return nil, err
			}
			for id, s := range m {
				candidates[id] += s
				shouldMatchCount[id]++
			}
		}
		for id := range candidates {
			if shouldMatchCount[id] < minShould {
				delete(candidates, id)
			}
		}
	} else {
		if candidates == nil {
			// Only must_not (or empty bool): start from all live docs with score 1.0.
			candidates = make(map[uint32]float64, len(seg.Docs))
			for i := range seg.Docs {
				if !seg.Docs[i].Deleted {
					candidates[uint32(i)] = 1.0
				}
			}
		}
		if len(shouldClauses) > 0 {
			shouldMatchCount := make(map[uint32]int)
			for _, c := range shouldClauses {
				m, err := evalNode(seg, c)
				if err != nil {
					return nil, err
				}
				for id, s := range m {
					if _, exists := candidates[id]; exists {
						candidates[id] += s
						shouldMatchCount[id]++
					}
				}
			}
			if minShould > 0 {
				for id := range candidates {
					if shouldMatchCount[id] < minShould {
						delete(candidates, id)
					}
				}
			}
		}
	}

	for _, c := range mustNotClauses {
		m, err := evalNode(seg, c)
		if err != nil {
			return nil, err
		}
		for id := range m {
			delete(candidates, id)
		}
	}

	// Ensure filter-only bool queries have non-zero score (1.0 * boost) like ES constant_score.
	boost := extractBoost(cfg, 1.0)
	if len(mustClauses) == 0 && len(shouldClauses) == 0 {
		for id := range candidates {
			candidates[id] = 1.0 * boost
		}
	} else if boost != 1.0 {
		for id := range candidates {
			candidates[id] *= boost
		}
	}
	return candidates, nil
}

func evalMatch(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	for field, raw := range cfg {
		var queryText string
		op := "or"
		boost := 1.0
		minShould := 0

		switch v := raw.(type) {
		case string:
			queryText = v
		case float64:
			queryText = strconv.FormatFloat(v, 'f', -1, 64)
		case bool:
			queryText = strconv.FormatBool(v)
		case map[string]any:
			queryText = fmt.Sprint(v["query"])
			if o, ok := v["operator"].(string); ok {
				op = strings.ToLower(o)
			}
			boost = extractBoost(v, 1.0)
			if msm, ok := toInt(v["minimum_should_match"]); ok {
				minShould = msm
			}
		default:
			return nil, fmt.Errorf("invalid match value for field %s", field)
		}

		tokens := segment.Analyze(queryText)
		if len(tokens) == 0 {
			return map[uint32]float64{}, nil
		}
		if op == "and" {
			minShould = len(tokens)
		} else if minShould <= 0 {
			minShould = 1
		}

		scores := make(map[uint32]float64)
		counts := make(map[uint32]int)
		for _, tok := range tokens {
			postings := seg.Postings[segment.TermKey(field, tok)]
			for _, p := range postings {
				scores[p.DocIdx] += bm25(seg, field, p.DocIdx, p.Freq, len(postings)) * boost
				counts[p.DocIdx]++
			}
		}
		for docIdx, cnt := range counts {
			if cnt < minShould {
				delete(scores, docIdx)
			}
		}
		return scores, nil
	}
	return map[uint32]float64{}, nil
}

func evalMatchPhrase(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	for field, raw := range cfg {
		var queryText string
		slop := 0
		boost := 1.0
		switch v := raw.(type) {
		case string:
			queryText = v
		case map[string]any:
			queryText = fmt.Sprint(v["query"])
			if s, ok := toInt(v["slop"]); ok && s >= 0 {
				slop = s
			}
			boost = extractBoost(v, 1.0)
		default:
			return nil, fmt.Errorf("invalid match_phrase value for field %s", field)
		}

		tokens := segment.Analyze(queryText)
		if len(tokens) == 0 {
			return map[uint32]float64{}, nil
		}
		if len(tokens) == 1 {
			return evalMatch(seg, map[string]any{field: tokens[0]})
		}

		termPostings := make([]map[uint32]segment.Posting, len(tokens))
		for i, tok := range tokens {
			m := make(map[uint32]segment.Posting)
			for _, p := range seg.Postings[segment.TermKey(field, tok)] {
				m[p.DocIdx] = p
			}
			termPostings[i] = m
		}

		scores := make(map[uint32]float64)
		for docIdx, firstPosting := range termPostings[0] {
			allPresent := true
			posLists := make([][]int, len(tokens))
			posLists[0] = firstPosting.Positions
			for i := 1; i < len(tokens); i++ {
				p, ok := termPostings[i][docIdx]
				if !ok {
					allPresent = false
					break
				}
				posLists[i] = p.Positions
			}
			if !allPresent {
				continue
			}
			matches := countPhraseMatches(posLists, slop)
			if matches > 0 {
				scores[docIdx] = bm25(seg, field, docIdx, matches, len(termPostings[0])) * boost * float64(len(tokens))
			}
		}
		return scores, nil
	}
	return map[uint32]float64{}, nil
}

func countPhraseMatches(posLists [][]int, slop int) int {
	matches := 0
	for _, startPos := range posLists[0] {
		if matchChain(posLists, 1, startPos, slop) {
			matches++
		}
	}
	return matches
}

func matchChain(posLists [][]int, idx int, prevPos int, remainingSlop int) bool {
	if idx >= len(posLists) {
		return true
	}
	for _, pos := range posLists[idx] {
		diff := pos - prevPos - 1
		if diff >= 0 && diff <= remainingSlop {
			if matchChain(posLists, idx+1, pos, remainingSlop-diff) {
				return true
			}
		}
	}
	return false
}

func evalMultiMatch(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	queryText := fmt.Sprint(cfg["query"])
	if queryText == "" || cfg["query"] == nil {
		return map[uint32]float64{}, nil
	}
	matchType := "best_fields"
	if t, ok := cfg["type"].(string); ok {
		matchType = strings.ToLower(t)
	}

	rawFields := toStringSlice(cfg["fields"])
	if len(rawFields) == 0 {
		rawFields = []string{"_all"}
	}

	type fieldBoost struct {
		name  string
		boost float64
	}
	var resolved []fieldBoost
	for _, rf := range rawFields {
		name := rf
		boost := 1.0
		if idx := strings.IndexByte(rf, '^'); idx > 0 {
			name = rf[:idx]
			if b, err := strconv.ParseFloat(rf[idx+1:], 64); err == nil {
				boost = b
			}
		}
		if strings.ContainsAny(name, "*?") {
			for f := range seg.FieldLengths {
				if f == "_all" {
					continue
				}
				if wildcardMatch(name, f) {
					resolved = append(resolved, fieldBoost{name: f, boost: boost})
				}
			}
		} else {
			resolved = append(resolved, fieldBoost{name: name, boost: boost})
		}
	}

	out := make(map[uint32]float64)
	for _, fb := range resolved {
		sub, err := evalMatch(seg, map[string]any{
			fb.name: map[string]any{"query": queryText, "boost": fb.boost},
		})
		if err != nil {
			return nil, err
		}
		for docIdx, s := range sub {
			if matchType == "most_fields" {
				out[docIdx] += s
			} else if s > out[docIdx] {
				out[docIdx] = s
			}
		}
	}
	return out, nil
}

func evalQueryString(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	q, _ := cfg["query"].(string)
	defaultField := "_all"
	if df, ok := cfg["default_field"].(string); ok && df != "" {
		defaultField = df
	}
	q = strings.TrimSpace(q)
	if q == "" || q == "*" || q == "*:*" {
		return evalNode(seg, map[string]any{"match_all": map[string]any{}})
	}

	parts := splitQueryStringParts(q)
	var must, should, mustNot []any
	for _, part := range parts {
		target := &should
		if strings.HasPrefix(part, "+") {
			target = &must
			part = part[1:]
		} else if strings.HasPrefix(part, "-") {
			target = &mustNot
			part = part[1:]
		}

		field := defaultField
		val := part
		if colon := strings.IndexByte(part, ':'); colon > 0 && !strings.HasPrefix(part, `"`) {
			field = part[:colon]
			val = part[colon+1:]
		}

		if strings.HasPrefix(val, `"`) && strings.HasSuffix(val, `"`) && len(val) >= 2 {
			phrase := val[1 : len(val)-1]
			*target = append(*target, map[string]any{"match_phrase": map[string]any{field: phrase}})
		} else if strings.ContainsAny(val, "*?") {
			*target = append(*target, map[string]any{"wildcard": map[string]any{field: strings.ToLower(val)}})
		} else {
			*target = append(*target, map[string]any{"match": map[string]any{field: val}})
		}
	}

	boolCfg := map[string]any{}
	if len(must) > 0 {
		boolCfg["must"] = must
	}
	if len(should) > 0 {
		boolCfg["should"] = should
	}
	if len(mustNot) > 0 {
		boolCfg["must_not"] = mustNot
	}
	return evalBool(seg, boolCfg)
}

func splitQueryStringParts(q string) []string {
	var parts []string
	var cur strings.Builder
	inQuote := false
	for i := 0; i < len(q); i++ {
		ch := q[i]
		if ch == '"' {
			inQuote = !inQuote
			cur.WriteByte(ch)
		} else if ch == ' ' && !inQuote {
			if cur.Len() > 0 {
				parts = append(parts, cur.String())
				cur.Reset()
			}
		} else {
			cur.WriteByte(ch)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}

func evalTerm(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	for field, raw := range cfg {
		target := raw
		boost := 1.0
		if m, ok := raw.(map[string]any); ok {
			target = m["value"]
			boost = extractBoost(m, 1.0)
		}
		targetStr := valueToString(target)
		res := make(map[uint32]float64)
		for i, dv := range seg.DocVals {
			if seg.Docs[i].Deleted {
				continue
			}
			for _, kw := range dv.Keywords[field] {
				if kw == targetStr {
					res[uint32(i)] = 1.0 * boost
					break
				}
			}
		}
		return res, nil
	}
	return map[uint32]float64{}, nil
}

func evalTerms(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	for field, raw := range cfg {
		if field == "boost" {
			continue
		}
		arr, ok := raw.([]any)
		if !ok {
			return nil, fmt.Errorf("terms query requires an array for field %s", field)
		}
		want := make(map[string]struct{}, len(arr))
		for _, v := range arr {
			want[valueToString(v)] = struct{}{}
		}
		res := make(map[uint32]float64)
		for i, dv := range seg.DocVals {
			if seg.Docs[i].Deleted {
				continue
			}
			for _, kw := range dv.Keywords[field] {
				if _, ok := want[kw]; ok {
					res[uint32(i)] = 1.0
					break
				}
			}
		}
		return res, nil
	}
	return map[uint32]float64{}, nil
}

func evalRange(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	for field, raw := range cfg {
		bounds, ok := raw.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("range query requires bounds object for field %s", field)
		}
		boost := extractBoost(bounds, 1.0)

		gt, hasGT, isDateGT := parseRangeBound(bounds["gt"])
		gte, hasGTE, isDateGTE := parseRangeBound(bounds["gte"])
		lt, hasLT, isDateLT := parseRangeBound(bounds["lt"])
		lte, hasLTE, isDateLTE := parseRangeBound(bounds["lte"])
		isDate := isDateGT || isDateGTE || isDateLT || isDateLTE

		res := make(map[uint32]float64)
		for i, dv := range seg.DocVals {
			if seg.Docs[i].Deleted {
				continue
			}
			var val float64
			var okVal bool
			if isDate {
				if ts, ok := dv.Dates[field]; ok {
					val = float64(ts)
					okVal = true
				}
			} else if num, ok := dv.Numbers[field]; ok {
				val = num
				okVal = true
			} else if ts, ok := dv.Dates[field]; ok {
				val = float64(ts)
				okVal = true
			}
			if !okVal {
				continue
			}
			if hasGT && !(val > gt) {
				continue
			}
			if hasGTE && !(val >= gte) {
				continue
			}
			if hasLT && !(val < lt) {
				continue
			}
			if hasLTE && !(val <= lte) {
				continue
			}
			res[uint32(i)] = 1.0 * boost
		}
		return res, nil
	}
	return map[uint32]float64{}, nil
}

func evalPrefix(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	for field, raw := range cfg {
		prefix := valueToString(raw)
		boost := 1.0
		if m, ok := raw.(map[string]any); ok {
			prefix = valueToString(m["value"])
			boost = extractBoost(m, 1.0)
		}
		res := make(map[uint32]float64)
		for i, dv := range seg.DocVals {
			if seg.Docs[i].Deleted {
				continue
			}
			for _, kw := range dv.Keywords[field] {
				if strings.HasPrefix(kw, prefix) {
					res[uint32(i)] = 1.0 * boost
					break
				}
			}
		}
		return res, nil
	}
	return map[uint32]float64{}, nil
}

func evalWildcard(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	for field, raw := range cfg {
		pattern := valueToString(raw)
		boost := 1.0
		if m, ok := raw.(map[string]any); ok {
			pattern = valueToString(m["value"])
			boost = extractBoost(m, 1.0)
		}
		res := make(map[uint32]float64)
		for i, dv := range seg.DocVals {
			if seg.Docs[i].Deleted {
				continue
			}
			for _, kw := range dv.Keywords[field] {
				if wildcardMatch(pattern, kw) {
					res[uint32(i)] = 1.0 * boost
					break
				}
			}
		}
		return res, nil
	}
	return map[uint32]float64{}, nil
}

func evalExists(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	field, _ := cfg["field"].(string)
	res := make(map[uint32]float64)
	for i, dv := range seg.DocVals {
		if seg.Docs[i].Deleted {
			continue
		}
		if _, ok := dv.Present[field]; ok {
			res[uint32(i)] = 1.0
		}
	}
	return res, nil
}

func evalIDs(seg *segment.Segment, cfg map[string]any) (map[uint32]float64, error) {
	vals := toStringSlice(cfg["values"])
	res := make(map[uint32]float64)
	for _, id := range vals {
		if idx, ok := seg.DocIDToIdx[id]; ok && !seg.Docs[idx].Deleted {
			res[idx] = 1.0
		}
	}
	return res, nil
}

func bm25(seg *segment.Segment, field string, docIdx uint32, tf int, df int) float64 {
	const k1 = 1.2
	const b = 0.75
	n := float64(seg.LiveDocs)
	if n <= 0 {
		n = 1
	}
	idf := math.Log(1.0 + (n-float64(df)+0.5)/(float64(df)+0.5))

	docLen := 1.0
	if lens, ok := seg.FieldLengths[field]; ok && int(docIdx) < len(lens) && lens[docIdx] > 0 {
		docLen = float64(lens[docIdx])
	}
	avgLen := seg.AvgFieldLen[field]
	if avgLen <= 0 {
		avgLen = 1.0
	}
	tfFloat := float64(tf)
	denom := tfFloat + k1*(1.0-b+b*(docLen/avgLen))
	if denom == 0 {
		return 0
	}
	return idf * ((tfFloat * (k1 + 1.0)) / denom)
}

// DocRef links a matched document across shards to its Segment DocValues for global aggregations and sorting.
type DocRef struct {
	Seg    *segment.Segment
	DocIdx uint32
	Score  float64
}

// ComputeAggregations evaluates Elasticsearch bucket and metric aggregations across all matched DocRefs.
func ComputeAggregations(docs []DocRef, aggsSpec map[string]any) (map[string]any, error) {
	if len(aggsSpec) == 0 {
		return nil, nil
	}
	result := make(map[string]any, len(aggsSpec))
	for name, rawDef := range aggsSpec {
		defMap, ok := rawDef.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("aggregation %q must be an object", name)
		}
		subAggs, _ := defMap["aggs"].(map[string]any)
		if len(subAggs) == 0 {
			subAggs, _ = defMap["aggregations"].(map[string]any)
		}

		var aggOut any
		var err error
		if cfg, ok := defMap["terms"].(map[string]any); ok {
			aggOut, err = evalTermsAgg(docs, cfg, subAggs)
		} else if cfg, ok := defMap["histogram"].(map[string]any); ok {
			aggOut, err = evalHistogramAgg(docs, cfg, subAggs)
		} else if cfg, ok := defMap["date_histogram"].(map[string]any); ok {
			aggOut, err = evalDateHistogramAgg(docs, cfg, subAggs)
		} else if cfg, ok := defMap["stats"].(map[string]any); ok {
			aggOut = evalStatsAgg(docs, cfg)
		} else if cfg, ok := defMap["min"].(map[string]any); ok {
			aggOut = evalSingleMetricAgg(docs, cfg, "min")
		} else if cfg, ok := defMap["max"].(map[string]any); ok {
			aggOut = evalSingleMetricAgg(docs, cfg, "max")
		} else if cfg, ok := defMap["sum"].(map[string]any); ok {
			aggOut = evalSingleMetricAgg(docs, cfg, "sum")
		} else if cfg, ok := defMap["avg"].(map[string]any); ok {
			aggOut = evalSingleMetricAgg(docs, cfg, "avg")
		} else if cfg, ok := defMap["value_count"].(map[string]any); ok {
			aggOut = evalSingleMetricAgg(docs, cfg, "count")
		} else if cfg, ok := defMap["cardinality"].(map[string]any); ok {
			aggOut = evalCardinalityAgg(docs, cfg)
		} else {
			return nil, fmt.Errorf("unsupported or malformed aggregation definition in %q", name)
		}
		if err != nil {
			return nil, err
		}
		result[name] = aggOut
	}
	return result, nil
}

func evalTermsAgg(docs []DocRef, cfg, subAggs map[string]any) (map[string]any, error) {
	field, _ := cfg["field"].(string)
	size := 10
	if s, ok := toInt(cfg["size"]); ok && s > 0 {
		size = s
	}
	groups := make(map[string][]DocRef)
	for _, ref := range docs {
		dv := ref.Seg.DocVals[ref.DocIdx]
		for _, kw := range dv.Keywords[field] {
			groups[kw] = append(groups[kw], ref)
		}
	}

	type bucketItem struct {
		key  string
		refs []DocRef
	}
	items := make([]bucketItem, 0, len(groups))
	for k, refs := range groups {
		items = append(items, bucketItem{key: k, refs: refs})
	}
	sort.Slice(items, func(i, j int) bool {
		if len(items[i].refs) == len(items[j].refs) {
			return items[i].key < items[j].key
		}
		return len(items[i].refs) > len(items[j].refs)
	})
	if len(items) > size {
		items = items[:size]
	}

	buckets := make([]map[string]any, 0, len(items))
	for _, item := range items {
		b := map[string]any{
			"key":       item.key,
			"doc_count": len(item.refs),
		}
		if len(subAggs) > 0 {
			subOut, err := ComputeAggregations(item.refs, subAggs)
			if err != nil {
				return nil, err
			}
			for k, v := range subOut {
				b[k] = v
			}
		}
		buckets = append(buckets, b)
	}
	return map[string]any{
		"doc_count_error_upper_bound": 0,
		"sum_other_doc_count":         0,
		"buckets":                     buckets,
	}, nil
}

func evalHistogramAgg(docs []DocRef, cfg, subAggs map[string]any) (map[string]any, error) {
	field, _ := cfg["field"].(string)
	interval := 10.0
	if iv, ok := toFloat(cfg["interval"]); ok && iv > 0 {
		interval = iv
	}
	groups := make(map[float64][]DocRef)
	for _, ref := range docs {
		dv := ref.Seg.DocVals[ref.DocIdx]
		if v, ok := dv.Numbers[field]; ok {
			key := math.Floor(v/interval) * interval
			groups[key] = append(groups[key], ref)
		}
	}
	keys := make([]float64, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Float64s(keys)

	buckets := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		b := map[string]any{
			"key":       k,
			"doc_count": len(groups[k]),
		}
		if len(subAggs) > 0 {
			subOut, err := ComputeAggregations(groups[k], subAggs)
			if err != nil {
				return nil, err
			}
			for sk, sv := range subOut {
				b[sk] = sv
			}
		}
		buckets = append(buckets, b)
	}
	return map[string]any{"buckets": buckets}, nil
}

func evalDateHistogramAgg(docs []DocRef, cfg, subAggs map[string]any) (map[string]any, error) {
	field, _ := cfg["field"].(string)
	intervalStr, _ := cfg["fixed_interval"].(string)
	if intervalStr == "" {
		intervalStr, _ = cfg["calendar_interval"].(string)
	}
	intervalMs := parseIntervalMillis(intervalStr)
	groups := make(map[int64][]DocRef)
	for _, ref := range docs {
		dv := ref.Seg.DocVals[ref.DocIdx]
		if ts, ok := dv.Dates[field]; ok {
			bucketKey := (ts / intervalMs) * intervalMs
			groups[bucketKey] = append(groups[bucketKey], ref)
		}
	}
	keys := make([]int64, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })

	buckets := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		b := map[string]any{
			"key_as_string": time.UnixMilli(k).UTC().Format(time.RFC3339),
			"key":           k,
			"doc_count":     len(groups[k]),
		}
		if len(subAggs) > 0 {
			subOut, err := ComputeAggregations(groups[k], subAggs)
			if err != nil {
				return nil, err
			}
			for sk, sv := range subOut {
				b[sk] = sv
			}
		}
		buckets = append(buckets, b)
	}
	return map[string]any{"buckets": buckets}, nil
}

func evalStatsAgg(docs []DocRef, cfg map[string]any) map[string]any {
	field, _ := cfg["field"].(string)
	var count int
	var sum float64
	var minVal, maxVal float64
	for _, ref := range docs {
		dv := ref.Seg.DocVals[ref.DocIdx]
		if v, ok := dv.Numbers[field]; ok {
			if count == 0 {
				minVal = v
				maxVal = v
			} else {
				if v < minVal {
					minVal = v
				}
				if v > maxVal {
					maxVal = v
				}
			}
			sum += v
			count++
		}
	}
	if count == 0 {
		return map[string]any{"count": 0, "min": nil, "max": nil, "avg": nil, "sum": 0.0}
	}
	return map[string]any{
		"count": count,
		"min":   minVal,
		"max":   maxVal,
		"avg":   sum / float64(count),
		"sum":   sum,
	}
}

func evalSingleMetricAgg(docs []DocRef, cfg map[string]any, kind string) map[string]any {
	stats := evalStatsAgg(docs, cfg)
	if kind == "count" {
		return map[string]any{"value": stats["count"]}
	}
	return map[string]any{"value": stats[kind]}
}

func evalCardinalityAgg(docs []DocRef, cfg map[string]any) map[string]any {
	field, _ := cfg["field"].(string)
	seen := make(map[string]struct{})
	for _, ref := range docs {
		dv := ref.Seg.DocVals[ref.DocIdx]
		for _, kw := range dv.Keywords[field] {
			seen[kw] = struct{}{}
		}
	}
	return map[string]any{"value": len(seen)}
}

// CompareDocRefs compares two matched documents according to Elasticsearch sort specifications.
func CompareDocRefs(a, b DocRef, sorts []SortSpec) int {
	for _, sp := range sorts {
		var cmp int
		if sp.Field == "_score" {
			switch {
			case a.Score < b.Score:
				cmp = -1
			case a.Score > b.Score:
				cmp = 1
			}
		} else {
			dvA := a.Seg.DocVals[a.DocIdx]
			dvB := b.Seg.DocVals[b.DocIdx]
			numA, okNumA := dvA.Numbers[sp.Field]
			numB, okNumB := dvB.Numbers[sp.Field]
			if okNumA && okNumB {
				switch {
				case numA < numB:
					cmp = -1
				case numA > numB:
					cmp = 1
				}
			} else if dA, okDA := dvA.Dates[sp.Field]; okDA {
				if dB, okDB := dvB.Dates[sp.Field]; okDB {
					switch {
					case dA < dB:
						cmp = -1
					case dA > dB:
						cmp = 1
					}
				}
			} else {
				var sA, sB string
				if kws := dvA.Keywords[sp.Field]; len(kws) > 0 {
					sA = kws[0]
				}
				if kws := dvB.Keywords[sp.Field]; len(kws) > 0 {
					sB = kws[0]
				}
				cmp = strings.Compare(sA, sB)
			}
		}
		if cmp != 0 {
			if sp.Desc {
				return -cmp
			}
			return cmp
		}
	}
	return strings.Compare(a.Seg.Docs[a.DocIdx].DocID, b.Seg.Docs[b.DocIdx].DocID)
}

func extractBoost(v any, def float64) float64 {
	if m, ok := v.(map[string]any); ok {
		if b, ok := toFloat(m["boost"]); ok {
			return b
		}
	}
	return def
}

func toClauseSlice(raw any) []map[string]any {
	if raw == nil {
		return nil
	}
	switch v := raw.(type) {
	case map[string]any:
		return []map[string]any{v}
	case []any:
		out := make([]map[string]any, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				out = append(out, m)
			}
		}
		return out
	}
	return nil
}

func toStringSlice(raw any) []string {
	switch v := raw.(type) {
	case string:
		return []string{v}
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			out = append(out, valueToString(item))
		}
		return out
	}
	return nil
}

func toInt(raw any) (int, bool) {
	switch v := raw.(type) {
	case int:
		return v, true
	case float64:
		return int(v), true
	case string:
		n, err := strconv.Atoi(v)
		return n, err == nil
	}
	return 0, false
}

func toFloat(raw any) (float64, bool) {
	switch v := raw.(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case string:
		f, err := strconv.ParseFloat(v, 64)
		return f, err == nil
	}
	return 0, false
}

func valueToString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	default:
		return fmt.Sprint(x)
	}
}

func parseRangeBound(raw any) (val float64, ok bool, isDate bool) {
	if raw == nil {
		return 0, false, false
	}
	if f, ok := toFloat(raw); ok {
		return f, true, false
	}
	if s, ok := raw.(string); ok {
		layouts := []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05", "2006-01-02"}
		for _, l := range layouts {
			if t, err := time.Parse(l, s); err == nil {
				return float64(t.UTC().UnixMilli()), true, true
			}
		}
	}
	return 0, false, false
}

func parseIntervalMillis(spec string) int64 {
	switch strings.ToLower(strings.TrimSpace(spec)) {
	case "1s", "second":
		return 1000
	case "1m", "minute":
		return 60 * 1000
	case "5m":
		return 5 * 60 * 1000
	case "1h", "hour":
		return 3600 * 1000
	case "1d", "day":
		return 24 * 3600 * 1000
	default:
		if d, err := time.ParseDuration(spec); err == nil && d > 0 {
			return d.Milliseconds()
		}
		return 3600 * 1000
	}
}

func wildcardMatch(pattern, s string) bool {
	if pattern == "*" {
		return true
	}
	var re strings.Builder
	re.WriteString("^")
	for _, r := range pattern {
		switch r {
		case '*':
			re.WriteString(".*")
		case '?':
			re.WriteString(".")
		default:
			re.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	re.WriteString("$")
	matched, err := regexp.MatchString(re.String(), s)
	return err == nil && matched
}
