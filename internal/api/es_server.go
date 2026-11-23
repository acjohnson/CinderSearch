package api

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/cindersearch/cindersearch/internal/checksum"
	"github.com/cindersearch/cindersearch/internal/cluster"
	"github.com/cindersearch/cindersearch/internal/query"
)

// Server exposes a drop-in Elasticsearch / OpenSearch REST API backed by a CinderSearch Node.
type Server struct {
	node *cluster.Node
}

// NewHandler returns an http.Handler implementing Elasticsearch REST endpoints and internal peer RPCs.
func NewHandler(node *cluster.Node) http.Handler {
	return &Server{node: node}
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Official Elasticsearch v8+ clients require X-Elastic-Product on every response.
	w.Header().Set("X-Elastic-Product", "Elasticsearch")

	path := strings.TrimRight(r.URL.Path, "/")
	if path == "" {
		path = "/"
	}

	// Root cluster info banner (Elasticsearch compatible)
	if path == "/" && r.Method == http.MethodGet {
		s.writeJSON(w, r, http.StatusOK, map[string]any{
			"name":         s.node.ID(),
			"cluster_name": "cindersearch-cluster",
			"roles":        s.node.Roles(),
			"tagline":      "You Know, for Search (CinderSearch S3+NVMe Engine)",
			"version": map[string]any{
				"number":         "8.11.0",
				"engine":         "cindersearch-go",
				"build_flavor":   "s3-nvme",
				"lucene_version": "compat",
			},
		})
		return
	}

	// Internal peer-to-peer RPC routes
	if strings.HasPrefix(path, "/_internal/") {
		s.handleInternal(w, r, path)
		return
	}

	// Cluster-wide routes
	switch {
	case path == "/_cluster/health" && r.Method == http.MethodGet:
		s.handleClusterHealth(w, r)
		return
	case path == "/_nodes" && r.Method == http.MethodGet:
		s.handleNodes(w, r)
		return
	case path == "/_cat/nodes" && r.Method == http.MethodGet:
		s.handleCatNodes(w, r)
		return
	case path == "/_cat/indices" && r.Method == http.MethodGet:
		s.handleCatIndices(w, r)
		return
	case path == "/_cat/shards" && r.Method == http.MethodGet:
		s.handleCatShards(w, r)
		return
	case (path == "/_cat/segments" || strings.HasPrefix(path, "/_cat/segments/")) && r.Method == http.MethodGet:
		var patterns []string
		if strings.HasPrefix(path, "/_cat/segments/") {
			patterns = strings.Split(strings.TrimPrefix(path, "/_cat/segments/"), ",")
		}
		s.handleCatSegments(w, r, patterns)
		return
	case path == "/_segments" && r.Method == http.MethodGet:
		s.handleSegments(w, r, nil)
		return
	case path == "/_forcemerge" && r.Method == http.MethodPost:
		s.handleForceMerge(w, r, nil)
		return
	case path == "/_cinder/stats" && r.Method == http.MethodGet:
		s.writeJSON(w, r, http.StatusOK, s.node.Stats())
		return
	case path == "/_bulk" && (r.Method == http.MethodPost || r.Method == http.MethodPut):
		s.handleBulk(w, r, "")
		return
	case path == "/_search" && (r.Method == http.MethodPost || r.Method == http.MethodGet):
		s.handleSearch(w, r, nil)
		return
	case path == "/_count" && (r.Method == http.MethodPost || r.Method == http.MethodGet):
		s.handleCount(w, r, nil)
		return
	case path == "/_aliases" && r.Method == http.MethodPost:
		s.handleUpdateAliases(w, r)
		return
	case (path == "/_aliases" || path == "/_alias") && r.Method == http.MethodGet:
		s.handleGetAliases(w, r, nil, "")
		return
	case strings.HasPrefix(path, "/_alias/"):
		aliasName := strings.TrimPrefix(path, "/_alias/")
		if r.Method == http.MethodHead {
			res := s.node.GetAliases(nil, aliasName)
			if len(res) == 0 {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodGet {
			s.handleGetAliases(w, r, nil, aliasName)
			return
		}
	}

	// Index-scoped routes: /{index}, /{index}/_doc/{id}, /{index}/_create/{id}, /{index}/_bulk,
	// /{index}/_search, /{index}/_count, /{index}/_refresh, /{index}/_mapping, /{index}/_alias/{name}
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.Split(trimmed, "/")
	indexName := parts[0]
	if indexName == "" || (strings.HasPrefix(indexName, "_") && indexName != "_all") {
		s.writeESError(w, r, http.StatusNotFound, "no_such_route", "unknown endpoint "+path)
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodHead:
			if s.node.HasIndexOrAlias(indexName) {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		case http.MethodPut:
			s.handleCreateIndex(w, r, indexName)
		case http.MethodGet:
			s.handleGetIndex(w, r, indexName)
		case http.MethodDelete:
			s.handleDeleteIndex(w, r, indexName)
		default:
			s.writeESError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", r.Method)
		}
		return
	}

	action := parts[1]
	switch action {
	case "_refresh":
		flush := r.URL.Query().Get("flush") == "true"
		resolved, err := s.node.ResolveIndices(strings.Split(indexName, ","))
		if err != nil {
			s.writeESError(w, r, http.StatusNotFound, "index_not_found_exception", err.Error())
			return
		}
		for _, idx := range resolved {
			if err := s.node.RefreshIndex(r.Context(), idx, flush, true); err != nil {
				s.writeESError(w, r, http.StatusInternalServerError, "refresh_error", err.Error())
				return
			}
		}
		s.writeJSON(w, r, http.StatusOK, map[string]any{"_shards": map[string]any{"total": len(resolved), "successful": len(resolved), "failed": 0}})
	case "_bulk":
		s.handleBulk(w, r, indexName)
	case "_search":
		indices := strings.Split(indexName, ",")
		s.handleSearch(w, r, indices)
	case "_count":
		indices := strings.Split(indexName, ",")
		s.handleCount(w, r, indices)
	case "_forcemerge":
		if r.Method != http.MethodPost {
			s.writeESError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", r.Method)
			return
		}
		s.handleForceMerge(w, r, strings.Split(indexName, ","))
	case "_segments":
		if r.Method != http.MethodGet {
			s.writeESError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", r.Method)
			return
		}
		s.handleSegments(w, r, strings.Split(indexName, ","))
	case "_mapping", "_mappings":
		s.handleMapping(w, r, indexName)
	case "_alias", "_aliases":
		aliasName := ""
		if len(parts) >= 3 {
			aliasName = parts[2]
		}
		s.handleIndexAlias(w, r, indexName, aliasName)
	case "_doc":
		docID := ""
		if len(parts) >= 3 {
			docID = parts[2]
		}
		s.handleDoc(w, r, indexName, docID, r.URL.Query().Get("op_type"))
	case "_create":
		if len(parts) < 3 || parts[2] == "" {
			s.writeESError(w, r, http.StatusBadRequest, "action_request_validation_exception", "document id required for _create")
			return
		}
		s.handleDoc(w, r, indexName, parts[2], "create")
	case "_update":
		if len(parts) < 3 {
			s.writeESError(w, r, http.StatusBadRequest, "action_request_validation_exception", "document id required")
			return
		}
		s.handleUpdateDoc(w, r, indexName, parts[2])
	default:
		s.writeESError(w, r, http.StatusNotFound, "no_such_route", "unknown index action "+action)
	}
}

func (s *Server) handleInternal(w http.ResponseWriter, r *http.Request, path string) {
	switch path {
	case "/_internal/index/sync":
		if r.Method == http.MethodDelete {
			idx := r.URL.Query().Get("index")
			_ = s.node.DeleteIndex(r.Context(), idx, false)
			s.writeJSON(w, r, http.StatusOK, map[string]any{"acknowledged": true})
			return
		}
		var meta cluster.IndexMetadata
		if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
			s.writeESError(w, r, http.StatusBadRequest, "json_parse_exception", err.Error())
			return
		}
		if err := s.node.ApplyIndexMetadata(r.Context(), meta); err != nil {
			s.writeESError(w, r, http.StatusInternalServerError, "sync_error", err.Error())
			return
		}
		s.writeJSON(w, r, http.StatusOK, map[string]any{"acknowledged": true})

	case "/_internal/refresh":
		idx := r.URL.Query().Get("index")
		flush := r.URL.Query().Get("flush") == "true"
		if err := s.node.RefreshIndex(r.Context(), idx, flush, false); err != nil {
			s.writeESError(w, r, http.StatusInternalServerError, "refresh_error", err.Error())
			return
		}
		s.writeJSON(w, r, http.StatusOK, map[string]any{"ok": true})

	case "/_internal/replicate":
		var req cluster.ReplicateBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeESError(w, r, http.StatusBadRequest, "json_parse_exception", err.Error())
			return
		}
		ackSeq, err := s.node.ApplyReplicaBatch(r.Context(), req)
		if err != nil {
			s.writeESError(w, r, http.StatusBadRequest, "replication_error", err.Error())
			return
		}
		s.writeJSON(w, r, http.StatusOK, cluster.ReplicateBatchResponse{AckSeq: ackSeq})

	case "/_internal/shard/search":
		var req cluster.ShardSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeESError(w, r, http.StatusBadRequest, "json_parse_exception", err.Error())
			return
		}
		hits, err := s.node.ExecuteLocalShardSearch(r.Context(), req)
		if err != nil {
			s.writeESError(w, r, http.StatusInternalServerError, "shard_search_error", err.Error())
			return
		}
		s.writeJSON(w, r, http.StatusOK, hits)

	case "/_internal/forcemerge":
		var req cluster.ForceMergeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeESError(w, r, http.StatusBadRequest, "json_parse_exception", err.Error())
			return
		}
		sum, err := s.node.ForceMergeLocal(r.Context(), req.IndexName, req.Options)
		if err != nil {
			s.writeESError(w, r, http.StatusInternalServerError, "forcemerge_error", err.Error())
			return
		}
		s.writeJSON(w, r, http.StatusOK, sum)

	case "/_internal/segments":
		idx := r.URL.Query().Get("index")
		s.writeJSON(w, r, http.StatusOK, s.node.LocalShardSegments(idx))

	default:
		s.writeESError(w, r, http.StatusNotFound, "no_such_route", path)
	}
}

func (s *Server) handleClusterHealth(w http.ResponseWriter, r *http.Request) {
	nodes := s.node.Ring().Nodes()
	dataNodes := s.node.Ring().DataNodes()
	indices := s.node.ListIndices()
	activeShards := 0
	for _, idx := range indices {
		activeShards += idx.Settings.NumberOfShards * (1 + idx.Settings.NumberOfReplicas)
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"cluster_name":         "cindersearch-cluster",
		"status":               "green",
		"number_of_nodes":      len(nodes),
		"number_of_data_nodes": len(dataNodes),
		"active_shards":        activeShards,
		"number_of_indices":    len(indices),
		"storage_backend":      "s3+nvme",
		"checksum_algorithm":   "crc32c",
	})
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	nodes := s.node.Ring().Nodes()
	nodeMap := make(map[string]any, len(nodes))
	for _, n := range nodes {
		nodeMap[n.ID] = map[string]any{
			"name":              n.ID,
			"transport_address": n.Address,
			"roles":             n.NormalizedRoles(),
		}
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"_nodes": map[string]any{
			"total":      len(nodes),
			"successful": len(nodes),
			"failed":     0,
		},
		"cluster_name": "cindersearch-cluster",
		"nodes":        nodeMap,
	})
}

func (s *Server) handleCatNodes(w http.ResponseWriter, r *http.Request) {
	nodes := s.node.Ring().Nodes()
	out := make([]map[string]any, 0, len(nodes))
	for _, n := range nodes {
		var short strings.Builder
		for _, role := range n.NormalizedRoles() {
			if len(role) > 0 {
				short.WriteByte(role[0])
			}
		}
		out = append(out, map[string]any{
			"name":      n.ID,
			"address":   n.Address,
			"roles":     n.NormalizedRoles(),
			"node.role": short.String(),
		})
	}
	s.writeJSON(w, r, http.StatusOK, out)
}

func (s *Server) handleCatIndices(w http.ResponseWriter, r *http.Request) {
	indices := s.node.ListIndices()
	out := make([]map[string]any, 0, len(indices))
	for _, idx := range indices {
		out = append(out, map[string]any{
			"health": "green",
			"status": "open",
			"index":  idx.Name,
			"pri":    idx.Settings.NumberOfShards,
			"rep":    idx.Settings.NumberOfReplicas,
		})
	}
	s.writeJSON(w, r, http.StatusOK, out)
}

func (s *Server) handleCatShards(w http.ResponseWriter, r *http.Request) {
	indices := s.node.ListIndices()
	var out []map[string]any
	for _, idx := range indices {
		placements, err := s.node.Ring().AllPlacements(idx.Name, idx.Settings.NumberOfShards, idx.Settings.NumberOfReplicas)
		if err != nil {
			continue
		}
		for _, p := range placements {
			out = append(out, map[string]any{
				"index":  idx.Name,
				"shard":  p.ShardID,
				"prirep": "p",
				"state":  "STARTED",
				"node":   p.Primary.ID,
			})
			for _, rep := range p.Replicas {
				out = append(out, map[string]any{
					"index":  idx.Name,
					"shard":  p.ShardID,
					"prirep": "r",
					"state":  "STARTED",
					"node":   rep.ID,
				})
			}
		}
	}
	s.writeJSON(w, r, http.StatusOK, out)
}

func (s *Server) handleForceMerge(w http.ResponseWriter, r *http.Request, indexPatterns []string) {
	q := r.URL.Query()
	opts := cluster.ForceMergeOptions{
		MaxNumSegments:     1,
		OnlyExpungeDeletes: q.Get("only_expunge_deletes") == "true",
		Flush:              q.Get("flush") != "false",
	}
	if v := q.Get("max_num_segments"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			s.writeESError(w, r, http.StatusBadRequest, "illegal_argument_exception", "max_num_segments must be >= 1")
			return
		}
		opts.MaxNumSegments = n
	}
	res, err := s.node.ForceMerge(r.Context(), indexPatterns, opts)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "index_not_found_exception") {
			status = http.StatusNotFound
		}
		s.writeESError(w, r, status, "forcemerge_exception", err.Error())
		return
	}
	s.writeJSON(w, r, http.StatusOK, res)
}

func (s *Server) handleSegments(w http.ResponseWriter, r *http.Request, indexPatterns []string) {
	res, err := s.node.IndexSegments(r.Context(), indexPatterns)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "index_not_found_exception") {
			status = http.StatusNotFound
		}
		s.writeESError(w, r, status, "segments_exception", err.Error())
		return
	}
	s.writeJSON(w, r, http.StatusOK, res)
}

func (s *Server) handleCatSegments(w http.ResponseWriter, r *http.Request, indexPatterns []string) {
	res, err := s.node.CatSegments(r.Context(), indexPatterns)
	if err != nil {
		status := http.StatusInternalServerError
		if strings.Contains(err.Error(), "index_not_found_exception") {
			status = http.StatusNotFound
		}
		s.writeESError(w, r, status, "segments_exception", err.Error())
		return
	}
	s.writeJSON(w, r, http.StatusOK, res)
}

func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request, indexName string) {
	var body struct {
		Settings cluster.IndexSettings `json:"settings"`
		Mappings map[string]any        `json:"mappings"`
		Aliases  map[string]any        `json:"aliases"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	var aliasNames []string
	for name := range body.Aliases {
		aliasNames = append(aliasNames, name)
	}
	meta := cluster.IndexMetadata{
		Name:     indexName,
		Settings: body.Settings,
		Mappings: body.Mappings,
		Aliases:  aliasNames,
	}
	if err := s.node.CreateIndex(r.Context(), meta, true); err != nil {
		s.writeESError(w, r, http.StatusBadRequest, "index_creation_exception", err.Error())
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"acknowledged":        true,
		"shards_acknowledged": true,
		"index":               indexName,
	})
}

func (s *Server) handleGetIndex(w http.ResponseWriter, r *http.Request, indexName string) {
	resolved, err := s.node.ResolveIndices(strings.Split(indexName, ","))
	if err != nil {
		s.writeESError(w, r, http.StatusNotFound, "index_not_found_exception", "no such index ["+indexName+"]")
		return
	}
	out := make(map[string]any, len(resolved))
	for _, name := range resolved {
		meta, ok := s.node.GetIndex(name)
		if !ok {
			continue
		}
		aliasesObj := make(map[string]any, len(meta.Aliases))
		for _, a := range meta.Aliases {
			aliasesObj[a] = map[string]any{}
		}
		out[name] = map[string]any{
			"aliases":  aliasesObj,
			"mappings": meta.Mappings,
			"settings": map[string]any{"index": meta.Settings},
		}
	}
	s.writeJSON(w, r, http.StatusOK, out)
}

func (s *Server) handleDeleteIndex(w http.ResponseWriter, r *http.Request, indexName string) {
	if err := s.node.DeleteIndex(r.Context(), indexName, true); err != nil {
		s.writeESError(w, r, http.StatusInternalServerError, "index_delete_exception", err.Error())
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"acknowledged": true})
}

func (s *Server) handleMapping(w http.ResponseWriter, r *http.Request, indexPattern string) {
	switch r.Method {
	case http.MethodGet:
		resolved, err := s.node.ResolveIndices(strings.Split(indexPattern, ","))
		if err != nil {
			s.writeESError(w, r, http.StatusNotFound, "index_not_found_exception", err.Error())
			return
		}
		out := make(map[string]any, len(resolved))
		for _, name := range resolved {
			if meta, ok := s.node.GetIndex(name); ok {
				mappings := meta.Mappings
				if mappings == nil {
					mappings = map[string]any{}
				}
				out[name] = map[string]any{"mappings": mappings}
			}
		}
		s.writeJSON(w, r, http.StatusOK, out)
	case http.MethodPut, http.MethodPost:
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			s.writeESError(w, r, http.StatusBadRequest, "mapper_parsing_exception", err.Error())
			return
		}
		resolved, err := s.node.ResolveIndices(strings.Split(indexPattern, ","))
		if err != nil {
			s.writeESError(w, r, http.StatusNotFound, "index_not_found_exception", err.Error())
			return
		}
		for _, name := range resolved {
			if err := s.node.PutMapping(r.Context(), name, body, true); err != nil {
				s.writeESError(w, r, http.StatusBadRequest, "mapper_parsing_exception", err.Error())
				return
			}
		}
		s.writeJSON(w, r, http.StatusOK, map[string]any{"acknowledged": true})
	default:
		s.writeESError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", r.Method)
	}
}

func (s *Server) handleUpdateAliases(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Actions []map[string]struct {
			Index   string   `json:"index"`
			Indices []string `json:"indices"`
			Alias   string   `json:"alias"`
		} `json:"actions"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeESError(w, r, http.StatusBadRequest, "parse_exception", err.Error())
		return
	}
	var actions []cluster.AliasAction
	for _, entry := range body.Actions {
		for op, spec := range entry {
			actions = append(actions, cluster.AliasAction{
				Action:  op,
				Index:   spec.Index,
				Indices: spec.Indices,
				Alias:   spec.Alias,
			})
		}
	}
	if err := s.node.UpdateAliases(r.Context(), actions, true); err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "index_not_found_exception") {
			status = http.StatusNotFound
		}
		s.writeESError(w, r, status, "aliases_action_exception", err.Error())
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{"acknowledged": true})
}

func (s *Server) handleGetAliases(w http.ResponseWriter, r *http.Request, indexPatterns []string, aliasFilter string) {
	res := s.node.GetAliases(indexPatterns, aliasFilter)
	if aliasFilter != "" && len(res) == 0 {
		s.writeESError(w, r, http.StatusNotFound, "alias_not_found_exception", "alias ["+aliasFilter+"] missing")
		return
	}
	s.writeJSON(w, r, http.StatusOK, res)
}

func (s *Server) handleIndexAlias(w http.ResponseWriter, r *http.Request, indexPattern, aliasName string) {
	switch r.Method {
	case http.MethodGet:
		s.handleGetAliases(w, r, strings.Split(indexPattern, ","), aliasName)
	case http.MethodHead:
		res := s.node.GetAliases(strings.Split(indexPattern, ","), aliasName)
		if len(res) == 0 {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
	case http.MethodPut, http.MethodPost:
		if aliasName == "" {
			s.writeESError(w, r, http.StatusBadRequest, "illegal_argument_exception", "alias name is required")
			return
		}
		err := s.node.UpdateAliases(r.Context(), []cluster.AliasAction{{
			Action: "add",
			Index:  indexPattern,
			Alias:  aliasName,
		}}, true)
		if err != nil {
			status := http.StatusBadRequest
			if strings.Contains(err.Error(), "index_not_found_exception") {
				status = http.StatusNotFound
			}
			s.writeESError(w, r, status, "aliases_action_exception", err.Error())
			return
		}
		s.writeJSON(w, r, http.StatusOK, map[string]any{"acknowledged": true})
	case http.MethodDelete:
		if aliasName == "" {
			s.writeESError(w, r, http.StatusBadRequest, "illegal_argument_exception", "alias name is required")
			return
		}
		err := s.node.UpdateAliases(r.Context(), []cluster.AliasAction{{
			Action: "remove",
			Index:  indexPattern,
			Alias:  aliasName,
		}}, true)
		if err != nil {
			status := http.StatusBadRequest
			if strings.Contains(err.Error(), "index_not_found_exception") {
				status = http.StatusNotFound
			}
			s.writeESError(w, r, status, "aliases_action_exception", err.Error())
			return
		}
		s.writeJSON(w, r, http.StatusOK, map[string]any{"acknowledged": true})
	default:
		s.writeESError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", r.Method)
	}
}

func (s *Server) handleDoc(w http.ResponseWriter, r *http.Request, indexName, docID, opType string) {
	routing := r.URL.Query().Get("routing")
	switch r.Method {
	case http.MethodHead:
		if docID == "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		exists, err := s.node.DocExists(r.Context(), indexName, docID, routing)
		if err != nil || !exists {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)

	case http.MethodPut, http.MethodPost:
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			s.writeESError(w, r, http.StatusBadRequest, "parse_exception", err.Error())
			return
		}
		var check map[string]any
		if err := json.Unmarshal(raw, &check); err != nil || check == nil {
			reason := "document body must be a JSON object"
			if err != nil {
				reason = err.Error()
			}
			s.writeESError(w, r, http.StatusBadRequest, "mapper_parsing_exception", reason)
			return
		}
		assignedID, shardID, crc, seq, err := s.node.IndexDocWithOpType(r.Context(), indexName, docID, raw, routing, opType)
		if err != nil {
			if errors.Is(err, cluster.ErrVersionConflict) {
				s.writeESError(w, r, http.StatusConflict, "version_conflict_engine_exception", err.Error())
				return
			}
			s.writeESError(w, r, http.StatusInternalServerError, "index_failed_exception", err.Error())
			return
		}
		status := http.StatusCreated
		result := "created"
		if docID != "" && !strings.EqualFold(opType, "create") {
			status = http.StatusOK
			result = "updated"
		}
		s.writeJSON(w, r, status, map[string]any{
			"_index":  indexName,
			"_id":     assignedID,
			"_shard":  shardID,
			"_seq_no": seq,
			"_crc32c": crc,
			"result":  result,
		})

	case http.MethodGet:
		if docID == "" {
			s.writeESError(w, r, http.StatusBadRequest, "action_request_validation_exception", "document id required")
			return
		}
		raw, found, crc, ptr, err := s.node.GetDoc(r.Context(), indexName, docID, routing)
		if err != nil {
			s.writeESError(w, r, http.StatusNotFound, "index_not_found_exception", err.Error())
			return
		}
		if !found {
			s.writeJSON(w, r, http.StatusNotFound, map[string]any{
				"_index": indexName,
				"_id":    docID,
				"found":  false,
			})
			return
		}
		resp := map[string]any{
			"_index":  indexName,
			"_id":     docID,
			"found":   true,
			"_crc32c": checksum.FormatHex(crc),
			"_source": json.RawMessage(raw),
		}
		if ptr != nil {
			resp["_s3_pointer"] = ptr
		}
		s.writeJSON(w, r, http.StatusOK, resp)

	case http.MethodDelete:
		if docID == "" {
			s.writeESError(w, r, http.StatusBadRequest, "action_request_validation_exception", "document id required")
			return
		}
		shardID, seq, err := s.node.DeleteDoc(r.Context(), indexName, docID, routing)
		if err != nil {
			s.writeESError(w, r, http.StatusNotFound, "index_not_found_exception", err.Error())
			return
		}
		s.writeJSON(w, r, http.StatusOK, map[string]any{
			"_index":  indexName,
			"_id":     docID,
			"_shard":  shardID,
			"_seq_no": seq,
			"result":  "deleted",
		})
	default:
		s.writeESError(w, r, http.StatusMethodNotAllowed, "method_not_allowed", r.Method)
	}
}

func (s *Server) handleUpdateDoc(w http.ResponseWriter, r *http.Request, indexName, docID string) {
	var body struct {
		Doc map[string]any `json:"doc"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeESError(w, r, http.StatusBadRequest, "json_parse_exception", err.Error())
		return
	}
	routing := r.URL.Query().Get("routing")
	existing := make(map[string]any)
	if raw, found, _, _, err := s.node.GetDoc(r.Context(), indexName, docID, routing); err == nil && found {
		_ = json.Unmarshal(raw, &existing)
	}
	for k, v := range body.Doc {
		existing[k] = v
	}
	merged, err := json.Marshal(existing)
	if err != nil {
		s.writeESError(w, r, http.StatusInternalServerError, "json_marshal_exception", err.Error())
		return
	}
	assignedID, shardID, crc, seq, err := s.node.IndexDoc(r.Context(), indexName, docID, merged, routing)
	if err != nil {
		s.writeESError(w, r, http.StatusInternalServerError, "update_failed_exception", err.Error())
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"_index":  indexName,
		"_id":     assignedID,
		"_shard":  shardID,
		"_seq_no": seq,
		"_crc32c": crc,
		"result":  "updated",
	})
}

func (s *Server) handleBulk(w http.ResponseWriter, r *http.Request, defaultIndex string) {
	start := time.Now()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		s.writeESError(w, r, http.StatusBadRequest, "parse_exception", err.Error())
		return
	}

	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64*1024), 32*1024*1024)

	var items []map[string]any
	hasErrors := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var actionLine map[string]map[string]any
		if err := json.Unmarshal([]byte(line), &actionLine); err != nil {
			s.writeESError(w, r, http.StatusBadRequest, "illegal_argument_exception", "malformed bulk action line: "+err.Error())
			return
		}

		for opName, meta := range actionLine {
			idxName := defaultIndex
			if v, ok := meta["_index"].(string); ok && v != "" {
				idxName = v
			}
			docID, _ := meta["_id"].(string)
			routing, _ := meta["routing"].(string)

			switch opName {
			case "delete":
				_, seq, err := s.node.DeleteDoc(r.Context(), idxName, docID, routing)
				entry := map[string]any{"_index": idxName, "_id": docID, "_seq_no": seq, "status": 200, "result": "deleted"}
				if err != nil {
					hasErrors = true
					entry["status"] = 404
					entry["error"] = err.Error()
				}
				items = append(items, map[string]any{"delete": entry})

			case "index", "create", "update":
				if !scanner.Scan() {
					s.writeESError(w, r, http.StatusBadRequest, "illegal_argument_exception", "missing document payload line after bulk action")
					return
				}
				docBytes := append([]byte(nil), bytes.TrimSpace(scanner.Bytes())...)
				var docCheck map[string]any
				if err := json.Unmarshal(docBytes, &docCheck); err != nil || docCheck == nil {
					hasErrors = true
					reason := "document body must be a JSON object"
					if err != nil {
						reason = err.Error()
					}
					items = append(items, map[string]any{
						opName: map[string]any{
							"_index": idxName,
							"_id":    docID,
							"status": http.StatusBadRequest,
							"error": map[string]any{
								"type":   "mapper_parsing_exception",
								"reason": reason,
							},
						},
					})
					continue
				}
				if opName == "update" {
					var partial struct {
						Doc map[string]any `json:"doc"`
					}
					if json.Unmarshal(docBytes, &partial) == nil && partial.Doc != nil {
						existing := make(map[string]any)
						if raw, found, _, _, err := s.node.GetDoc(r.Context(), idxName, docID, routing); err == nil && found {
							_ = json.Unmarshal(raw, &existing)
						}
						for k, v := range partial.Doc {
							existing[k] = v
						}
						docBytes, _ = json.Marshal(existing)
					}
				}
				opType := ""
				if opName == "create" {
					opType = "create"
				}
				id, _, crc, seq, err := s.node.IndexDocWithOpType(r.Context(), idxName, docID, docBytes, routing, opType)
				entry := map[string]any{
					"_index":  idxName,
					"_id":     id,
					"_seq_no": seq,
					"_crc32c": checksum.FormatHex(crc),
					"status":  201,
					"result":  "created",
				}
				if err != nil {
					hasErrors = true
					if errors.Is(err, cluster.ErrVersionConflict) {
						entry["_id"] = docID
						entry["status"] = http.StatusConflict
						entry["error"] = map[string]any{
							"type":   "version_conflict_engine_exception",
							"reason": err.Error(),
						}
					} else {
						entry["status"] = 500
						entry["error"] = err.Error()
					}
				}
				items = append(items, map[string]any{opName: entry})
			}
		}
	}

	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"took":   time.Since(start).Milliseconds(),
		"errors": hasErrors,
		"items":  items,
	})
}

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request, indexNames []string) {
	var req query.SearchRequest
	if r.Body != nil {
		raw, err := io.ReadAll(r.Body)
		if err == nil && len(bytes.TrimSpace(raw)) > 0 {
			if err := json.Unmarshal(raw, &req); err != nil {
				s.writeESError(w, r, http.StatusBadRequest, "parsing_exception", err.Error())
				return
			}
		}
	}

	// Support GET /_search?q=...&size=...&from=...
	if qParam := r.URL.Query().Get("q"); qParam != "" && len(req.Query) == 0 {
		req.Query = map[string]any{
			"query_string": map[string]any{"query": qParam},
		}
	}
	if sizeStr := r.URL.Query().Get("size"); sizeStr != "" && req.Size == nil {
		if n, err := strconv.Atoi(sizeStr); err == nil {
			req.Size = &n
		}
	}
	if fromStr := r.URL.Query().Get("from"); fromStr != "" && req.From == 0 {
		if n, err := strconv.Atoi(fromStr); err == nil {
			req.From = n
		}
	}

	mode := cluster.RetrievalMode(strings.ToLower(r.URL.Query().Get("retrieval")))
	if mode == "" {
		mode = cluster.RetrievalMode(strings.ToLower(r.Header.Get("X-Cinder-Retrieval")))
	}
	if mode == "" {
		mode = cluster.RetrievalDropIn
	}

	preferReader := strings.EqualFold(r.URL.Query().Get("preference"), "reader") ||
		strings.EqualFold(r.Header.Get("X-Cinder-Preference"), "reader")

	resp, err := s.node.SearchWithPreference(r.Context(), indexNames, req, mode, preferReader)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "index_not_found_exception") {
			status = http.StatusNotFound
		}
		s.writeESError(w, r, status, "search_phase_execution_exception", err.Error())
		return
	}
	s.writeJSON(w, r, http.StatusOK, resp)
}

func (s *Server) handleCount(w http.ResponseWriter, r *http.Request, indexNames []string) {
	var body struct {
		Query map[string]any `json:"query"`
	}
	if r.Body != nil {
		raw, err := io.ReadAll(r.Body)
		if err == nil && len(bytes.TrimSpace(raw)) > 0 {
			if err := json.Unmarshal(raw, &body); err != nil {
				s.writeESError(w, r, http.StatusBadRequest, "parsing_exception", err.Error())
				return
			}
		}
	}
	if qParam := r.URL.Query().Get("q"); qParam != "" && len(body.Query) == 0 {
		body.Query = map[string]any{
			"query_string": map[string]any{"query": qParam},
		}
	}
	preferReader := strings.EqualFold(r.URL.Query().Get("preference"), "reader") ||
		strings.EqualFold(r.Header.Get("X-Cinder-Preference"), "reader")

	resp, err := s.node.Count(r.Context(), indexNames, body.Query, preferReader)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "index_not_found_exception") {
			status = http.StatusNotFound
		}
		s.writeESError(w, r, status, "search_phase_execution_exception", err.Error())
		return
	}
	s.writeJSON(w, r, http.StatusOK, resp)
}

func (s *Server) writeJSON(w http.ResponseWriter, r *http.Request, status int, payload any) {
	contentType := "application/json"
	if r != nil && strings.Contains(r.Header.Get("Accept"), "application/vnd.elasticsearch+json") {
		contentType = "application/vnd.elasticsearch+json; compatible-with=8"
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if r == nil || (!strings.HasPrefix(r.URL.Path, "/_internal/") && r.URL.Query().Get("pretty") != "false") {
		enc.SetIndent("", "  ")
	}
	_ = enc.Encode(payload)
}

func (s *Server) writeESError(w http.ResponseWriter, r *http.Request, status int, errType, reason string) {
	s.writeJSON(w, r, status, map[string]any{
		"error": map[string]any{
			"root_cause": []map[string]any{{
				"type":   errType,
				"reason": reason,
			}},
			"type":   errType,
			"reason": reason,
		},
		"status": status,
	})
}

// FormatEndpointBanner returns a concise summary line for startup logs.
func FormatEndpointBanner(nodeID, addr, s3Endpoint string) string {
	return fmt.Sprintf("CinderSearch node=%s listening=%s s3=%s", nodeID, addr, s3Endpoint)
}
