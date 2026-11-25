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
	case (path == "/_cluster/health" || strings.HasPrefix(path, "/_cluster/health/")) && r.Method == http.MethodGet:
		var patterns []string
		if strings.HasPrefix(path, "/_cluster/health/") {
			raw := strings.TrimPrefix(path, "/_cluster/health/")
			if raw != "" {
				patterns = strings.Split(raw, ",")
			}
		}
		s.handleClusterHealth(w, r, patterns)
		return
	case (path == "/_cluster/state" || strings.HasPrefix(path, "/_cluster/state/")) && r.Method == http.MethodGet:
		var metrics []string
		if strings.HasPrefix(path, "/_cluster/state/") {
			rest := strings.TrimPrefix(path, "/_cluster/state/")
			if rest != "" {
				firstSeg := strings.SplitN(rest, "/", 2)[0]
				if firstSeg != "" {
					metrics = strings.Split(firstSeg, ",")
				}
			}
		}
		s.handleClusterState(w, r, metrics)
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
	case path == "/_cinder/membership" && r.Method == http.MethodGet:
		s.writeJSON(w, r, http.StatusOK, s.node.MembershipSnapshot(r.Context()))
		return
	case path == "/_cinder/drain" && r.Method == http.MethodPost:
		res, err := s.node.Drain(r.Context())
		if err != nil {
			s.writeESError(w, r, http.StatusInternalServerError, "drain_exception", err.Error())
			return
		}
		s.writeJSON(w, r, http.StatusOK, res)
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

func (s *Server) handleClusterHealth(w http.ResponseWriter, r *http.Request, indexPatterns []string) {
	q := r.URL.Query()
	level := strings.ToLower(strings.TrimSpace(q.Get("level")))
	if level != "" && level != "cluster" && level != "indices" && level != "shards" {
		s.writeESError(w, r, http.StatusBadRequest, "illegal_argument_exception", "invalid level ["+level+"]")
		return
	}
	waitForStatus := strings.TrimSpace(q.Get("wait_for_status"))
	timeout := 30 * time.Second
	if rawTimeout := strings.TrimSpace(q.Get("timeout")); rawTimeout != "" {
		d, err := parseESDuration(rawTimeout)
		if err != nil || d < 0 {
			s.writeESError(w, r, http.StatusBadRequest, "illegal_argument_exception", "invalid timeout ["+rawTimeout+"]")
			return
		}
		timeout = d
	}

	resp, err := s.node.WaitForClusterHealth(r.Context(), indexPatterns, level, waitForStatus, timeout)
	if err != nil {
		status := http.StatusInternalServerError
		errType := "cluster_health_exception"
		switch {
		case strings.Contains(err.Error(), "index_not_found_exception"):
			status = http.StatusNotFound
			errType = "index_not_found_exception"
		case strings.Contains(err.Error(), "illegal_argument_exception"):
			status = http.StatusBadRequest
			errType = "illegal_argument_exception"
		}
		s.writeESError(w, r, status, errType, err.Error())
		return
	}

	httpStatus := http.StatusOK
	if timedOut, ok := resp["timed_out"].(bool); ok && timedOut {
		httpStatus = http.StatusRequestTimeout
	}
	s.writeJSON(w, r, httpStatus, resp)
}

func (s *Server) handleClusterState(w http.ResponseWriter, r *http.Request, metrics []string) {
	s.writeJSON(w, r, http.StatusOK, s.node.ClusterState(metrics))
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
	s.writeCat(w, r, out, []string{"name", "address", "node.role", "roles"})
}

func (s *Server) handleCatIndices(w http.ResponseWriter, r *http.Request) {
	indices := s.node.ListIndices()
	dataCount := len(s.node.Ring().DataNodes())
	out := make([]map[string]any, 0, len(indices))
	for _, idx := range indices {
		health := "green"
		if dataCount == 0 {
			health = "red"
		} else if idx.Settings.NumberOfReplicas > dataCount-1 {
			health = "yellow"
		}
		out = append(out, map[string]any{
			"health": health,
			"status": "open",
			"index":  idx.Name,
			"pri":    idx.Settings.NumberOfShards,
			"rep":    idx.Settings.NumberOfReplicas,
		})
	}
	s.writeCat(w, r, out, []string{"health", "status", "index", "pri", "rep"})
}

func (s *Server) handleCatShards(w http.ResponseWriter, r *http.Request) {
	indices := s.node.ListIndices()
	out := make([]map[string]any, 0)
	for _, idx := range indices {
		placements, err := s.node.Ring().AllPlacements(idx.Name, idx.Settings.NumberOfShards, idx.Settings.NumberOfReplicas)
		if err != nil {
			for sid := 0; sid < idx.Settings.NumberOfShards; sid++ {
				out = append(out, map[string]any{
					"index":  idx.Name,
					"shard":  sid,
					"prirep": "p",
					"state":  "UNASSIGNED",
					"node":   "",
				})
				for u := 0; u < idx.Settings.NumberOfReplicas; u++ {
					out = append(out, map[string]any{
						"index":  idx.Name,
						"shard":  sid,
						"prirep": "r",
						"state":  "UNASSIGNED",
						"node":   "",
					})
				}
			}
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
			for u := len(p.Replicas); u < idx.Settings.NumberOfReplicas; u++ {
				out = append(out, map[string]any{
					"index":  idx.Name,
					"shard":  p.ShardID,
					"prirep": "r",
					"state":  "UNASSIGNED",
					"node":   "",
				})
			}
		}
	}
	s.writeCat(w, r, out, []string{"index", "shard", "prirep", "state", "node"})
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
	s.writeCat(w, r, res, []string{"index", "shard", "prirep", "segment", "docs.count", "docs.deleted", "size.bytes", "committed", "searchable"})
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
		res, err := s.node.IndexDocMeta(r.Context(), indexName, docID, raw, routing, opType)
		if err != nil {
			if errors.Is(err, cluster.ErrVersionConflict) {
				s.writeESError(w, r, http.StatusConflict, "version_conflict_engine_exception", err.Error())
				return
			}
			s.writeESError(w, r, http.StatusInternalServerError, "index_failed_exception", err.Error())
			return
		}
		status := http.StatusCreated
		if docID != "" && !strings.EqualFold(opType, "create") {
			status = http.StatusOK
		}
		result := "updated"
		if res.Created {
			result = "created"
		}
		s.writeJSON(w, r, status, map[string]any{
			"_index":        indexName,
			"_id":           res.ID,
			"_version":      res.Version,
			"_shard":        res.ShardID,
			"_seq_no":       res.SeqNo,
			"_primary_term": res.PrimaryTerm,
			"_crc32c":       res.CRC32C,
			"result":        result,
			"_shards": map[string]any{
				"total":      res.ShardsTotal,
				"successful": res.ShardsSucc,
				"failed":     0,
			},
		})

	case http.MethodGet:
		if docID == "" {
			s.writeESError(w, r, http.StatusBadRequest, "action_request_validation_exception", "document id required")
			return
		}
		res, err := s.node.GetDocMeta(r.Context(), indexName, docID, routing)
		if err != nil {
			s.writeESError(w, r, http.StatusNotFound, "index_not_found_exception", err.Error())
			return
		}
		if !res.Found {
			s.writeJSON(w, r, http.StatusNotFound, map[string]any{
				"_index": indexName,
				"_id":    docID,
				"found":  false,
			})
			return
		}
		resp := map[string]any{
			"_index":        indexName,
			"_id":           docID,
			"_version":      res.Version,
			"_seq_no":       res.SeqNo,
			"_primary_term": res.PrimaryTerm,
			"found":         true,
			"_crc32c":       checksum.FormatHex(res.CRC32C),
			"_source":       json.RawMessage(res.Source),
		}
		if res.S3Pointer != nil {
			resp["_s3_pointer"] = res.S3Pointer
		}
		s.writeJSON(w, r, http.StatusOK, resp)

	case http.MethodDelete:
		if docID == "" {
			s.writeESError(w, r, http.StatusBadRequest, "action_request_validation_exception", "document id required")
			return
		}
		res, err := s.node.DeleteDocMeta(r.Context(), indexName, docID, routing)
		if err != nil {
			s.writeESError(w, r, http.StatusNotFound, "index_not_found_exception", err.Error())
			return
		}
		s.writeJSON(w, r, http.StatusOK, map[string]any{
			"_index":        indexName,
			"_id":           docID,
			"_version":      res.Version,
			"_shard":        res.ShardID,
			"_seq_no":       res.SeqNo,
			"_primary_term": res.PrimaryTerm,
			"result":        "deleted",
			"_shards": map[string]any{
				"total":      res.ShardsTotal,
				"successful": res.ShardsSucc,
				"failed":     0,
			},
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
	res, err := s.node.IndexDocMeta(r.Context(), indexName, docID, merged, routing, "")
	if err != nil {
		s.writeESError(w, r, http.StatusInternalServerError, "update_failed_exception", err.Error())
		return
	}
	s.writeJSON(w, r, http.StatusOK, map[string]any{
		"_index":        indexName,
		"_id":           res.ID,
		"_version":      res.Version,
		"_shard":        res.ShardID,
		"_seq_no":       res.SeqNo,
		"_primary_term": res.PrimaryTerm,
		"_crc32c":       res.CRC32C,
		"result":        "updated",
		"_shards": map[string]any{
			"total":      res.ShardsTotal,
			"successful": res.ShardsSucc,
			"failed":     0,
		},
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
				res, err := s.node.DeleteDocMeta(r.Context(), idxName, docID, routing)
				entry := map[string]any{
					"_index":        idxName,
					"_id":           docID,
					"_version":      res.Version,
					"_seq_no":       res.SeqNo,
					"_primary_term": res.PrimaryTerm,
					"_shards": map[string]any{
						"total":      res.ShardsTotal,
						"successful": res.ShardsSucc,
						"failed":     0,
					},
					"status": http.StatusOK,
					"result": "deleted",
				}
				if err != nil {
					hasErrors = true
					entry["status"] = http.StatusNotFound
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
				res, err := s.node.IndexDocMeta(r.Context(), idxName, docID, docBytes, routing, opType)
				itemStatus := http.StatusCreated
				itemResult := "created"
				if opName == "update" || !res.Created {
					itemStatus = http.StatusOK
					itemResult = "updated"
				}
				entry := map[string]any{
					"_index":        idxName,
					"_id":           res.ID,
					"_version":      res.Version,
					"_seq_no":       res.SeqNo,
					"_primary_term": res.PrimaryTerm,
					"_crc32c":       checksum.FormatHex(res.CRC32C),
					"_shards": map[string]any{
						"total":      res.ShardsTotal,
						"successful": res.ShardsSucc,
						"failed":     0,
					},
					"status": itemStatus,
					"result": itemResult,
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
						entry["status"] = http.StatusInternalServerError
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

func (s *Server) writeCat(w http.ResponseWriter, r *http.Request, rows []map[string]any, defaultCols []string) {
	q := r.URL.Query()
	format := strings.ToLower(strings.TrimSpace(q.Get("format")))
	accept := strings.ToLower(r.Header.Get("Accept"))
	hasV := q.Has("v") && !strings.EqualFold(q.Get("v"), "false")
	wantText := format == "text" || format == "txt" || (format == "" && (hasV || strings.Contains(accept, "text/plain")))

	if !wantText {
		s.writeJSON(w, r, http.StatusOK, rows)
		return
	}

	cols := defaultCols
	if rawH := strings.TrimSpace(q.Get("h")); rawH != "" {
		custom := make([]string, 0)
		for _, c := range strings.Split(rawH, ",") {
			c = strings.TrimSpace(c)
			if c != "" {
				custom = append(custom, c)
			}
		}
		if len(custom) > 0 {
			cols = custom
		}
	}

	widths := make([]int, len(cols))
	if hasV {
		for i, c := range cols {
			widths[i] = len(c)
		}
	}

	grid := make([][]string, len(rows))
	for ri, row := range rows {
		line := make([]string, len(cols))
		for ci, col := range cols {
			val := formatCatCell(row[col])
			line[ci] = val
			if len(val) > widths[ci] {
				widths[ci] = len(val)
			}
		}
		grid[ri] = line
	}

	var buf strings.Builder
	writeRow := func(cells []string) {
		for i, cell := range cells {
			if i > 0 {
				buf.WriteString("  ")
			}
			buf.WriteString(cell)
			if i < len(cells)-1 && len(cell) < widths[i] {
				buf.WriteString(strings.Repeat(" ", widths[i]-len(cell)))
			}
		}
		buf.WriteByte('\n')
	}

	if hasV {
		writeRow(cols)
	}
	for _, line := range grid {
		writeRow(line)
	}

	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(buf.String()))
}

func formatCatCell(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case []string:
		return strings.Join(x, ",")
	case []any:
		parts := make([]string, len(x))
		for i, item := range x {
			parts[i] = fmt.Sprint(item)
		}
		return strings.Join(parts, ",")
	default:
		return fmt.Sprint(v)
	}
}

func parseESDuration(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if d, err := time.ParseDuration(raw); err == nil {
		return d, nil
	}
	if strings.HasSuffix(raw, "d") {
		days, err := strconv.ParseFloat(strings.TrimSuffix(raw, "d"), 64)
		if err != nil {
			return 0, err
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Duration(ms) * time.Millisecond, nil
	}
	return 0, fmt.Errorf("invalid duration %q", raw)
}

func (s *Server) writeCat(w http.ResponseWriter, r *http.Request, rows []map[string]any, defaultCols []string) {
	q := r.URL.Query()
	format := strings.ToLower(strings.TrimSpace(q.Get("format")))
	accept := strings.ToLower(r.Header.Get("Accept"))
	hasV := q.Has("v") && !strings.EqualFold(q.Get("v"), "false")
	wantText := format == "text" || format == "txt" || (format == "" && (hasV || strings.Contains(accept, "text/plain")))

	if !wantText {
		s.writeJSON(w, r, http.StatusOK, rows)
		return
	}

	cols := defaultCols
	if rawH := strings.TrimSpace(q.Get("h")); rawH != "" {
		custom := make([]string, 0)
		for _, c := range strings.Split(rawH, ",") {
			c = strings.TrimSpace(c)
			if c != "" {
				custom = append(custom, c)
			}
		}
		if len(custom) > 0 {
			cols = custom
		}
	}

	widths := make([]int, len(cols))
	if hasV {
		for i, c := range cols {
			widths[i] = len(c)
		}
	}

	grid := make([][]string, len(rows))
	for ri, row := range rows {
		line := make([]string, len(cols))
		for ci, col := range cols {
			val := formatCatCell(row[col])
			line[ci] = val
			if len(val) > widths[ci] {
				widths[ci] = len(val)
			}
		}
		grid[ri] = line
	}

	var buf strings.Builder
	writeRow := func(cells []string) {
		for i, cell := range cells {
			if i > 0 {
				buf.WriteString("  ")
			}
			buf.WriteString(cell)
			if i < len(cells)-1 && len(cell) < widths[i] {
				buf.WriteString(strings.Repeat(" ", widths[i]-len(cell)))
			}
		}
		buf.WriteByte('\n')
	}

	if hasV {
		writeRow(cols)
	}
	for _, line := range grid {
		writeRow(line)
	}

	w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(buf.String()))
}

func formatCatCell(v any) string {
	if v == nil {
		return ""
	}
	switch x := v.(type) {
	case []string:
		return strings.Join(x, ",")
	case []any:
		parts := make([]string, len(x))
		for i, item := range x {
			parts[i] = fmt.Sprint(item)
		}
		return strings.Join(parts, ",")
	default:
		return fmt.Sprint(v)
	}
}

func parseESDuration(raw string) (time.Duration, error) {
	raw = strings.TrimSpace(raw)
	if d, err := time.ParseDuration(raw); err == nil {
		return d, nil
	}
	if strings.HasSuffix(raw, "d") {
		days, err := strconv.ParseFloat(strings.TrimSuffix(raw, "d"), 64)
		if err != nil {
			return 0, err
		}
		return time.Duration(days * float64(24*time.Hour)), nil
	}
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil {
		return time.Duration(ms) * time.Millisecond, nil
	}
	return 0, fmt.Errorf("invalid duration %q", raw)
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
