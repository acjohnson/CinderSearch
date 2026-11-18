package api

import (
	"bufio"
	"bytes"
	"encoding/json"
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
	path := strings.TrimRight(r.URL.Path, "/")
	if path == "" {
		path = "/"
	}

	// Root cluster info banner (Elasticsearch compatible)
	if path == "/" && r.Method == http.MethodGet {
		s.writeJSON(w, http.StatusOK, map[string]any{
			"name":         s.node.ID(),
			"cluster_name": "cindersearch-cluster",
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
		s.handleClusterHealth(w)
		return
	case path == "/_cat/indices" && r.Method == http.MethodGet:
		s.handleCatIndices(w)
		return
	case path == "/_cat/shards" && r.Method == http.MethodGet:
		s.handleCatShards(w)
		return
	case path == "/_cinder/stats" && r.Method == http.MethodGet:
		s.writeJSON(w, http.StatusOK, s.node.Stats())
		return
	case path == "/_bulk" && (r.Method == http.MethodPost || r.Method == http.MethodPut):
		s.handleBulk(w, r, "")
		return
	case path == "/_search" && (r.Method == http.MethodPost || r.Method == http.MethodGet):
		s.handleSearch(w, r, nil)
		return
	}

	// Index-scoped routes: /{index}, /{index}/_doc/{id}, /{index}/_bulk, /{index}/_search, /{index}/_refresh
	trimmed := strings.TrimPrefix(path, "/")
	parts := strings.Split(trimmed, "/")
	indexName := parts[0]
	if indexName == "" || strings.HasPrefix(indexName, "_") {
		s.writeESError(w, http.StatusNotFound, "no_such_route", "unknown endpoint "+path)
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodPut:
			s.handleCreateIndex(w, r, indexName)
		case http.MethodGet:
			s.handleGetIndex(w, indexName)
		case http.MethodDelete:
			s.handleDeleteIndex(w, r, indexName)
		default:
			s.writeESError(w, http.StatusMethodNotAllowed, "method_not_allowed", r.Method)
		}
		return
	}

	action := parts[1]
	switch action {
	case "_refresh":
		flush := r.URL.Query().Get("flush") == "true"
		if err := s.node.RefreshIndex(r.Context(), indexName, flush, true); err != nil {
			s.writeESError(w, http.StatusInternalServerError, "refresh_error", err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]any{"_shards": map[string]any{"total": 1, "successful": 1, "failed": 0}})
	case "_bulk":
		s.handleBulk(w, r, indexName)
	case "_search":
		indices := strings.Split(indexName, ",")
		s.handleSearch(w, r, indices)
	case "_doc":
		docID := ""
		if len(parts) >= 3 {
			docID = parts[2]
		}
		s.handleDoc(w, r, indexName, docID)
	case "_update":
		if len(parts) < 3 {
			s.writeESError(w, http.StatusBadRequest, "action_request_validation_exception", "document id required")
			return
		}
		s.handleUpdateDoc(w, r, indexName, parts[2])
	default:
		s.writeESError(w, http.StatusNotFound, "no_such_route", "unknown index action "+action)
	}
}

func (s *Server) handleInternal(w http.ResponseWriter, r *http.Request, path string) {
	switch path {
	case "/_internal/index/sync":
		if r.Method == http.MethodDelete {
			idx := r.URL.Query().Get("index")
			_ = s.node.DeleteIndex(r.Context(), idx, false)
			s.writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true})
			return
		}
		var meta cluster.IndexMetadata
		if err := json.NewDecoder(r.Body).Decode(&meta); err != nil {
			s.writeESError(w, http.StatusBadRequest, "json_parse_exception", err.Error())
			return
		}
		if err := s.node.ApplyIndexMetadata(r.Context(), meta); err != nil {
			s.writeESError(w, http.StatusInternalServerError, "sync_error", err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true})

	case "/_internal/refresh":
		idx := r.URL.Query().Get("index")
		flush := r.URL.Query().Get("flush") == "true"
		if err := s.node.RefreshIndex(r.Context(), idx, flush, false); err != nil {
			s.writeESError(w, http.StatusInternalServerError, "refresh_error", err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case "/_internal/replicate":
		var req cluster.ReplicateBatchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeESError(w, http.StatusBadRequest, "json_parse_exception", err.Error())
			return
		}
		ackSeq, err := s.node.ApplyReplicaBatch(r.Context(), req)
		if err != nil {
			s.writeESError(w, http.StatusBadRequest, "replication_error", err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, cluster.ReplicateBatchResponse{AckSeq: ackSeq})

	case "/_internal/shard/search":
		var req cluster.ShardSearchRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			s.writeESError(w, http.StatusBadRequest, "json_parse_exception", err.Error())
			return
		}
		hits, err := s.node.ExecuteLocalShardSearch(r.Context(), req)
		if err != nil {
			s.writeESError(w, http.StatusInternalServerError, "shard_search_error", err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, hits)

	default:
		s.writeESError(w, http.StatusNotFound, "no_such_route", path)
	}
}

func (s *Server) handleClusterHealth(w http.ResponseWriter) {
	nodes := s.node.Ring().Nodes()
	indices := s.node.ListIndices()
	activeShards := 0
	for _, idx := range indices {
		activeShards += idx.Settings.NumberOfShards * (1 + idx.Settings.NumberOfReplicas)
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"cluster_name":       "cindersearch-cluster",
		"status":             "green",
		"number_of_nodes":    len(nodes),
		"active_shards":      activeShards,
		"number_of_indices":  len(indices),
		"storage_backend":    "s3+nvme",
		"checksum_algorithm": "crc32c",
	})
}

func (s *Server) handleCatIndices(w http.ResponseWriter) {
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
	s.writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleCatShards(w http.ResponseWriter) {
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
	s.writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleCreateIndex(w http.ResponseWriter, r *http.Request, indexName string) {
	var body struct {
		Settings cluster.IndexSettings `json:"settings"`
		Mappings map[string]any        `json:"mappings"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&body)
	}
	meta := cluster.IndexMetadata{
		Name:     indexName,
		Settings: body.Settings,
		Mappings: body.Mappings,
	}
	if err := s.node.CreateIndex(r.Context(), meta, true); err != nil {
		s.writeESError(w, http.StatusBadRequest, "index_creation_exception", err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		"acknowledged":        true,
		"shards_acknowledged": true,
		"index":               indexName,
	})
}

func (s *Server) handleGetIndex(w http.ResponseWriter, indexName string) {
	meta, ok := s.node.GetIndex(indexName)
	if !ok {
		s.writeESError(w, http.StatusNotFound, "index_not_found_exception", "no such index ["+indexName+"]")
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
		indexName: map[string]any{
			"mappings": meta.Mappings,
			"settings": map[string]any{"index": meta.Settings},
		},
	})
}

func (s *Server) handleDeleteIndex(w http.ResponseWriter, r *http.Request, indexName string) {
	if err := s.node.DeleteIndex(r.Context(), indexName, true); err != nil {
		s.writeESError(w, http.StatusInternalServerError, "index_delete_exception", err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true})
}

func (s *Server) handleDoc(w http.ResponseWriter, r *http.Request, indexName, docID string) {
	routing := r.URL.Query().Get("routing")
	switch r.Method {
	case http.MethodPut, http.MethodPost:
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			s.writeESError(w, http.StatusBadRequest, "parse_exception", err.Error())
			return
		}
		var check map[string]any
		if err := json.Unmarshal(raw, &check); err != nil {
			s.writeESError(w, http.StatusBadRequest, "mapper_parsing_exception", err.Error())
			return
		}
		assignedID, shardID, crc, seq, err := s.node.IndexDoc(r.Context(), indexName, docID, raw, routing)
		if err != nil {
			s.writeESError(w, http.StatusInternalServerError, "index_failed_exception", err.Error())
			return
		}
		status := http.StatusCreated
		result := "created"
		if docID != "" {
			status = http.StatusOK
			result = "updated"
		}
		s.writeJSON(w, status, map[string]any{
			"_index":  indexName,
			"_id":     assignedID,
			"_shard":  shardID,
			"_seq_no": seq,
			"_crc32c": crc,
			"result":  result,
		})

	case http.MethodGet:
		if docID == "" {
			s.writeESError(w, http.StatusBadRequest, "action_request_validation_exception", "document id required")
			return
		}
		raw, found, crc, ptr, err := s.node.GetDoc(r.Context(), indexName, docID, routing)
		if err != nil {
			s.writeESError(w, http.StatusNotFound, "index_not_found_exception", err.Error())
			return
		}
		if !found {
			s.writeJSON(w, http.StatusNotFound, map[string]any{
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
		s.writeJSON(w, http.StatusOK, resp)

	case http.MethodDelete:
		if docID == "" {
			s.writeESError(w, http.StatusBadRequest, "action_request_validation_exception", "document id required")
			return
		}
		shardID, seq, err := s.node.DeleteDoc(r.Context(), indexName, docID, routing)
		if err != nil {
			s.writeESError(w, http.StatusNotFound, "index_not_found_exception", err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]any{
			"_index":  indexName,
			"_id":     docID,
			"_shard":  shardID,
			"_seq_no": seq,
			"result":  "deleted",
		})
	default:
		s.writeESError(w, http.StatusMethodNotAllowed, "method_not_allowed", r.Method)
	}
}

func (s *Server) handleUpdateDoc(w http.ResponseWriter, r *http.Request, indexName, docID string) {
	var body struct {
		Doc map[string]any `json:"doc"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeESError(w, http.StatusBadRequest, "json_parse_exception", err.Error())
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
		s.writeESError(w, http.StatusInternalServerError, "json_marshal_exception", err.Error())
		return
	}
	assignedID, shardID, crc, seq, err := s.node.IndexDoc(r.Context(), indexName, docID, merged, routing)
	if err != nil {
		s.writeESError(w, http.StatusInternalServerError, "update_failed_exception", err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]any{
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
		s.writeESError(w, http.StatusBadRequest, "parse_exception", err.Error())
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
			s.writeESError(w, http.StatusBadRequest, "illegal_argument_exception", "malformed bulk action line: "+err.Error())
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
					s.writeESError(w, http.StatusBadRequest, "illegal_argument_exception", "missing document payload line after bulk action")
					return
				}
				docBytes := append([]byte(nil), bytes.TrimSpace(scanner.Bytes())...)
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
				id, _, crc, seq, err := s.node.IndexDoc(r.Context(), idxName, docID, docBytes, routing)
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
					entry["status"] = 500
					entry["error"] = err.Error()
				}
				items = append(items, map[string]any{opName: entry})
			}
		}
	}

	s.writeJSON(w, http.StatusOK, map[string]any{
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
				s.writeESError(w, http.StatusBadRequest, "parsing_exception", err.Error())
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

	resp, err := s.node.Search(r.Context(), indexNames, req, mode)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "index_not_found_exception") {
			status = http.StatusNotFound
		}
		s.writeESError(w, status, "search_phase_execution_exception", err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, resp)
}

func (s *Server) writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) writeESError(w http.ResponseWriter, status int, errType, reason string) {
	s.writeJSON(w, status, map[string]any{
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
