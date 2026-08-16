// Package api exposes the storage engine over the network: a JSON REST API
// over HTTP, and a lightweight text protocol over raw TCP (mini-Redis
// style). Both layers are shard- and replication-aware: a request for a key
// this node doesn't own is transparently proxied to the owning node.
package api

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/sricharanraj/distributed-kv-store/internal/cluster"
	"github.com/sricharanraj/distributed-kv-store/internal/storage"
)

// Server wires the storage engine to the cluster layer and serves the REST API.
type Server struct {
	Engine            *storage.Engine
	Membership        *cluster.Membership
	Replicator        *cluster.Replicator
	ReplicationFactor int
}

func NewServer(engine *storage.Engine, m *cluster.Membership, rf int) *Server {
	return &Server{
		Engine:            engine,
		Membership:        m,
		Replicator:        cluster.NewReplicator(),
		ReplicationFactor: rf,
	}
}

// Handler builds the http.Handler exposing the public REST API and the
// internal replication endpoint.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/cluster/status", s.handleClusterStatus)
	mux.HandleFunc("/kv", s.handleScan)      // GET /kv?prefix=
	mux.HandleFunc("/kv/", s.handleKV)       // GET/PUT/DELETE /kv/{key}
	mux.HandleFunc("/internal/replicate/", s.handleInternalReplicate)
	return loggingMiddleware(mux)
}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start))
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "node": s.Membership.Self})
}

func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"self":               s.Membership.Self,
		"nodes":              s.Membership.Ring.Nodes(),
		"replication_factor": s.ReplicationFactor,
	})
}

// owners returns the ordered list of nodes (primary first) responsible for key.
func (s *Server) owners(key string) []string {
	return s.Membership.Ring.GetN(key, s.ReplicationFactor)
}

func (s *Server) isOwner(key string) bool {
	for _, o := range s.owners(key) {
		if s.Membership.IsSelf(o) {
			return true
		}
	}
	return false
}

func (s *Server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, r, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, r, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request, key string) {
	if !s.isOwner(key) {
		primary := s.owners(key)[0]
		body, status, err := s.Replicator.FetchGet(r.Context(), primary, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.WriteHeader(status)
		w.Write(body)
		return
	}

	val, err := s.Engine.Get(key)
	if err == storage.ErrNotFound {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(val)
}

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	owners := s.owners(key)
	if !s.isOwner(key) {
		// Not our shard: proxy to the primary owner, which will coordinate replication.
		if err := s.Replicator.PushPut(r.Context(), owners[0], key, body); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	if err := s.Engine.Put(key, body); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.replicateAsync(context.Background(), owners, key, body, false)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request, key string) {
	owners := s.owners(key)
	if !s.isOwner(key) {
		if err := s.Replicator.PushDelete(r.Context(), owners[0], key); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
		return
	}

	if err := s.Engine.Delete(key); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.replicateAsync(context.Background(), owners, key, nil, true)
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// replicateAsync pushes the write to the other replica owners in the
// background so client latency only reflects the local write.
func (s *Server) replicateAsync(ctx context.Context, owners []string, key string, value []byte, tombstone bool) {
	for _, node := range owners {
		if s.Membership.IsSelf(node) {
			continue
		}
		node := node
		go func() {
			ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			var err error
			if tombstone {
				err = s.Replicator.PushDelete(ctx, node, key)
			} else {
				err = s.Replicator.PushPut(ctx, node, key, value)
			}
			if err != nil {
				log.Printf("replication to %s failed: %v", node, err)
			}
		}()
	}
}

// handleInternalReplicate applies a write directly to the local engine
// without further shard routing; it is only ever called by a peer node
// acting as the write coordinator.
func (s *Server) handleInternalReplicate(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/internal/replicate/")
	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if err := s.Engine.Put(key, body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	case http.MethodDelete:
		if err := s.Engine.Delete(key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleScan(w http.ResponseWriter, r *http.Request) {
	prefix := r.URL.Query().Get("prefix")
	results, err := s.Engine.Scan(prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make(map[string]string, len(results))
	for k, v := range results {
		out[k] = string(v)
	}
	writeJSON(w, http.StatusOK, out)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
