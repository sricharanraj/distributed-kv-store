package cluster

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"
)

// Replicator pushes writes to the replica nodes for a key over HTTP, hitting
// each peer's internal (non-public) replication endpoint. Replication here
// is synchronous best-effort: the caller decides how many acks to require
// (see ReplicateFactor / quorum handling in the API layer).
type Replicator struct {
	client *http.Client
}

func NewReplicator() *Replicator {
	return &Replicator{client: &http.Client{Timeout: 3 * time.Second}}
}

// PushPut sends key=value to nodeAddr's internal replication endpoint.
func (r *Replicator) PushPut(ctx context.Context, nodeAddr, key string, value []byte) error {
	url := fmt.Sprintf("http://%s/internal/replicate/%s", nodeAddr, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(value))
	if err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("replicate put to %s: %w", nodeAddr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("replicate put to %s: status %d", nodeAddr, resp.StatusCode)
	}
	return nil
}

// PushDelete sends a delete for key to nodeAddr's internal replication endpoint.
func (r *Replicator) PushDelete(ctx context.Context, nodeAddr, key string) error {
	url := fmt.Sprintf("http://%s/internal/replicate/%s", nodeAddr, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
	if err != nil {
		return err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("replicate delete to %s: %w", nodeAddr, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("replicate delete to %s: status %d", nodeAddr, resp.StatusCode)
	}
	return nil
}

// FetchGet proxies a read to nodeAddr's public GET endpoint, used when this
// node isn't a replica owner for the key and must forward the request.
func (r *Replicator) FetchGet(ctx context.Context, nodeAddr, key string) ([]byte, int, error) {
	url := fmt.Sprintf("http://%s/kv/%s", nodeAddr, key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	buf.ReadFrom(resp.Body)
	return buf.Bytes(), resp.StatusCode, nil
}
