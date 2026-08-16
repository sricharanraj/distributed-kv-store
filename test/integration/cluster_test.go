// Package integration spins up multiple in-process nodes (each with its own
// storage engine and HTTP server) to exercise sharding, cross-node request
// proxying, and replication end to end.
package integration

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sricharanraj/distributed-kv-store/internal/api"
	"github.com/sricharanraj/distributed-kv-store/internal/cluster"
	"github.com/sricharanraj/distributed-kv-store/internal/storage"
)

type testNode struct {
	addr   string
	server *api.Server
	engine *storage.Engine
	ts     *httptest.Server
}

// startCluster brings up n in-process nodes, all aware of each other, each
// on a fixed 127.0.0.1 address chosen up front so membership can be built
// before any server starts listening.
func startCluster(t *testing.T, n, replicationFactor int) []*testNode {
	t.Helper()

	listeners := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := 0; i < n; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	nodes := make([]*testNode, n)
	for i := 0; i < n; i++ {
		dir := t.TempDir()
		engine, err := storage.Open(storage.DefaultConfig(dir))
		if err != nil {
			t.Fatalf("open engine: %v", err)
		}
		t.Cleanup(func() { engine.Close() })

		var peers []string
		for j, a := range addrs {
			if j != i {
				peers = append(peers, a)
			}
		}
		membership := cluster.NewMembership(addrs[i], peers)
		server := api.NewServer(engine, membership, replicationFactor)

		ts := httptest.NewUnstartedServer(server.Handler())
		ts.Listener.Close()
		ts.Listener = listeners[i]
		ts.Start()
		t.Cleanup(ts.Close)

		nodes[i] = &testNode{addr: addrs[i], server: server, engine: engine, ts: ts}
	}
	return nodes
}

func httpPut(t *testing.T, addr, key, value string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPut, "http://"+addr+"/kv/"+key, strings.NewReader(value))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT %s: %v", key, err)
	}
	return resp
}

func httpGet(t *testing.T, addr, key string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/kv/" + key)
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(body)
}

// TestClusterWriteThenReadFromAnyNode writes a key via one node and confirms
// it can be read back through every node in the cluster, proving that
// requests to non-owning nodes are correctly proxied to the shard owner.
func TestClusterWriteThenReadFromAnyNode(t *testing.T) {
	nodes := startCluster(t, 3, 1)

	resp := httpPut(t, nodes[0].addr, "hello", "world")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	for _, n := range nodes {
		gresp, body := httpGet(t, n.addr, "hello")
		if gresp.StatusCode != http.StatusOK {
			t.Fatalf("GET from node %s: status %d", n.addr, gresp.StatusCode)
		}
		if body != "world" {
			t.Fatalf("GET from node %s: got %q, want %q", n.addr, body, "world")
		}
	}
}

// TestClusterReplicationSurvivesOwnerRead confirms that with replication
// factor 3 across 3 nodes, a key written through one node is present in
// every node's local engine (i.e. actually replicated, not just proxied).
func TestClusterReplicationSurvivesOwnerRead(t *testing.T) {
	nodes := startCluster(t, 3, 3)

	resp := httpPut(t, nodes[0].addr, "replicated-key", "value1")
	resp.Body.Close()

	// Replication happens asynchronously off the write path; give it a moment.
	deadline := time.Now().Add(2 * time.Second)
	for {
		allPresent := true
		for _, n := range nodes {
			if _, err := n.engine.Get("replicated-key"); err != nil {
				allPresent = false
				break
			}
		}
		if allPresent {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("replication did not reach all nodes within deadline")
		}
		time.Sleep(20 * time.Millisecond)
	}

	for _, n := range nodes {
		v, err := n.engine.Get("replicated-key")
		if err != nil || string(v) != "value1" {
			t.Fatalf("node %s local engine: got %q err=%v, want value1", n.addr, v, err)
		}
	}
}

// TestClusterDeletePropagates confirms deletes proxy/replicate the same way writes do.
func TestClusterDeletePropagates(t *testing.T) {
	nodes := startCluster(t, 2, 2)

	httpPut(t, nodes[0].addr, "temp", "x").Body.Close()

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err0 := nodes[0].engine.Get("temp"); err0 == nil {
			if _, err1 := nodes[1].engine.Get("temp"); err1 == nil {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("initial replication did not complete")
		}
		time.Sleep(20 * time.Millisecond)
	}

	req, _ := http.NewRequest(http.MethodDelete, "http://"+nodes[1].addr+"/kv/temp", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	resp.Body.Close()

	deadline = time.Now().Add(2 * time.Second)
	for {
		_, err0 := nodes[0].engine.Get("temp")
		_, err1 := nodes[1].engine.Get("temp")
		if err0 == storage.ErrNotFound && err1 == storage.ErrNotFound {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("delete did not propagate to all nodes")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestClusterStatusEndpoint sanity-checks the introspection endpoint.
func TestClusterStatusEndpoint(t *testing.T) {
	nodes := startCluster(t, 2, 1)
	resp, err := http.Get("http://" + nodes[0].addr + "/cluster/status")
	if err != nil {
		t.Fatalf("GET /cluster/status: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}
