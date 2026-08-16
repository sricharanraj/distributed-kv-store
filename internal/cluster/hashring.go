// Package cluster implements sharding (consistent hashing), membership, and
// replication across multiple KV store nodes.
package cluster

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// virtualNodesPerNode controls how many points each physical node owns on
// the ring. More virtual nodes gives a more even key distribution at the
// cost of a bigger ring to search.
const virtualNodesPerNode = 150

// HashRing implements consistent hashing with virtual nodes so that adding
// or removing a node only reshuffles a small fraction of keys (unlike plain
// hash % N sharding, where every key moves).
type HashRing struct {
	mu       sync.RWMutex
	points   []uint32          // sorted virtual node hashes
	owners   map[uint32]string // virtual node hash -> node ID
	nodes    map[string]bool   // node ID -> present
}

func NewHashRing() *HashRing {
	return &HashRing{owners: map[uint32]string{}, nodes: map[string]bool{}}
}

func hashKey(s string) uint32 {
	sum := sha1.Sum([]byte(s))
	return binary.BigEndian.Uint32(sum[:4])
}

// AddNode adds a physical node (identified by its address, e.g. "10.0.0.1:8080")
// and its virtual nodes to the ring.
func (r *HashRing) AddNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nodes[id] {
		return
	}
	r.nodes[id] = true
	for i := 0; i < virtualNodesPerNode; i++ {
		h := hashKey(fmt.Sprintf("%s#%d", id, i))
		r.owners[h] = id
		r.points = append(r.points, h)
	}
	sort.Slice(r.points, func(i, j int) bool { return r.points[i] < r.points[j] })
}

// RemoveNode removes a physical node and all of its virtual nodes.
func (r *HashRing) RemoveNode(id string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.nodes[id] {
		return
	}
	delete(r.nodes, id)
	kept := r.points[:0]
	for _, h := range r.points {
		if r.owners[h] == id {
			delete(r.owners, h)
			continue
		}
		kept = append(kept, h)
	}
	r.points = kept
}

// Get returns the node owning key: the first virtual node clockwise from
// hash(key) on the ring.
func (r *HashRing) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return "", false
	}
	h := hashKey(key)
	idx := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })
	if idx == len(r.points) {
		idx = 0
	}
	return r.owners[r.points[idx]], true
}

// GetN returns up to n distinct physical nodes for key, walking clockwise
// around the ring. The first result is the primary/owner; the rest are
// replica targets. Used to place a key's N replicas.
func (r *HashRing) GetN(key string, n int) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.points) == 0 {
		return nil
	}
	h := hashKey(key)
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= h })

	seen := map[string]bool{}
	var out []string
	for i := 0; i < len(r.points) && len(out) < n; i++ {
		idx := (start + i) % len(r.points)
		id := r.owners[r.points[idx]]
		if !seen[id] {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// Nodes returns the current set of physical node IDs.
func (r *HashRing) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
