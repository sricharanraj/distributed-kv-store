package cluster

import (
	"fmt"
	"testing"
)

func TestHashRingGetIsStable(t *testing.T) {
	r := NewHashRing()
	r.AddNode("node-a")
	r.AddNode("node-b")
	r.AddNode("node-c")

	owner1, ok := r.Get("some-key")
	if !ok {
		t.Fatalf("expected an owner for some-key")
	}
	owner2, _ := r.Get("some-key")
	if owner1 != owner2 {
		t.Fatalf("Get should be deterministic: got %q then %q", owner1, owner2)
	}
}

func TestHashRingDistributesKeysAcrossNodes(t *testing.T) {
	r := NewHashRing()
	nodes := []string{"node-a", "node-b", "node-c", "node-d"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	counts := map[string]int{}
	const numKeys = 10000
	for i := 0; i < numKeys; i++ {
		owner, _ := r.Get(fmt.Sprintf("key-%d", i))
		counts[owner]++
	}

	if len(counts) != len(nodes) {
		t.Fatalf("expected all %d nodes to own at least one key, got %d nodes with keys: %v", len(nodes), len(counts), counts)
	}
	// With 150 virtual nodes per physical node, distribution should be
	// roughly even; assert no node gets a wildly disproportionate share.
	expected := numKeys / len(nodes)
	for node, c := range counts {
		if c < expected/2 || c > expected*2 {
			t.Errorf("node %s got %d keys, expected roughly %d (allowing 2x skew)", node, c, expected)
		}
	}
}

func TestHashRingRemoveNodeMovesOnlyItsKeys(t *testing.T) {
	r := NewHashRing()
	nodes := []string{"node-a", "node-b", "node-c"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	const numKeys = 2000
	before := make(map[string]string, numKeys)
	for i := 0; i < numKeys; i++ {
		k := fmt.Sprintf("key-%d", i)
		owner, _ := r.Get(k)
		before[k] = owner
	}

	r.RemoveNode("node-b")

	moved := 0
	for k, oldOwner := range before {
		newOwner, _ := r.Get(k)
		if newOwner == "node-b" {
			t.Fatalf("removed node still owns key %q", k)
		}
		if newOwner != oldOwner {
			moved++
		}
	}
	// Only keys that were owned by node-b should have moved (consistent
	// hashing's whole point); allow a little slack for boundary effects.
	if moved > numKeys/2 {
		t.Errorf("too many keys moved after removing one of three nodes: %d/%d", moved, numKeys)
	}
}

func TestHashRingGetNReturnsDistinctNodes(t *testing.T) {
	r := NewHashRing()
	for _, n := range []string{"a", "b", "c", "d"} {
		r.AddNode(n)
	}
	owners := r.GetN("some-key", 3)
	if len(owners) != 3 {
		t.Fatalf("expected 3 owners, got %d: %v", len(owners), owners)
	}
	seen := map[string]bool{}
	for _, o := range owners {
		if seen[o] {
			t.Fatalf("GetN returned duplicate owner %q: %v", o, owners)
		}
		seen[o] = true
	}
}

func TestHashRingGetNCappedAtNodeCount(t *testing.T) {
	r := NewHashRing()
	r.AddNode("only-node")
	owners := r.GetN("key", 5)
	if len(owners) != 1 {
		t.Fatalf("expected 1 owner when only 1 node exists, got %v", owners)
	}
}

func TestHashRingEmptyRing(t *testing.T) {
	r := NewHashRing()
	if _, ok := r.Get("key"); ok {
		t.Fatalf("expected no owner on empty ring")
	}
	if owners := r.GetN("key", 3); owners != nil {
		t.Fatalf("expected nil owners on empty ring, got %v", owners)
	}
}
