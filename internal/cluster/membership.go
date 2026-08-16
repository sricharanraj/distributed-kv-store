package cluster

// Membership tracks the static (or dynamically joined) set of peer
// addresses that make up the cluster. This project uses a simple
// statically-configured membership list rather than a gossip protocol:
// each node is started with the full peer list, which is enough to
// demonstrate sharding and replication without the added complexity of
// failure detection / gossip convergence.
type Membership struct {
	Self string
	Ring *HashRing
}

// NewMembership builds a ring containing self plus every peer.
func NewMembership(self string, peers []string) *Membership {
	ring := NewHashRing()
	ring.AddNode(self)
	for _, p := range peers {
		if p != "" && p != self {
			ring.AddNode(p)
		}
	}
	return &Membership{Self: self, Ring: ring}
}

// IsSelf reports whether nodeID refers to this node.
func (m *Membership) IsSelf(nodeID string) bool {
	return nodeID == m.Self
}
