// Package storage implements the on-disk and in-memory storage engine:
// a skip-list memtable, write-ahead log, sorted-string-table (SSTable)
// segments on disk, and bloom filters to make negative lookups cheap.
package storage

import (
	"math/rand"
)

const maxSkipListLevel = 16

// skipListNode is a single node in the skip list.
type skipListNode struct {
	key      string
	value    []byte
	tombstone bool
	forward  []*skipListNode
}

// skipList is a sorted, in-memory structure used as the active memtable.
// It is NOT safe for concurrent use on its own; callers (engine.go) guard
// it with a sync.RWMutex.
type skipList struct {
	head   *skipListNode
	level  int
	size   int // number of entries (including tombstones)
	bytes  int // approximate size in bytes, used for flush threshold
	rnd    *rand.Rand
}

func newSkipList() *skipList {
	return &skipList{
		head:  &skipListNode{forward: make([]*skipListNode, maxSkipListLevel)},
		level: 1,
		rnd:   rand.New(rand.NewSource(rand.Int63())),
	}
}

func (s *skipList) randomLevel() int {
	lvl := 1
	for lvl < maxSkipListLevel && s.rnd.Float64() < 0.5 {
		lvl++
	}
	return lvl
}

// Put inserts or overwrites key with value, clearing any tombstone.
func (s *skipList) Put(key string, value []byte) {
	s.insert(key, value, false)
}

// Delete inserts a tombstone marker for key.
func (s *skipList) Delete(key string) {
	s.insert(key, nil, true)
}

func (s *skipList) insert(key string, value []byte, tombstone bool) {
	update := make([]*skipListNode, maxSkipListLevel)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
		update[i] = x
	}
	x = x.forward[0]
	if x != nil && x.key == key {
		s.bytes += len(value) - len(x.value)
		x.value = value
		x.tombstone = tombstone
		return
	}

	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	node := &skipListNode{key: key, value: value, tombstone: tombstone, forward: make([]*skipListNode, lvl)}
	for i := 0; i < lvl; i++ {
		node.forward[i] = update[i].forward[i]
		update[i].forward[i] = node
	}
	s.size++
	s.bytes += len(key) + len(value) + 1
}

// Get returns the value, whether the key exists, and whether it is a tombstone.
func (s *skipList) Get(key string) (value []byte, found bool, tombstone bool) {
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && x.forward[i].key < key {
			x = x.forward[i]
		}
	}
	x = x.forward[0]
	if x != nil && x.key == key {
		return x.value, true, x.tombstone
	}
	return nil, false, false
}

// entry is a materialized (key, value, tombstone) triple.
type entry struct {
	key       string
	value     []byte
	tombstone bool
}

// All returns every entry in ascending key order, used when flushing to an SSTable.
func (s *skipList) All() []entry {
	out := make([]entry, 0, s.size)
	for x := s.head.forward[0]; x != nil; x = x.forward[0] {
		out = append(out, entry{key: x.key, value: x.value, tombstone: x.tombstone})
	}
	return out
}
