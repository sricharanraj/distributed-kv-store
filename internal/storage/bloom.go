package storage

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// bloomFilter is a classic Bloom filter used per-SSTable so that a lookup
// for a key which isn't in that segment almost always avoids touching disk.
// It uses double hashing (Kirsch-Mitzenmacher) to derive k hash functions
// from two independent FNV hashes.
type bloomFilter struct {
	bits []uint64
	m    uint32 // number of bits
	k    uint32 // number of hash functions
}

// newBloomFilter sizes the filter for n expected entries at the given
// target false-positive rate p.
func newBloomFilter(n int, p float64) *bloomFilter {
	if n <= 0 {
		n = 1
	}
	m := optimalM(n, p)
	k := optimalK(m, n)
	words := (m + 63) / 64
	return &bloomFilter{bits: make([]uint64, words), m: uint32(m), k: uint32(k)}
}

func optimalM(n int, p float64) int {
	m := -1 * float64(n) * math.Log(p) / (math.Ln2 * math.Ln2)
	if m < 1 {
		m = 1
	}
	return int(math.Ceil(m))
}

func optimalK(m, n int) int {
	k := (float64(m) / float64(n)) * math.Ln2
	if k < 1 {
		k = 1
	}
	return int(math.Round(k))
}

func (b *bloomFilter) hashes(key string) (h1, h2 uint32) {
	f1 := fnv.New32a()
	f1.Write([]byte(key))
	h1 = f1.Sum32()

	f2 := fnv.New32()
	f2.Write([]byte(key))
	h2 = f2.Sum32()
	return
}

func (b *bloomFilter) Add(key string) {
	h1, h2 := b.hashes(key)
	for i := uint32(0); i < b.k; i++ {
		idx := (h1 + i*h2) % b.m
		b.bits[idx/64] |= 1 << (idx % 64)
	}
}

// MightContain returns false only when key is definitely absent.
func (b *bloomFilter) MightContain(key string) bool {
	h1, h2 := b.hashes(key)
	for i := uint32(0); i < b.k; i++ {
		idx := (h1 + i*h2) % b.m
		if b.bits[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}
	return true
}

// serialize/deserialize allow persisting the filter alongside its SSTable.
func (b *bloomFilter) serialize() []byte {
	buf := make([]byte, 8+len(b.bits)*8)
	binary.LittleEndian.PutUint32(buf[0:4], b.m)
	binary.LittleEndian.PutUint32(buf[4:8], b.k)
	for i, w := range b.bits {
		binary.LittleEndian.PutUint64(buf[8+i*8:], w)
	}
	return buf
}

func deserializeBloom(buf []byte) *bloomFilter {
	m := binary.LittleEndian.Uint32(buf[0:4])
	k := binary.LittleEndian.Uint32(buf[4:8])
	words := (len(buf) - 8) / 8
	bits := make([]uint64, words)
	for i := 0; i < words; i++ {
		bits[i] = binary.LittleEndian.Uint64(buf[8+i*8:])
	}
	return &bloomFilter{bits: bits, m: m, k: k}
}
