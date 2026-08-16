package storage

import (
	"fmt"
	"testing"
)

func TestBloomFilterNoFalseNegatives(t *testing.T) {
	bf := newBloomFilter(1000, 0.01)
	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key-%d", i)
		bf.Add(keys[i])
	}
	for _, k := range keys {
		if !bf.MightContain(k) {
			t.Fatalf("bloom filter false negative for %q (must never happen)", k)
		}
	}
}

func TestBloomFilterFalsePositiveRateIsReasonable(t *testing.T) {
	n := 5000
	bf := newBloomFilter(n, 0.01)
	for i := 0; i < n; i++ {
		bf.Add(fmt.Sprintf("present-%d", i))
	}

	fp := 0
	trials := 5000
	for i := 0; i < trials; i++ {
		if bf.MightContain(fmt.Sprintf("absent-%d", i)) {
			fp++
		}
	}
	rate := float64(fp) / float64(trials)
	// Configured for 1% FP rate; allow generous slack for hash variance.
	if rate > 0.05 {
		t.Fatalf("false positive rate too high: %f (%d/%d)", rate, fp, trials)
	}
}

func TestBloomFilterSerializeRoundTrip(t *testing.T) {
	bf := newBloomFilter(100, 0.01)
	bf.Add("hello")
	bf.Add("world")

	buf := bf.serialize()
	bf2 := deserializeBloom(buf)

	if !bf2.MightContain("hello") || !bf2.MightContain("world") {
		t.Fatalf("deserialized filter lost membership")
	}
}
