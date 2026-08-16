// Package benchmark measures storage-engine throughput. Run with:
//
//	go test ./benchmark/... -bench=. -benchmem
package benchmark

import (
	"fmt"
	"testing"

	"github.com/sricharanraj/distributed-kv-store/internal/storage"
)

func newBenchEngine(b *testing.B) *storage.Engine {
	b.Helper()
	e, err := storage.Open(storage.DefaultConfig(b.TempDir()))
	if err != nil {
		b.Fatalf("open engine: %v", err)
	}
	b.Cleanup(func() { e.Close() })
	return e
}

func BenchmarkEnginePut(b *testing.B) {
	e := newBenchEngine(b)
	val := []byte("benchmark-value-000000000000")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Put(fmt.Sprintf("key-%d", i), val)
	}
}

func BenchmarkEngineGetHit(b *testing.B) {
	e := newBenchEngine(b)
	val := []byte("benchmark-value-000000000000")
	const n = 10000
	for i := 0; i < n; i++ {
		e.Put(fmt.Sprintf("key-%d", i), val)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get(fmt.Sprintf("key-%d", i%n))
	}
}

func BenchmarkEngineGetMiss(b *testing.B) {
	e := newBenchEngine(b)
	val := []byte("benchmark-value-000000000000")
	for i := 0; i < 10000; i++ {
		e.Put(fmt.Sprintf("key-%d", i), val)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Get(fmt.Sprintf("missing-%d", i))
	}
}

func BenchmarkEngineMixedReadWrite(b *testing.B) {
	e := newBenchEngine(b)
	val := []byte("benchmark-value-000000000000")
	for i := 0; i < 1000; i++ {
		e.Put(fmt.Sprintf("key-%d", i), val)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%10 == 0 {
			e.Put(fmt.Sprintf("key-%d", i%1000), val)
		} else {
			e.Get(fmt.Sprintf("key-%d", i%1000))
		}
	}
}
