package mdag

import (
	"testing"

	"github.com/mamorski/committee-sampling/internal/hash"
)

// BenchmarkOracle exercises the pooled-buffer concat+hash path. Oracle is called per
// merkle layer per gossiped message; the buffer now comes from a sync.Pool so the
// per-call buffer allocation is gone. Run: go test -bench . -benchmem ./internal/mdag/
func BenchmarkOracle(b *testing.B) {
	m := &MDAG{oracle: HashOracle(hash.Oracle)}
	layer := [][]byte{
		[]byte("label-row-element-one-0123456789"),
		[]byte("label-row-element-two-abcdefghij"),
		[]byte("label-row-element-three-klmnopqr"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Oracle(layer...)
	}
}

// BenchmarkOracleParallel surfaces pool contention under concurrent verify workers.
func BenchmarkOracleParallel(b *testing.B) {
	m := &MDAG{oracle: HashOracle(hash.Oracle)}
	layer := [][]byte{
		[]byte("label-row-element-one-0123456789"),
		[]byte("label-row-element-two-abcdefghij"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = m.Oracle(layer...)
		}
	})
}
