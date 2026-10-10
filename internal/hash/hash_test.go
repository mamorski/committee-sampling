package hash

import "testing"

// BenchmarkSum exercises the pooled-hasher hot path. ReportAllocs should show the
// only per-call allocation is the 32-byte digest returned by Sum (the hasher itself
// comes from the pool). Run: go test -bench . -benchmem ./internal/hash/
func BenchmarkSum(b *testing.B) {
	data := [][]byte{
		[]byte("session-id-0123456789"),
		[]byte("verification-key-bytes-abcdef"),
		[]byte("challenge-value"),
		[]byte("pi-rp-proof-bytes"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Sum(data...)
	}
}

// BenchmarkSumParallel mirrors the real workload: many goroutines (threadpool
// workers across nodes) hammering Sum concurrently, so it also surfaces pool
// contention.
func BenchmarkSumParallel(b *testing.B) {
	data := [][]byte{
		[]byte("session-id-0123456789"),
		[]byte("verification-key-bytes-abcdef"),
		[]byte("challenge-value"),
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = Sum(data...)
		}
	})
}
