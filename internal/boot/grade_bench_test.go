package boot

import (
	stdsync "sync"
	"testing"

	"go.uber.org/zap"
)

// phiVRF is a representative VRF output (interpreted as a big-endian integer > 0).
var phiVRF = []byte{
	0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
	0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
	0x0f, 0x1e, 0x2d, 0x3c, 0x4b, 0x5a, 0x69, 0x78,
	0x87, 0x96, 0xa5, 0xb4, 0xc3, 0xd2, 0xe1, 0xf0,
}

const (
	benchD      = 4
	benchN      = 100
	benchLambda = 256
	benchWi     = 1000.0
	benchDeltaW = 10.0
)

// BenchmarkComputeGrade measures one raw grade computation — the big.Int/big.Float
// work that ran on every gossiped message before memoization. Run:
// go test -bench . -benchmem ./internal/boot/
func BenchmarkComputeGrade(b *testing.B) {
	logger := zap.NewNop()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = computeGrade(benchD, benchN, benchLambda, benchWi, benchDeltaW, phiVRF, logger)
	}
}

// BenchmarkGradeCacheHit measures the per-message cost once a member's grade is
// cached: build the key (string(φ)) and a sync.Map load. This is what the gradeF
// closure does on the second-and-onward call for any member, so the gap against
// BenchmarkComputeGrade is the saved work per redundant message.
func BenchmarkGradeCacheHit(b *testing.B) {
	var cache stdsync.Map
	cache.Store(string(phiVRF), 5)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if v, ok := cache.Load(string(phiVRF)); ok {
			_ = v.(int)
		}
	}
}
