package resourcebound

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/mamorski/committee-sampling/internal/hash"
	"github.com/mamorski/committee-sampling/internal/resourceproof"
	"github.com/mamorski/committee-sampling/internal/vdf"
	"github.com/mamorski/committee-sampling/internal/vrf"
	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

// fTagFixture is one valid proof plus the real verifiers, mirroring the
// production filterF (bootstrap.go) + rp.Ver wrapped by fTag. It lets us
// benchmark the real verification cost against a cache lookup, and unit-test
// the cache key.
type fTagFixture struct {
	sid, id string
	vk, ch  []byte
	aux     *pb.Aux
	weight  float64
	rp      *resourceproof.ResourceProof
	vdfFn   *vdf.Vdf
	vrfFn   *vrf.Vrf
}

func newFTagFixture(tb testing.TB) *fTagFixture {
	tb.Helper()
	logger := zap.NewNop()
	vrfFn := vrf.New(logger)
	vdfFn := vdf.New(logger)
	rp := resourceproof.New(logger)

	sid := "bench-session"
	id := "12D3KooWBenchNodeIdentifierExampleStringAaBbCc"
	ch := []byte("challenge-value-for-benchmark")
	weight := 1.0

	sk, vk, err := vrfFn.Generate(256)
	require.NoError(tb, err)

	// VDF over H(id, vk, ch); delay 0 avoids the eval sleep during setup.
	vdfInput := hash.Sum([]byte(id), vk, ch)
	phiVdf, piVdf, err := vdfFn.Eval(vdfInput, vk, 0)
	require.NoError(tb, err)

	// VRF over H(phiVdf, sid).
	vrfInput := hash.Sum(phiVdf, []byte(sid))
	phiVrf, piVrf, err := vrfFn.Eval(vrfInput, sk)
	require.NoError(tb, err)

	// Resource proof (PoW with difficulty = int(weight)).
	piRP, err := rp.Prove(vk, weight, ch, nil)
	require.NoError(tb, err)

	aux := &pb.Aux{
		PiRP: piRP,
		AuxKey: &pb.AuxKeyMessage{
			PhiVrf: phiVrf,
			PiVrf:  piVrf,
			PhiVdf: phiVdf,
			PiVdf:  piVdf,
		},
	}
	return &fTagFixture{sid: sid, id: id, vk: vk, ch: ch, aux: aux, weight: weight, rp: rp, vdfFn: vdfFn, vrfFn: vrfFn}
}

// computeFTag replicates production filterF + rp.Ver: the real per-message
// verification cost the cache avoids (x509 parse + ECVRF + VDF + PoW).
func (f *fTagFixture) computeFTag() bool {
	ak := f.aux.AuxKey
	if !f.rp.Ver(f.vk, f.weight, f.ch, f.aux.PiRP) {
		return false
	}
	vdfInput := hash.Sum([]byte(f.id), f.vk, f.ch)
	ok, err := f.vdfFn.Verify(vdfInput, ak.PhiVdf, ak.PiVdf, f.vk)
	if err != nil || !ok {
		return false
	}
	vrfInput := hash.Sum(ak.PhiVdf, []byte(f.sid))
	ok, err = f.vrfFn.Verify(vrfInput, ak.PhiVrf, ak.PiVrf, f.vk)
	if err != nil || !ok {
		return false
	}
	return true
}

// BenchmarkFTagCompute measures recomputing fTag every message (the storm).
func BenchmarkFTagCompute(b *testing.B) {
	f := newFTagFixture(b)
	require.True(b, f.computeFTag())
	b.ReportAllocs()
	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		ok = f.computeFTag()
	}
	if !ok {
		b.Fatal("expected valid proof")
	}
}

// BenchmarkFTagCacheHit measures the replacement: build the key + sync.Map hit.
func BenchmarkFTagCacheHit(b *testing.B) {
	f := newFTagFixture(b)
	var cache sync.Map
	cache.Store(fTagCacheKey(f.id, f.vk, f.ch, f.aux), true)
	b.ReportAllocs()
	b.ResetTimer()
	var ok bool
	for i := 0; i < b.N; i++ {
		v, found := cache.Load(fTagCacheKey(f.id, f.vk, f.ch, f.aux))
		ok = found && v.(bool)
	}
	if !ok {
		b.Fatal("expected cache hit")
	}
}
