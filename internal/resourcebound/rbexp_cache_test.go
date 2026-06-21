package resourcebound

import (
	"testing"

	"github.com/stretchr/testify/require"

	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

func cloneAux(a *pb.Aux) *pb.Aux {
	cp := func(b []byte) []byte { return append([]byte(nil), b...) }
	return &pb.Aux{
		PiRP: cp(a.PiRP),
		AuxKey: &pb.AuxKeyMessage{
			PhiVrf: cp(a.AuxKey.PhiVrf),
			PiVrf:  cp(a.AuxKey.PiVrf),
			PhiVdf: cp(a.AuxKey.PhiVdf),
			PiVdf:  cp(a.AuxKey.PiVdf),
		},
	}
}

// Identical inputs must produce an identical key (cache hits work).
func TestFTagCacheKey_Deterministic(t *testing.T) {
	f := newFTagFixture(t)
	require.Equal(t,
		fTagCacheKey(f.id, f.vk, f.ch, f.aux),
		fTagCacheKey(f.id, f.vk, f.ch, f.aux),
	)
}

// Flipping any verification-determining field must change the key, so a
// tampered message can never read a cached `true` for a different (invalid)
// proof — it misses the cache and is re-verified/rejected.
func TestFTagCacheKey_TamperChangesKey(t *testing.T) {
	f := newFTagFixture(t)
	base := fTagCacheKey(f.id, f.vk, f.ch, f.aux)

	tamper := func(name string, mutate func(a *pb.Aux)) {
		a := cloneAux(f.aux)
		mutate(a)
		require.NotEqual(t, base, fTagCacheKey(f.id, f.vk, f.ch, a), "key must change when %s is tampered", name)
	}
	tamper("PiRP", func(a *pb.Aux) { a.PiRP[0] ^= 0xFF })
	tamper("PhiVrf", func(a *pb.Aux) { a.AuxKey.PhiVrf[0] ^= 0xFF })
	tamper("PiVrf", func(a *pb.Aux) { a.AuxKey.PiVrf[0] ^= 0xFF })
	tamper("PhiVdf", func(a *pb.Aux) { a.AuxKey.PhiVdf[0] ^= 0xFF })
	tamper("PiVdf", func(a *pb.Aux) { a.AuxKey.PiVdf[0] ^= 0xFF })

	// id / vk / ch are also part of the key.
	require.NotEqual(t, base, fTagCacheKey(f.id+"x", f.vk, f.ch, f.aux))
	require.NotEqual(t, base, fTagCacheKey(f.id, append(append([]byte(nil), f.vk...), 0x01), f.ch, f.aux))
	require.NotEqual(t, base, fTagCacheKey(f.id, f.vk, append(f.ch, 0x01), f.aux))
}

// The length-prefix framing must be injective even when field boundaries shift
// (binary fields can contain the separator byte a naive join would use).
func TestFTagCacheKey_InjectiveFraming(t *testing.T) {
	f := newFTagFixture(t)
	a := cloneAux(f.aux)
	// Move one byte from the end of vk into the start of ch: a naive
	// concatenation would collide; length-prefixed framing must not.
	vk1 := append([]byte(nil), f.vk...)
	ch1 := append([]byte(nil), f.ch...)
	moved := vk1[len(vk1)-1]
	vk2 := vk1[:len(vk1)-1]
	ch2 := append([]byte{moved}, ch1...)

	require.NotEqual(t,
		fTagCacheKey(f.id, vk1, ch1, a),
		fTagCacheKey(f.id, vk2, ch2, a),
	)
}
