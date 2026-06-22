package hash

import (
	"hash"
	"sync"

	"golang.org/x/crypto/blake2b"
)

// hasherPool reuses blake2b hashers across the hot verify path. Sum is called per
// merkle layer per gossiped message; allocating a fresh hasher each call dominated
// GC pressure. A pooled hasher (Reset on get) keeps the result slice as the only
// per-call allocation.
var hasherPool = sync.Pool{
	New: func() any {
		h, _ := blake2b.New256(nil)
		return h
	},
}

func Sum(data ...[]byte) []byte {
	hasher := hasherPool.Get().(hash.Hash)
	hasher.Reset()
	for _, d := range data {
		hasher.Write(d)
	}
	sum := hasher.Sum(nil)
	hasherPool.Put(hasher)

	return sum
}

func Oracle(data []byte) []byte {
	hash := blake2b.Sum256(data)
	return hash[:]
}
