package hash

import (
	"golang.org/x/crypto/blake2b"
)

func Sum(data ...[]byte) []byte {
	hasher, _ := blake2b.New256(nil)
	for _, d := range data {
		hasher.Write(d)
	}

	return hasher.Sum(nil)
}

func Oracle(data []byte) []byte {
	hash := blake2b.Sum256(data)
	return hash[:]
}
