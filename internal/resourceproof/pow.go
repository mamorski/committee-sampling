package resourceproof

import (
	"encoding/binary"
	"fmt"

	hash2 "github.com/mamorski/committee-sampling/internal/hash"
)

// ProofOfWork interface
type ProofOfWork interface {
	Prove(challenge []byte, difficulty int) ([]byte, error)
	Verify(challenge []byte, difficulty int, proof []byte) bool
}

type pow struct{}

// Prove performs a Proof of Work by finding nonce such that
// the hash of (challenge || nonce) has `difficulty` leading zero bits.
func (p *pow) Prove(challenge []byte, difficulty int) ([]byte, error) {
	var nonce uint64 = 0
	var proof []byte

	for {
		hash := hash2.Sum(challenge, uint64ToBytes(nonce))

		if hasLeadingZeros(hash[:], difficulty) {
			proof = uint64ToBytes(nonce)
			break
		}

		nonce++
		if nonce == 0 {
			return nil, fmt.Errorf("proof of work failed (overflow)")
		}
	}

	return proof, nil
}

// Verify checks if the given proof is valid for the given challenge and difficulty.
func (p *pow) Verify(challenge []byte, difficulty int, proof []byte) bool {
	hash := hash2.Sum(challenge, proof)

	return hasLeadingZeros(hash[:], difficulty)
}

func uint64ToBytes(num uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, num)
	return buf
}

func hasLeadingZeros(hash []byte, difficulty int) bool {
	bits := 0

	for _, b := range hash {
		for i := 7; i >= 0; i-- {
			if b&(1<<i) != 0 {
				return bits >= difficulty
			}
			bits++
		}
	}

	return bits >= difficulty
}
