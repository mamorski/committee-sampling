package resourceproof

import (
	"bytes"
	"fmt"
)

type ResourceProof struct {
	proofOfWork ProofOfWork
}

// lint:ignore U1000 This function is not used with PoW
func (r *ResourceProof) Setup(_ []byte) ([]byte, error) {
	return nil, nil
}

func (r *ResourceProof) Prove(vk []byte, omega float64, ch []byte, _ []byte) ([]byte, error) {
	var challenge bytes.Buffer
	challenge.Write(vk)
	challenge.Write(ch)
	proof, err := r.proofOfWork.Prove(challenge.Bytes(), int(omega))
	if err != nil {
		return nil, fmt.Errorf("failed to prove: %w", err)
	}

	return proof, nil
}

func (r *ResourceProof) Ver(vk []byte, omega float64, ch []byte, pi []byte) bool {
	var challenge bytes.Buffer
	challenge.Write(vk)
	challenge.Write(ch)
	return r.proofOfWork.Verify(challenge.Bytes(), int(omega), pi)
}

func New() *ResourceProof {
	return &ResourceProof{
		proofOfWork: &pow{},
	}
}
