package resource_proof

import "fmt"

type ResourceProof interface {
	Setup(vk []byte) ([]byte, error)
	Prove(vk []byte, omega, ch int, aux []byte) ([]byte, int, int, []byte, error)
	Ver(vk []byte, omega, ch int, pi []byte) bool
}

type resourceProof struct {
	proofOfWork ProofOfWork
}

// lint:ignore U1000 This function is not used with PoW
func (r *resourceProof) Setup(_ []byte) ([]byte, error) {
	fmt.Println("Unused function")
	return nil, nil
}

func (r *resourceProof) Prove(vk []byte, omega, ch int, _ []byte) ([]byte, int, int, []byte, error) {
	challenge := []byte(string(vk) + string(rune(ch)))
	proof, err := r.proofOfWork.Prove(challenge, omega)
	if err != nil {
		return nil, 0, 0, nil, fmt.Errorf("failed to prove: %w", err)
	}

	return vk, omega, ch, proof, nil
}

func (r *resourceProof) Ver(vk []byte, omega, ch int, pi []byte) bool {
	challenge := []byte(string(vk) + string(rune(ch)))
	return r.proofOfWork.Verify(challenge, omega, pi)
}

func New() ResourceProof {
	return &resourceProof{
		proofOfWork: &pow{},
	}
}
