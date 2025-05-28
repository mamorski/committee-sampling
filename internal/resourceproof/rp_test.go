package resourceproof

import (
	"errors"
	"testing"
)

// Mock implementation of the ProofOfWork interface for testing.
type mockProofOfWork struct {
	shouldFail bool
}

// Prove function mock: Returns a valid or failed proof based on shouldFail.
func (m *mockProofOfWork) Prove(_ []byte, _ int) ([]byte, error) {
	if m.shouldFail {
		return nil, errors.New("mocked proof failure")
	}
	return []byte("valid_proof"), nil
}

// Verify function mock: Always returns true if the proof is "valid_proof".
func (m *mockProofOfWork) Verify(_ []byte, _ int, proof []byte) bool {
	return string(proof) == "valid_proof"
}

// Test Prove function with a successful mock PoW.
func TestProve_Success(t *testing.T) {
	rp := &ResourceProof{
		proofOfWork: &mockProofOfWork{shouldFail: false},
	}

	vk := []byte("test_vk")
	omega := 5.0
	ch := []byte("ch")
	aux := []byte("aux_data")

	proof, err := rp.Prove(vk, omega, ch, aux)
	if err != nil {
		t.Fatalf("Prove failed: %v", err)
	}

	if string(proof) != "valid_proof" {
		t.Fatalf("Expected proof 'valid_proof', got '%s'", string(proof))
	}
}

// Test Prove function with a failing mock PoW.
func TestProve_Failure(t *testing.T) {
	rp := &ResourceProof{
		proofOfWork: &mockProofOfWork{shouldFail: true},
	}

	vk := []byte("test_vk")
	omega := 5.0
	ch := []byte("ch")
	aux := []byte("aux_data")

	_, err := rp.Prove(vk, omega, ch, aux)
	if err == nil {
		t.Fatal("Expected error from Prove, but got none")
	}
}

// Test Verify function with valid inputs.
func TestVerify_Success(t *testing.T) {
	rp := &ResourceProof{
		proofOfWork: &mockProofOfWork{shouldFail: false},
	}

	vk := []byte("test_vk")
	omega := 5.0
	ch := []byte("ch")
	proof := []byte("valid_proof")

	if !rp.Ver(vk, omega, ch, proof) {
		t.Fatal("Expected Verify to return true, but got false")
	}
}

// Test Verify function with an invalid proof.
func TestVerify_Failure(t *testing.T) {
	rp := &ResourceProof{
		proofOfWork: &mockProofOfWork{shouldFail: false},
	}

	vk := []byte("test_vk")
	omega := 5.0
	ch := []byte("ch")
	proof := []byte("invalid_proof")

	if rp.Ver(vk, omega, ch, proof) {
		t.Fatal("Expected Verify to return false, but got true")
	}
}
