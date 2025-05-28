package resourceproof

import (
	"testing"
)

func TestProofOfWork(t *testing.T) {
	pow := &pow{}
	challenge := []byte("test challenge")
	difficulty := 16

	proof, err := pow.Prove(challenge, difficulty)
	if err != nil {
		t.Fatalf("Prove failed: %v", err)
	}

	valid := pow.Verify(challenge, difficulty, proof)
	if !valid {
		t.Fatal("Expected valid proof, but got invalid")
	}
}

func TestVerify_InvalidProof(t *testing.T) {
	pow := &pow{}
	challenge := []byte("test challenge")
	difficulty := 16
	proof, _ := pow.Prove(challenge, difficulty)

	proof[0] ^= 0xFF
	valid := pow.Verify(challenge, difficulty, proof)
	if valid {
		t.Fatal("Expected invalid proof, but got valid")
	}
}
