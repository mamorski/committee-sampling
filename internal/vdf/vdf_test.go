package vdf

import (
	"testing"
	"time"
)

// Test VDF Setup function
func TestSetup(t *testing.T) {
	v := New()

	// Call Setup with λ = 256 bits and δ = 1 second
	vk, err := v.Setup(256, 1)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	if len(vk) != 32 {
		t.Fatalf("Expected VK length 32 bytes, got %d bytes", len(vk))
	}
}

// Test VDF Eval function with valid input
func TestEval(t *testing.T) {
	v := New()

	// Setup with λ = 256 bits and δ = 1 second
	vk, err := v.Setup(256, 1)
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	x := []byte("test input")
	start := time.Now()
	phi, pi, err := v.Eval(x, vk, 1)
	if err != nil {
		t.Fatalf("Eval failed: %v", err)
	}

	elapsed := time.Since(start)
	if elapsed < time.Second {
		t.Fatalf("Eval completed too quickly: %v", elapsed)
	}

	if len(phi) == 0 || len(pi) == 0 {
		t.Fatalf("Eval output or proof is empty")
	}
}

// Test Verify function with correct output and proof
func TestVerify_Valid(t *testing.T) {
	v := New()

	vk, _ := v.Setup(256, 1)
	x := []byte("test input")
	phi, pi, _ := v.Eval(x, vk, 1)

	valid, err := v.Verify(x, phi, pi, vk)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	if !valid {
		t.Fatalf("Expected valid proof, but got invalid")
	}
}

// Test Verify function with incorrect output
func TestVerify_InvalidPhi(t *testing.T) {
	v := New()

	vk, _ := v.Setup(256, 1)
	x := []byte("test input")
	phi, pi, _ := v.Eval(x, vk, 1)

	// Tamper with phi to make it invalid
	phi[0] ^= 0xFF

	valid, err := v.Verify(x, phi, pi, vk)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	if valid {
		t.Fatalf("Expected invalid proof due to tampered phi, but got valid")
	}
}

// Test Verify function with incorrect proof
func TestVerify_InvalidPi(t *testing.T) {
	v := New()

	vk, _ := v.Setup(256, 1)
	x := []byte("test input")
	phi, pi, _ := v.Eval(x, vk, 1)

	// Tamper with proof to make it invalid
	pi[0] ^= 0xFF

	valid, err := v.Verify(x, phi, pi, vk)
	if err != nil {
		t.Fatalf("Verify failed: %v", err)
	}

	if valid {
		t.Fatalf("Expected invalid proof due to tampered pi, but got valid")
	}
}
