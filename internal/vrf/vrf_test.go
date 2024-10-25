package vrf

import (
	"crypto/elliptic"
	"testing"
)

func TestGen(t *testing.T) {
	vrfInstance := New()

	// Test with a valid lambda (128, 192, 256).
	for _, lambda := range []int{128, 192, 256} {
		secretKey, verificationKey, err := vrfInstance.Gen(lambda)
		if err != nil {
			t.Fatalf("Gen(%d) failed: %v", lambda, err)
		}

		if len(secretKey) == 0 {
			t.Errorf("Gen(%d) returned empty secret key", lambda)
		}
		if len(verificationKey) == 0 {
			t.Errorf("Gen(%d) returned empty verification key", lambda)
		}
	}

	// Test with an invalid lambda.
	_, _, err := vrfInstance.Gen(512)
	if err == nil {
		t.Error("Gen(512) should have failed, but it did not")
	}
}

// TestEval tests the VRF evaluation for all supported lambda values.
func TestEval(t *testing.T) {
	vrfInstance := New()

	// Test all suitable lambda values: 128, 192, 256.
	for _, lambda := range []int{128, 192, 256} {
		t.Run(lambdaToCurveName(lambda), func(t *testing.T) {
			// Generate key pair.
			secretKey, _, err := vrfInstance.Gen(lambda)
			if err != nil {
				t.Fatalf("Gen(%d) failed: %v", lambda, err)
			}

			// Evaluate VRF with a sample input.
			input := "test-input"
			phi, pi, err := vrfInstance.Eval(input, secretKey)
			if err != nil {
				t.Fatalf("Eval(%d) failed: %v", lambda, err)
			}

			if len(phi) == 0 || len(pi) == 0 {
				t.Errorf("Eval(%d) returned empty phi or pi", lambda)
			}
		})
	}
}

// TestVerify tests the VRF verification for all supported lambda values.
func TestVerify(t *testing.T) {
	vrfInstance := New()

	// Test all suitable lambda values: 128, 192, 256.
	for _, lambda := range []int{128, 192, 256} {
		t.Run(lambdaToCurveName(lambda), func(t *testing.T) {
			// Generate key pair.
			secretKey, verificationKey, err := vrfInstance.Gen(lambda)
			if err != nil {
				t.Fatalf("Gen(%d) failed: %v", lambda, err)
			}

			// Evaluate VRF with a sample input.
			input := "test-input"
			phi, pi, err := vrfInstance.Eval(input, secretKey)
			if err != nil {
				t.Fatalf("Eval(%d) failed: %v", lambda, err)
			}

			// Verify the VRF output.
			valid, err := vrfInstance.Verify(input, phi, pi, verificationKey)
			if err != nil {
				t.Fatalf("Verify(%d) failed: %v", lambda, err)
			}
			if !valid {
				t.Errorf("Verify(%d) returned false, but it should be true", lambda)
			}

			// Test with invalid proof (pi).
			invalidPi := []byte("invalid-proof")
			valid, err = vrfInstance.Verify(input, phi, invalidPi, verificationKey)
			if err == nil || valid {
				t.Errorf("Verify(%d) with invalid proof should have failed", lambda)
			}
		})
	}
}

// Helper function to get the curve name based on lambda.
func lambdaToCurveName(lambda int) string {
	switch lambda {
	case 128:
		return "P256"
	case 192:
		return "P384"
	case 256:
		return "P521"
	default:
		return "Unknown"
	}
}

func TestSelectCurve(t *testing.T) {
	tests := []struct {
		lambda     int
		expected   elliptic.Curve
		shouldFail bool
	}{
		{128, elliptic.P224(), false},
		{192, elliptic.P384(), false},
		{256, elliptic.P256(), false},
		{512, nil, true}, // Invalid lambda.
	}

	for _, test := range tests {
		curve, _, err := selectCurveAndHash(test.lambda)
		if test.shouldFail {
			if err == nil {
				t.Errorf("selectCurveAndHash(%d) should have failed", test.lambda)
			}
		} else {
			if err != nil {
				t.Fatalf("selectCurveAndHash(%d) failed: %v", test.lambda, err)
			}
			if curve != test.expected {
				t.Errorf("selectCurveAndHash(%d) returned wrong curve", test.lambda)
			}
		}
	}
}
