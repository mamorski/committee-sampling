package vrf

import (
	"crypto/elliptic"
	"testing"

	"github.com/stretchr/testify/suite"
)

type VRFSuite struct {
	suite.Suite
	vrf *Vrf
}

func (s *VRFSuite) SetupTest() {
	s.vrf = New()
}

func (s *VRFSuite) TestGen() {
	for _, lambda := range []int{28, 32, 48} {
		secretKey, verificationKey, err := s.vrf.Generate(lambda)
		s.NoError(err)
		s.NotEmpty(secretKey)
		s.NotEmpty(verificationKey)
	}
	_, _, err := s.vrf.Generate(512)
	s.Error(err)
}

func (s *VRFSuite) TestEval() {
	for _, lambda := range []int{28, 32, 48} {
		secretKey, _, err := s.vrf.Generate(lambda)
		s.Require().NoError(err)
		phi, pi, err := s.vrf.Eval([]byte("test-input"), secretKey)
		s.NoError(err)
		s.NotEmpty(phi)
		s.NotEmpty(pi)
	}
}

func (s *VRFSuite) TestVerify() {
	for _, lambda := range []int{28, 48, 32} {
		secretKey, verificationKey, err := s.vrf.Generate(lambda)
		s.Require().NoError(err)
		phi, pi, err := s.vrf.Eval([]byte("test-input"), secretKey)
		s.Require().NoError(err)
		valid, err := s.vrf.Verify([]byte("test-input"), phi, pi, verificationKey)
		s.NoError(err)
		s.True(valid)
		invalidPi := []byte("invalid-proof")
		valid, err = s.vrf.Verify([]byte("test-input"), phi, invalidPi, verificationKey)
		s.Error(err)
		s.False(valid)
	}
}

func (s *VRFSuite) TestSelectCurve() {
	tests := []struct {
		lambda     int
		expected   elliptic.Curve
		shouldFail bool
	}{
		{28, elliptic.P224(), false},
		{48, elliptic.P384(), false},
		{32, elliptic.P256(), false},
		{512, nil, true},
	}
	for _, test := range tests {
		curve, _, err := selectCurveAndHash(test.lambda)
		if test.shouldFail {
			s.Error(err)
		} else {
			s.NoError(err)
			s.Equal(test.expected, curve)
		}
	}
}

func TestVRFSuite(t *testing.T) {
	suite.Run(t, new(VRFSuite))
}
