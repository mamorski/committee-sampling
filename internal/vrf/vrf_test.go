package vrf

import (
	"crypto/elliptic"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

type VRFSuite struct {
	suite.Suite
	vrf *Vrf
}

func (s *VRFSuite) SetupTest() {
	s.vrf = New(zap.NewNop())
}

func (s *VRFSuite) TestGen() {
	for _, lambda := range []int{224, 256, 384} {
		secretKey, verificationKey, err := s.vrf.Generate(lambda)
		s.NoError(err)
		s.NotEmpty(secretKey)
		s.NotEmpty(verificationKey)
	}
	_, _, err := s.vrf.Generate(512)
	s.Error(err)
}

func (s *VRFSuite) TestEval() {
	for _, lambda := range []int{224, 256, 384} {
		secretKey, _, err := s.vrf.Generate(lambda)
		s.Require().NoError(err)
		phi, pi, err := s.vrf.Eval([]byte("test-input"), secretKey)
		s.NoError(err)
		s.NotEmpty(phi)
		s.NotEmpty(pi)
	}
}

func (s *VRFSuite) TestVerify() {
	for _, lambda := range []int{224, 384, 256} {
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
		lambda        int
		expected      elliptic.Curve
		expectedSuite byte
		shouldFail    bool
	}{
		{224, elliptic.P224(), 0x02, false},
		{384, elliptic.P384(), 0x03, false},
		{256, elliptic.P256(), 0x01, false},
		{512, nil, 0x00, true},
	}
	for _, test := range tests {
		curve, _, suiteString, err := selectCurveAndHash(test.lambda)
		if test.shouldFail {
			s.Error(err)
		} else {
			s.NoError(err)
			s.Equal(test.expected, curve)
			s.Equal(test.expectedSuite, suiteString)
		}
	}
}

func TestVRFSuite(t *testing.T) {
	suite.Run(t, new(VRFSuite))
}
