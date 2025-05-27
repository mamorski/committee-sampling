package vdf

import (
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type VDFSuite struct {
	suite.Suite
	v VDF
}

func (s *VDFSuite) SetupTest() {
	s.v = New()
}

func (s *VDFSuite) TestSetup() {
	vk, err := s.v.Setup(256, 1)
	s.NoError(err)
	s.Equal(32, len(vk))
}

func (s *VDFSuite) TestEval() {
	vk, err := s.v.Setup(256, 1)
	s.Require().NoError(err)

	x := []byte("test input")
	start := time.Now()
	phi, pi, err := s.v.Eval(x, vk, 1)
	s.NoError(err)

	elapsed := time.Since(start)
	s.GreaterOrEqual(elapsed, time.Second)

	s.NotEmpty(phi)
	s.NotEmpty(pi)
}

func (s *VDFSuite) TestVerify_Valid() {
	vk, err := s.v.Setup(256, 1)
	s.Require().NoError(err)

	x := []byte("test input")
	phi, pi, err := s.v.Eval(x, vk, 1)
	s.Require().NoError(err)

	valid, err := s.v.Verify(x, phi, pi, vk)
	s.NoError(err)
	s.True(valid)
}

func (s *VDFSuite) TestVerify_InvalidPhi() {
	vk, err := s.v.Setup(256, 1)
	s.Require().NoError(err)

	x := []byte("test input")
	phi, pi, err := s.v.Eval(x, vk, 1)
	s.Require().NoError(err)

	phi[0] ^= 0xFF
	valid, err := s.v.Verify(x, phi, pi, vk)
	s.NoError(err)
	s.False(valid)
}

func (s *VDFSuite) TestVerify_InvalidPi() {
	vk, err := s.v.Setup(256, 1)
	s.Require().NoError(err)

	x := []byte("test input")
	phi, pi, err := s.v.Eval(x, vk, 1)
	s.Require().NoError(err)

	pi[0] ^= 0xFF
	valid, err := s.v.Verify(x, phi, pi, vk)
	s.NoError(err)
	s.False(valid)
}

func TestVDFSuite(t *testing.T) {
	suite.Run(t, new(VDFSuite))
}
