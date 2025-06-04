package vdf

import (
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

type VDFSuite struct {
	suite.Suite
	v *Vdf
}

func Setup(lambda int) ([]byte, error) {

	vk := make([]byte, lambda/8)
	_, err := rand.Read(vk)
	if err != nil {
		return nil, fmt.Errorf("failed to generate verification key: %w", err)
	}

	return vk, nil
}

func (s *VDFSuite) SetupTest() {
	s.v = New(zap.NewNop())
}

func (s *VDFSuite) TestEval() {
	vk, err := Setup(256)
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
	vk, err := Setup(256)
	s.Require().NoError(err)

	x := []byte("test input")
	phi, pi, err := s.v.Eval(x, vk, 1)
	s.Require().NoError(err)

	valid, err := s.v.Verify(x, phi, pi, vk)
	s.NoError(err)
	s.True(valid)
}

func (s *VDFSuite) TestVerify_InvalidPhi() {
	vk, err := Setup(256)
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
	vk, err := Setup(256)
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
