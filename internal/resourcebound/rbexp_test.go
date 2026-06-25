package resourcebound

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"

	"github.com/mamorski/committee-sampling/internal/common"
	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

// Mocks

type MockResourceProof struct{ mock.Mock }

func (m *MockResourceProof) Setup(vk []byte) ([]byte, error) {
	args := m.Called(vk)
	return args.Get(0).([]byte), args.Error(1)
}
func (m *MockResourceProof) Prove(vk []byte, weight float64, challenge []byte, aux []byte) ([]byte, error) {
	args := m.Called(vk, weight, challenge, aux)
	return args.Get(0).([]byte), args.Error(1)
}
func (m *MockResourceProof) Ver(vk []byte, weight float64, challenge []byte, rpProof []byte) bool {
	args := m.Called(vk, weight, challenge, rpProof)
	return args.Bool(0)
}

type MockExPost struct{ mock.Mock }

func (m *MockExPost) Generate(session string, vk []byte) ([][][]byte, []byte, error) {
	args := m.Called(session, vk)
	return args.Get(0).([][][]byte), args.Get(1).([]byte), args.Error(2)
}
func (m *MockExPost) Verify(session string, vk []byte, fSigmaExp *common.FSigmaExp, auxTag *common.AuxTag, auxLocal float64,
	_ common.FilterTagF) (*common.Committee, error) {

	args := m.Called(session, vk, fSigmaExp, auxTag, auxLocal, mock.Anything)
	return args.Get(0).(*common.Committee), args.Error(1)
}

type MockExAnte struct{ mock.Mock }

func (m *MockExAnte) Generate(session string, vk []byte, challenge []byte, rpProof []byte) ([][][]byte, error) {
	args := m.Called(session, vk, challenge, rpProof)
	return args.Get(0).([][][]byte), args.Error(1)
}
func (m *MockExAnte) Verify(session string, vk []byte, sigma [][][]byte, auxTag *common.AuxTag, auxLocal float64,
	_ common.FilterTagF) (*common.Committee, error) {

	args := m.Called(session, vk, sigma, auxTag, auxLocal, mock.Anything)
	return args.Get(0).(*common.Committee), args.Error(1)
}

type RbExpSuite struct {
	suite.Suite
	rp     *MockResourceProof
	exp    *MockExPost
	exa    *MockExAnte
	weight float64
	rbexp  *RbExp
}

func (s *RbExpSuite) SetupTest() {
	s.rp = new(MockResourceProof)
	s.exp = new(MockExPost)
	s.exa = new(MockExAnte)
	s.weight = 1.0
	s.rbexp = New(s.rp, s.exp, s.exa, func(string, string, []byte, []byte, *pb.AuxKeyMessage) bool { return true }, s.weight, false, zap.NewNop())
}

func TestRbExpSuite(t *testing.T) {
	suite.Run(t, new(RbExpSuite))
}

func (s *RbExpSuite) TestGen_Success() {
	sid := "session1"
	vk := []byte("vk")
	auxRP := []byte("auxRP")
	challenge := []byte("challenge")
	piRP := []byte("piRP")
	sigmaExp := [][][]byte{{[]byte("sigmaExp")}}
	sigmaExa := [][][]byte{{[]byte("sigmaExa")}}

	s.rp.On("Setup", vk).Return(auxRP, nil).Once()
	s.exp.On("Generate", sid, vk).Return(sigmaExp, challenge, nil).Once()
	s.rp.On("Prove", vk, s.weight, challenge, auxRP).Return(piRP, nil).Once()
	s.exa.On("Generate", sid, vk, challenge, piRP).Return(sigmaExa, nil).Once()

	ch, proof, err := s.rbexp.Generate(sid, vk)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), challenge, ch)
	assert.Equal(s.T(), piRP, proof.PiRP)
	assert.Equal(s.T(), sigmaExp, proof.SigmaExp)
	assert.Equal(s.T(), sigmaExa, proof.SigmaExa)

	s.rp.AssertExpectations(s.T())
	s.exp.AssertExpectations(s.T())
	s.exa.AssertExpectations(s.T())
}

func (s *RbExpSuite) TestGen_ErrorSetup() {
	sid := "session1"
	vk := []byte("fail_setup")
	s.rp.On("Setup", vk).Return([]byte{}, assert.AnError).Once()

	_, _, err := s.rbexp.Generate(sid, vk)
	require.Error(s.T(), err)
	s.rp.AssertExpectations(s.T())
	s.exp.AssertExpectations(s.T())
	s.exa.AssertExpectations(s.T())
}

func (s *RbExpSuite) TestGen_ErrorExpGenerate() {
	sid := "fail_exp_generate"
	vk := []byte("vk")
	auxRP := []byte("auxRP")

	s.rp.On("Setup", vk).Return(auxRP, nil).Once()
	s.exp.On("Generate", sid, vk).Return([][][]byte{}, []byte{}, assert.AnError).Once()

	_, _, err := s.rbexp.Generate(sid, vk)
	require.Error(s.T(), err)
	s.rp.AssertExpectations(s.T())
	s.exp.AssertExpectations(s.T())
	s.exa.AssertExpectations(s.T())
}

func (s *RbExpSuite) TestGen_ErrorRPProve() {
	sid := "session1"
	vk := []byte("fail_rp_prove")
	auxRP := []byte("auxRP")
	challenge := []byte("challenge")
	sigmaExp := [][][]byte{{[]byte("sigmaExp")}}

	s.rp.On("Setup", vk).Return(auxRP, nil).Once()
	s.exp.On("Generate", sid, vk).Return(sigmaExp, challenge, nil).Once()
	s.rp.On("Prove", vk, s.weight, challenge, auxRP).Return([]byte{}, assert.AnError).Once()

	_, _, err := s.rbexp.Generate(sid, vk)
	require.Error(s.T(), err)
	s.rp.AssertExpectations(s.T())
	s.exp.AssertExpectations(s.T())
	s.exa.AssertExpectations(s.T())
}

func (s *RbExpSuite) TestGen_ErrorExaGenerate() {
	sid := "fail_exa_generate"
	vk := []byte("vk")
	auxRP := []byte("auxRP")
	challenge := []byte("challenge")
	piRP := []byte("piRP")
	sigmaExp := [][][]byte{{[]byte("sigmaExp")}}

	s.rp.On("Setup", vk).Return(auxRP, nil).Once()
	s.exp.On("Generate", sid, vk).Return(sigmaExp, challenge, nil).Once()
	s.rp.On("Prove", vk, s.weight, challenge, auxRP).Return(piRP, nil).Once()
	s.exa.On("Generate", sid, vk, challenge, piRP).Return([][][]byte{}, assert.AnError).Once()

	_, _, err := s.rbexp.Generate(sid, vk)
	require.Error(s.T(), err)
	s.rp.AssertExpectations(s.T())
	s.exp.AssertExpectations(s.T())
	s.exa.AssertExpectations(s.T())
}

func (s *RbExpSuite) TestVer_Success() {
	sid := "session1"
	vk := []byte("vk")
	challenge := []byte("challenge")
	piRP := []byte("piRP")
	sigmaExp := [][][]byte{{[]byte("sigmaExp")}}
	sigmaExa := [][][]byte{{[]byte("sigmaExa")}}
	proof := &common.RBExpProof{
		PiRP:     piRP,
		SigmaExp: sigmaExp,
		SigmaExa: sigmaExa,
	}
	auxKey := &common.AuxKey{
		PhiVRF: []byte("phi"),
		PiVRF:  []byte("pivr"),
		PhiVDF: []byte("phivdf"),
		PiVDF:  []byte("pivdf"),
	}
	auxTag := &common.AuxTag{PiRP: piRP, AuxKey: auxKey}
	fSigmaExp := &common.FSigmaExp{Challenge: challenge, Sigma: sigmaExp}
	outputP := &common.Committee{}
	outputP.Add(vk, "id1", 8)
	outputA := &common.Committee{}
	outputA.Add(vk, "id1", 5)

	s.exp.On("Verify", sid, vk, fSigmaExp, auxTag, 0.0, mock.Anything).Return(outputP, nil).Once()
	s.exa.On("Verify", sid, vk, sigmaExa, auxTag, 0.0, mock.Anything).Return(outputA, nil).Once()

	outputs, err := s.rbexp.Verify(sid, vk, challenge, proof, auxKey, 0)
	require.NoError(s.T(), err)
	require.Len(s.T(), outputs, 1)
	out := outputs[0]
	decoded, _ := base64.StdEncoding.DecodeString(out.VK)
	require.Equal(s.T(), vk, decoded)
	require.Equal(s.T(), 5, out.Grade)
	require.Equal(s.T(), "id1", out.ID)

	s.rp.AssertExpectations(s.T())
	s.exp.AssertExpectations(s.T())
	s.exa.AssertExpectations(s.T())
}

func (s *RbExpSuite) TestVer_NoMatchingOutput() {
	sid := "nomatch"
	vk := []byte("vk")
	challenge := []byte("challenge")
	piRP := []byte("piRP")
	sigmaExp := [][][]byte{{[]byte("sigmaExp")}}
	sigmaExa := [][][]byte{{[]byte("sigmaExa")}}
	proof := &common.RBExpProof{
		PiRP:     piRP,
		SigmaExp: sigmaExp,
		SigmaExa: sigmaExa,
	}
	auxKey := &common.AuxKey{}
	auxTag := &common.AuxTag{PiRP: piRP, AuxKey: auxKey}
	fSigmaExp := &common.FSigmaExp{Challenge: challenge, Sigma: sigmaExp}

	s.exp.On("Verify", sid, vk, fSigmaExp, auxTag, 0.0, mock.Anything).Return(&common.Committee{}, nil).Once()
	s.exa.On("Verify", sid, vk, sigmaExa, auxTag, 0.0, mock.Anything).Return(&common.Committee{}, nil).Once()

	_, err := s.rbexp.Verify(sid, vk, challenge, proof, auxKey, 0)
	require.Error(s.T(), err)
	s.rp.AssertExpectations(s.T())
	s.exp.AssertExpectations(s.T())
	s.exa.AssertExpectations(s.T())
}

func (s *RbExpSuite) TestVer_ErrorExPost() {
	sid := "fail_exp_verify"
	vk := []byte("vk")
	challenge := []byte("challenge")
	piRP := []byte("piRP")
	sigmaExp := [][][]byte{{[]byte("sigmaExp")}}
	sigmaExa := [][][]byte{{[]byte("sigmaExa")}}
	proof := &common.RBExpProof{
		PiRP:     piRP,
		SigmaExp: sigmaExp,
		SigmaExa: sigmaExa,
	}
	auxKey := &common.AuxKey{}
	auxTag := &common.AuxTag{PiRP: piRP, AuxKey: auxKey}
	fSigmaExp := &common.FSigmaExp{Challenge: challenge, Sigma: sigmaExp}

	s.exp.On("Verify", sid, vk, fSigmaExp, auxTag, 0.0, mock.Anything).Return(&common.Committee{}, assert.AnError).Once()
	s.exa.On("Verify", sid, vk, sigmaExa, auxTag, 0.0, mock.Anything).Return(&common.Committee{}, nil).Once()

	_, err := s.rbexp.Verify(sid, vk, challenge, proof, auxKey, 0)
	require.Error(s.T(), err)
	s.rp.AssertExpectations(s.T())
	s.exp.AssertExpectations(s.T())
	s.exa.AssertExpectations(s.T())
}

func (s *RbExpSuite) TestVer_ErrorExAnte() {
	sid := "fail_exa_verify"
	vk := []byte("vk")
	challenge := []byte("challenge")
	piRP := []byte("piRP")
	sigmaExp := [][][]byte{{[]byte("sigmaExp")}}
	sigmaExa := [][][]byte{{[]byte("sigmaExa")}}
	proof := &common.RBExpProof{
		PiRP:     piRP,
		SigmaExp: sigmaExp,
		SigmaExa: sigmaExa,
	}
	auxKey := &common.AuxKey{}
	auxTag := &common.AuxTag{PiRP: piRP, AuxKey: auxKey}
	fSigmaExp := &common.FSigmaExp{Challenge: challenge, Sigma: sigmaExp}

	s.exp.On("Verify", sid, vk, fSigmaExp, auxTag, 0.0, mock.Anything).Return(&common.Committee{}, nil).Once()
	s.exa.On("Verify", sid, vk, sigmaExa, auxTag, 0.0, mock.Anything).Return(&common.Committee{}, assert.AnError).Once()

	_, err := s.rbexp.Verify(sid, vk, challenge, proof, auxKey, 0)
	require.Error(s.T(), err)
	s.rp.AssertExpectations(s.T())
	s.exp.AssertExpectations(s.T())
	s.exa.AssertExpectations(s.T())
}

func (s *RbExpSuite) TestVer_FilterFalse() {
	sid := "session1"
	vk := []byte("vk")
	challenge := []byte("challenge")
	piRP := []byte("piRP")
	sigmaExp := [][][]byte{{[]byte("sigmaExp")}}
	sigmaExa := [][][]byte{{[]byte("sigmaExa")}}
	proof := &common.RBExpProof{
		PiRP:     piRP,
		SigmaExp: sigmaExp,
		SigmaExa: sigmaExa,
	}
	auxKey := &common.AuxKey{}
	auxTag := &common.AuxTag{PiRP: piRP, AuxKey: auxKey}
	fSigmaExp := &common.FSigmaExp{Challenge: challenge, Sigma: sigmaExp}

	// Use a filter that always returns false by changing the rbexp instance
	s.rbexp = New(s.rp, s.exp, s.exa, func(string, string, []byte, []byte, *pb.AuxKeyMessage) bool { return false }, s.weight, false, zap.NewNop())

	s.exp.On("Verify", sid, vk, fSigmaExp, auxTag, 0.0, mock.Anything).Return(&common.Committee{}, nil).Once()
	s.exa.On("Verify", sid, vk, sigmaExa, auxTag, 0.0, mock.Anything).Return(&common.Committee{}, nil).Once()

	_, err := s.rbexp.Verify(sid, vk, challenge, proof, auxKey, 0)
	require.Error(s.T(), err)
	s.rp.AssertExpectations(s.T())
	s.exp.AssertExpectations(s.T())
	s.exa.AssertExpectations(s.T())
}
