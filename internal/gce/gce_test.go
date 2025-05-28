package gce

import (
	"testing"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

// VRFMock mocks the VRF interface
type VRFMock struct {
	mock.Mock
}

func (m *VRFMock) Gen(lambda int) ([]byte, []byte, error) {
	args := m.Called(lambda)
	return args.Get(0).([]byte), args.Get(1).([]byte), args.Error(2)
}

func (m *VRFMock) Eval(message, sk []byte) ([]byte, []byte, error) {
	args := m.Called(message, sk)
	return args.Get(0).([]byte), args.Get(1).([]byte), args.Error(2)
}

// VDFMock mocks the VDF interface
type VDFMock struct {
	mock.Mock
}

func (m *VDFMock) Setup(lambda, delta int) ([]byte, error) {
	args := m.Called(lambda, delta)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *VDFMock) Eval(message, vk []byte, delay int) ([]byte, []byte, error) {
	args := m.Called(message, vk, delay)
	return args.Get(0).([]byte), args.Get(1).([]byte), args.Error(2)
}

// RBExpMock mocks the RBExp interface
type RBExpMock struct {
	mock.Mock
}

func (m *RBExpMock) Generate(sid string, vk []byte) ([]byte, *common.RBExpProof, error) {
	args := m.Called(sid, vk)
	return args.Get(0).([]byte), args.Get(1).(*common.RBExpProof), args.Error(2)
}

func (m *RBExpMock) Verify(sid string, vk, ch []byte, proof *common.RBExpProof, auxKey *common.AuxKey,
	auxLocal float64) ([]*common.RBExpOutput, error) {

	args := m.Called(sid, vk, ch, proof, auxKey, auxLocal)
	return args.Get(0).([]*common.RBExpOutput), args.Error(1)
}

// GCETestSuite is the testify suite for GCE
type GCETestSuite struct {
	suite.Suite
	vrf   *VRFMock
	vdf   *VDFMock
	rbexp *RBExpMock
}

func (s *GCETestSuite) SetupTest() {
	s.vrf = new(VRFMock)
	s.vdf = new(VDFMock)
	s.rbexp = new(RBExpMock)
}

func (s *GCETestSuite) TestInitialize_Success() {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := 10

	sk := []byte("sk_128")
	vk := []byte("vk_128")
	challenge := []byte("challenge_test_session_vk_128")
	proof := &common.RBExpProof{
		PiRP:     []byte("piRP_vk_128"),
		SigmaExp: [][][]byte{{[]byte("sigmaExp_test_session")}},
		SigmaExa: [][][]byte{{[]byte("sigmaExa_test_session")}},
	}
	vdfVk := []byte("vdfVk_10")
	phiVDF := []byte("phiVDF_test_idvk_128challenge_test_session_vk_128")
	piVDF := []byte("piVDF_test_idvk_128challenge_test_session_vk_128")

	s.vrf.On("Generate", lambda).Return(sk, vk, nil).Once()
	identityData := append([]byte(id), vk...)
	s.rbexp.On("Generate", sid, identityData).Return(challenge, proof, nil).Once()
	s.vdf.On("Setup", lambda, delay).Return(vdfVk, nil).Once()
	vdfInput := HashData([]byte(id), vk, challenge)
	s.vdf.On("Eval", vdfInput, vdfVk, delay).Return(phiVDF, piVDF, nil).Once()

	state, err := Initialize(id, sid, s.vrf, s.rbexp, s.vdf, delay, lambda)
	s.Require().NoError(err)
	s.NotNil(state)
	s.Equal(sk, state.VRFSecret)
	s.Equal(vk, state.VRFPublic)
	s.Equal(challenge, state.Challenge)
	s.Equal(proof, state.RBExpProof)
	s.Equal(phiVDF, state.PhiVDF)
	s.Equal(piVDF, state.PiVDF)

	s.vrf.AssertExpectations(s.T())
	s.rbexp.AssertExpectations(s.T())
	s.vdf.AssertExpectations(s.T())
}

func (s *GCETestSuite) TestInitialize_ErrorVRFGen() {
	id := "test_id"
	sid := "test_session"
	lambda := 0
	delay := 10
	s.vrf.On("Generate", lambda).Return([]byte(nil), []byte(nil), assert.AnError).Once()
	state, err := Initialize(id, sid, s.vrf, s.rbexp, s.vdf, delay, lambda)
	s.Error(err)
	s.Nil(state)
	s.vrf.AssertExpectations(s.T())
}

func (s *GCETestSuite) TestInitialize_ErrorRBExpGen() {
	id := "test_id"
	sid := "fail_rbexp_gen"
	lambda := 128
	delay := 10
	sk := []byte("sk_128")
	vk := []byte("vk_128")
	s.vrf.On("Generate", lambda).Return(sk, vk, nil).Once()
	identityData := append([]byte(id), vk...)
	s.rbexp.On("Generate", sid, identityData).Return([]byte(nil), (*common.RBExpProof)(nil), assert.AnError).Once()
	state, err := Initialize(id, sid, s.vrf, s.rbexp, s.vdf, delay, lambda)
	s.Error(err)
	s.Nil(state)
	s.vrf.AssertExpectations(s.T())
	s.rbexp.AssertExpectations(s.T())
}

func (s *GCETestSuite) TestInitialize_ErrorVDFSetup() {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := 0
	sk := []byte("sk_128")
	vk := []byte("vk_128")
	s.vrf.On("Generate", lambda).Return(sk, vk, nil).Once()
	identityData := append([]byte(id), vk...)
	s.rbexp.On("Generate", sid, identityData).Return([]byte("challenge"), &common.RBExpProof{}, nil).Once()
	s.vdf.On("Setup", lambda, delay).Return([]byte(nil), assert.AnError).Once()
	state, err := Initialize(id, sid, s.vrf, s.rbexp, s.vdf, delay, lambda)
	s.Error(err)
	s.Nil(state)
	s.vrf.AssertExpectations(s.T())
	s.rbexp.AssertExpectations(s.T())
	s.vdf.AssertExpectations(s.T())
}

func (s *GCETestSuite) TestInitialize_ErrorVDFEval() {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := -1
	sk := []byte("sk_128")
	vk := []byte("vk_128")
	challenge := []byte("challenge_test_session_vk_128")
	proof := &common.RBExpProof{}
	vdfVk := []byte("vdfVk_-1")
	s.vrf.On("Generate", lambda).Return(sk, vk, nil).Once()
	identityData := append([]byte(id), vk...)
	s.rbexp.On("Generate", sid, identityData).Return(challenge, proof, nil).Once()
	s.vdf.On("Setup", lambda, delay).Return(vdfVk, nil).Once()
	vdfInput := HashData([]byte(id), vk, challenge)
	s.vdf.On("Eval", vdfInput, vdfVk, delay).Return([]byte(nil), []byte(nil), assert.AnError).Once()
	state, err := Initialize(id, sid, s.vrf, s.rbexp, s.vdf, delay, lambda)
	s.Error(err)
	s.Nil(state)
	s.vrf.AssertExpectations(s.T())
	s.rbexp.AssertExpectations(s.T())
	s.vdf.AssertExpectations(s.T())
}

func (s *GCETestSuite) TestCommitteeElection_Success() {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := 10
	weight := 100.0
	sk := []byte("sk_128")
	vk := []byte("vk_128")
	challenge := []byte("challenge_test_session_vk_128")
	proof := &common.RBExpProof{}
	vdfVk := []byte("vdfVk_10")
	phiVDF := []byte("phiVDF_test_idvk_128challenge_test_session_vk_128")
	piVDF := []byte("piVDF_test_idvk_128challenge_test_session_vk_128")
	phiVrf := []byte("phiVRF_hash")
	piVrf := []byte("piVRF_hash")
	outputs := []*common.RBExpOutput{{VK: append([]byte(id), vk...), Grade: 1}}

	s.vrf.On("Generate", lambda).Return(sk, vk, nil).Once()
	identityData := append([]byte(id), vk...)
	s.rbexp.On("Generate", sid, identityData).Return(challenge, proof, nil).Once()
	s.vdf.On("Setup", lambda, delay).Return(vdfVk, nil).Once()
	vdfInput := HashData([]byte(id), vk, challenge)
	s.vdf.On("Eval", vdfInput, vdfVk, delay).Return(phiVDF, piVDF, nil).Once()
	state, _ := Initialize(id, sid, s.vrf, s.rbexp, s.vdf, delay, lambda)

	hashInput := HashData(phiVDF, []byte(sid))
	s.vrf.On("Eval", hashInput, sk).Return(phiVrf, piVrf, nil).Once()
	auxKey := &common.AuxKey{PhiVRF: phiVrf, PiVRF: piVrf, PhiVDF: phiVDF, PiVDF: piVDF}
	s.rbexp.On("Verify", sid, identityData, challenge, proof, auxKey, weight).Return(outputs, nil).Once()

	committee, err := CommitteeElection(id, sid, state, weight, s.vrf, s.rbexp)
	s.NoError(err)
	s.Len(committee, 1)
	s.Equal(outputs[0].VK, committee[0].IdentityData)
	s.Equal(outputs[0].Grade, committee[0].Grade)

	s.vrf.AssertExpectations(s.T())
	s.rbexp.AssertExpectations(s.T())
}

func (s *GCETestSuite) TestCommitteeElection_NilState() {
	_, err := CommitteeElection("test_id", "test_session", nil, 100.0, s.vrf, s.rbexp)
	s.Error(err)
}

func (s *GCETestSuite) TestCommitteeElection_ErrorVRFEval() {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := 10
	weight := 100.0
	sk := []byte("fail_secret")
	vk := []byte("vk_128")
	challenge := []byte("challenge_test_session_vk_128")
	proof := &common.RBExpProof{}
	vdfVk := []byte("vdfVk_10")
	phiVDF := []byte("phiVDF_test_idvk_128challenge_test_session_vk_128")
	piVDF := []byte("piVDF_test_idvk_128challenge_test_session_vk_128")

	s.vrf.On("Generate", lambda).Return(sk, vk, nil).Once()
	identityData := append([]byte(id), vk...)
	s.rbexp.On("Generate", sid, identityData).Return(challenge, proof, nil).Once()
	s.vdf.On("Setup", lambda, delay).Return(vdfVk, nil).Once()
	vdfInput := HashData([]byte(id), vk, challenge)
	s.vdf.On("Eval", vdfInput, vdfVk, delay).Return(phiVDF, piVDF, nil).Once()
	state, _ := Initialize(id, sid, s.vrf, s.rbexp, s.vdf, delay, lambda)

	hashInput := HashData(phiVDF, []byte(sid))
	s.vrf.On("Eval", hashInput, sk).Return([]byte(nil), []byte(nil), assert.AnError).Once()

	_, err := CommitteeElection(id, sid, state, weight, s.vrf, s.rbexp)
	s.Error(err)
	s.vrf.AssertExpectations(s.T())
}

func (s *GCETestSuite) TestCommitteeElection_ErrorRBExpVer() {
	id := "test_id"
	sid := "fail_ver"
	lambda := 128
	delay := 10
	weight := 100.0
	sk := []byte("sk_128")
	vk := []byte("vk_128")
	challenge := []byte("challenge_test_session_vk_128")
	proof := &common.RBExpProof{}
	vdfVk := []byte("vdfVk_10")
	phiVDF := []byte("phiVDF_test_idvk_128challenge_test_session_vk_128")
	piVDF := []byte("piVDF_test_idvk_128challenge_test_session_vk_128")

	s.vrf.On("Generate", lambda).Return(sk, vk, nil).Once()
	identityData := append([]byte(id), vk...)
	s.rbexp.On("Generate", "test_session", identityData).Return(challenge, proof, nil).Once()
	s.vdf.On("Setup", lambda, delay).Return(vdfVk, nil).Once()
	vdfInput := HashData([]byte(id), vk, challenge)
	s.vdf.On("Eval", vdfInput, vdfVk, delay).Return(phiVDF, piVDF, nil).Once()
	state, _ := Initialize(id, "test_session", s.vrf, s.rbexp, s.vdf, delay, lambda)

	hashInput := HashData(phiVDF, []byte(sid))
	phiVrf := []byte("phiVRF_hash")
	piVrf := []byte("piVRF_hash")
	s.vrf.On("Eval", hashInput, sk).Return(phiVrf, piVrf, nil).Once()
	auxKey := &common.AuxKey{PhiVRF: phiVrf, PiVRF: piVrf, PhiVDF: phiVDF, PiVDF: piVDF}
	s.rbexp.On("Verify", sid, identityData, challenge, proof, auxKey, weight).Return([]*common.RBExpOutput(nil), assert.AnError).Once()

	_, err := CommitteeElection(id, sid, state, weight, s.vrf, s.rbexp)
	s.Error(err)
	s.vrf.AssertExpectations(s.T())
	s.rbexp.AssertExpectations(s.T())
}

func TestGCESuite(t *testing.T) {
	suite.Run(t, new(GCETestSuite))
}
