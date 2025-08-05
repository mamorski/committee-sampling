package expost

import (
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/network"
	pb "github.com/mamorski/committee-sampling/pkg/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Helper function to create a valid peer.ID from string for testing
func createTestPeerID(id string) peer.ID {
	// For testing, we can create a valid peer ID from the string
	// by using a simple encoding that libp2p can handle
	testID := "12D3KooW" + id + "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890"
	if len(testID) > 52 {
		testID = testID[:52] // Truncate to valid length
	}
	peerID, err := peer.Decode(testID)
	if err != nil {
		// Fallback: create a simple peer ID for testing
		return peer.ID(id)
	}
	return peerID
}

type MockNetwork struct {
	mock.Mock
}

func (m *MockNetwork) RegisterHandler(protocolID string, handler network.MessageHandler) {
	m.Called(protocolID, handler)
}

func (m *MockNetwork) SendProtocolMessage(protocolID string, message []byte) {
	m.Called(protocolID, message)
}

func (m *MockNetwork) GetNodeID() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockNetwork) GetNeighbors() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockNetwork) IsNeighbor(peerID peer.ID) bool {
	args := m.Called(peerID)
	return args.Bool(0)
}

func (m *MockNetwork) Close() error {
	args := m.Called()
	return args.Error(0)
}

type MockMDAG struct {
	mock.Mock
}

func (m *MockMDAG) Generate(sid string, vk []byte, vi ...[]byte) ([][][]byte, error) {
	args := m.Called(sid, vk, vi)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([][][]byte), args.Error(1)
}

func (m *MockMDAG) GetComputedLabel(roundIndex int) []byte {
	args := m.Called(roundIndex)
	return args.Get(0).([]byte)
}

func (m *MockMDAG) Verify(merkleRoot []byte, path [][]byte) bool {
	args := m.Called(merkleRoot, path)
	return args.Bool(0)
}

func (m *MockMDAG) Oracle(h ...[]byte) []byte {
	args := m.Called(h)
	return args.Get(0).([]byte)
}

type ExPostTestSuite struct {
	suite.Suite
	mockNetwork *MockNetwork
	mockMDAG    *MockMDAG
	logger      *zap.Logger
	expost      *ExPost
	sid         string
	vk          []byte
	startTime   time.Time
}

func (suite *ExPostTestSuite) SetupTest() {
	suite.mockNetwork = new(MockNetwork)
	suite.mockMDAG = new(MockMDAG)
	suite.logger = zap.NewNop()
	suite.sid = "test-session"
	suite.vk = []byte("test-vk")
	suite.startTime = time.Now()

	syncer := newDelayedSync(50 * time.Millisecond)

	suite.expost = &ExPost{
		network:      suite.mockNetwork,
		logger:       suite.logger.Named("expost"),
		mdag:         suite.mockMDAG,
		synchronizer: syncer,
		d:            3,
		D:            5,
		lambda:       32,
		gradeFunc:    mockGradeFunc,
		isRunning:    true,
		sid:          suite.sid,
		vk:           suite.vk,
		mu:           sync.Mutex{},
		messages:     make(map[int][]receivedMessage),
	}
}

func (suite *ExPostTestSuite) TestNew() {
	// Test parameters
	testSid := "test-new-session"
	testVk := []byte("test-new-vk")
	testDiameter := 4
	testD := 6
	testLambda := 64
	testGradeFunc := mockGradeFunc
	testLogger := zap.NewNop()

	// Setup mock expectations
	mockNet := new(MockNetwork)
	mockMDAG := new(MockMDAG)

	mockNet.On("RegisterHandler", "/expost/1.0.0/test-new-session", mock.AnythingOfType("network.MessageHandler")).Once()

	// Call the New function
	expost := New(mockNet, mockMDAG, testSid, testVk, newDelayedSync(50*time.Millisecond),
		testDiameter, testD, testLambda, testGradeFunc, testLogger)

	// Verify the instance is created correctly
	suite.NotNil(expost)

	// Verify all fields are set correctly
	suite.Equal(mockNet, expost.network)
	suite.Equal(mockMDAG, expost.mdag)
	suite.Equal(testSid, expost.sid)
	suite.Equal(testVk, expost.vk)
	suite.Equal(testDiameter, expost.d)
	suite.Equal(testD, expost.D)
	suite.Equal(testLambda, expost.lambda)
	suite.True(expost.isRunning)

	// Verify function pointers are set
	suite.NotNil(expost.gradeFunc)

	// Verify logger is named correctly
	suite.NotNil(expost.logger)

	// Verify maps are initialized
	suite.NotNil(expost.messages)
	suite.Len(expost.messages, 0) // Should be empty initially

	// Verify initial state
	suite.Nil(expost.state)

	// Verify mock expectations
	mockNet.AssertExpectations(suite.T())
	mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestNewProtocolIDGeneration() {
	testSid := "special/chars@session#123"
	testVk := []byte("test-vk")
	testLogger := zap.NewNop()

	mockNet := new(MockNetwork)
	mockMDAG := new(MockMDAG)

	// Verify the protocol ID is generated correctly with special characters
	expectedProtocolID := "/expost/1.0.0/special/chars@session#123"
	mockNet.On("RegisterHandler", expectedProtocolID, mock.AnythingOfType("network.MessageHandler")).Once()

	expost := New(mockNet, mockMDAG, testSid, testVk, newDelayedSync(10*time.Millisecond), 3, 5, 32, mockGradeFunc, testLogger)

	suite.NotNil(expost)
	suite.Equal(testSid, expost.sid)

	mockNet.AssertExpectations(suite.T())
	mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestGenerateHappyFlow() {
	expectedState := [][][]byte{
		{[]byte("state1")},
		{[]byte("state2")},
	}
	expectedLabel := []byte("label-R")

	suite.mockMDAG.On("Generate", suite.sid, suite.vk, mock.Anything).Return(expectedState, nil).Once()
	suite.mockMDAG.On("GetComputedLabel", 15).Return(expectedLabel).Once() // d*D = 3*5 = 15

	state, label, err := suite.expost.Generate(suite.sid, suite.vk)

	suite.NoError(err)
	suite.Equal(expectedState, state)
	suite.Equal(expectedLabel, label)
	suite.Equal(expectedState, suite.expost.state)

	suite.mockMDAG.AssertExpectations(suite.T())
	suite.mockNetwork.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestGenerateMDAGError() {
	suite.mockMDAG.On("Generate", suite.sid, suite.vk, mock.Anything).Return(nil, assert.AnError).Once()

	state, label, err := suite.expost.Generate(suite.sid, suite.vk)

	suite.Error(err)
	suite.Nil(state)
	suite.Nil(label)
	suite.Contains(err.Error(), "MDAG generation failed")

	suite.mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestVerifyHappyFlow() {
	sigma := createTestSigma(20) // More than d*D = 15
	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	suite.expost.state = sigma

	// Create FSigmaExp with proper sigma and challenge
	fSigmaExp := &common.FSigmaExp{
		Challenge: []byte("test-challenge"),
		Sigma:     sigma,
	}

	suite.mockNetwork.On("GetNodeID").Return("test-node").Times(15)

	results, err := suite.expost.Verify(suite.sid, suite.vk, fSigmaExp, auxTag, 0.5, mockFilterTagFunc)

	suite.NoError(err)
	suite.NotNil(results)

	suite.mockNetwork.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestVerifySessionMismatch() {
	auxTag := &common.AuxTag{AuxKey: &common.AuxKey{}}

	// Create FSigmaExp even though it won't be used due to session mismatch
	fSigmaExp := &common.FSigmaExp{
		Challenge: []byte("test-challenge"),
		Sigma:     createTestSigma(20),
	}

	results, err := suite.expost.Verify("wrong-session", suite.vk, fSigmaExp, auxTag, 0.5, mockFilterTagFunc)

	suite.Error(err)
	suite.Nil(results)
	suite.Contains(err.Error(), "session ID mismatch")
}

func (suite *ExPostTestSuite) TestVerifyInsufficientSigmaLength() {
	auxTag := &common.AuxTag{AuxKey: &common.AuxKey{}}

	// Create FSigmaExp with insufficient sigma length
	fSigmaExp := &common.FSigmaExp{
		Challenge: []byte("test-challenge"),
		Sigma:     createTestSigma(10), // Less than d*D = 15
	}

	results, err := suite.expost.Verify(suite.sid, suite.vk, fSigmaExp, auxTag, 0.5, mockFilterTagFunc)

	suite.Error(err)
	suite.Nil(results)
	suite.Contains(err.Error(), "sigma length is less than required rounds")
	suite.False(suite.expost.isRunning)
}

func (suite *ExPostTestSuite) TestHandleMessageHappyFlow() {
	suite.expost.isRunning = true

	msg := &pb.TimestampMessage{
		SessionId:       suite.sid,
		VerificationKey: []byte("test-vk"),
		Value:           []byte("test-value"),
		Aux: &pb.Aux{
			PiRP: []byte("pi-rp"),
			AuxKey: &pb.AuxKeyMessage{
				PhiVrf: []byte("phi-vrf"),
				PiVrf:  []byte("pi-vrf"),
				PhiVdf: []byte("phi-vdf"),
				PiVdf:  []byte("pi-vdf"),
			},
		},
		MerklePath: []*pb.State{{Row: [][]byte{[]byte("path1")}}},
		Round:      1,
		Id:         "node1",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	// Set up mock expectations for IsNeighbor call
	suite.mockNetwork.On("IsNeighbor", createTestPeerID("node1")).Return(true).Once()

	// Set up mock expectations for GetNodeID call (needed for metrics)
	suite.mockNetwork.On("GetNodeID").Return("test-node").Once()

	err = suite.expost.handleMessage(createTestPeerID("node1"), msgBytes)
	suite.NoError(err)

	suite.expost.mu.Lock()
	messages := suite.expost.messages[1]
	suite.expost.mu.Unlock()

	suite.Require().Len(messages, 1)
	receivedMsg := messages[0]
	suite.Equal(suite.sid, receivedMsg.sid)
	suite.Equal([]byte("test-vk"), receivedMsg.vk)
	suite.Equal([]byte("test-value"), receivedMsg.v)

	// Verify mock expectations
	suite.mockNetwork.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestHandleMessageNotRunning() {
	suite.expost.isRunning = false

	err := suite.expost.handleMessage(createTestPeerID("node1"), []byte("test"))
	suite.Error(err)
	suite.Contains(err.Error(), "protocol not running")
}

func (suite *ExPostTestSuite) TestHandleMessageInvalidPayload() {
	suite.expost.isRunning = true

	err := suite.expost.handleMessage(createTestPeerID("node1"), []byte("invalid-proto"))
	suite.Error(err)
}

func (suite *ExPostTestSuite) TestHandleMessageSessionMismatch() {
	suite.expost.isRunning = true

	msg := &pb.TimestampMessage{
		SessionId: "wrong-session",
		Id:        "node1",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)
	suite.mockNetwork.On("GetNodeID").Return("test-node").Twice()

	err = suite.expost.handleMessage(createTestPeerID("node1"), msgBytes)
	suite.Error(err)
	suite.Contains(err.Error(), "session id mismatch")
}

func (suite *ExPostTestSuite) TestHandleMessageUnknownNeighbor() {
	suite.expost.isRunning = true

	msg := &pb.TimestampMessage{
		SessionId: suite.sid,
		Id:        "unknown-node",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	// Set up mock expectations for IsNeighbor call
	suite.mockNetwork.On("IsNeighbor", createTestPeerID("unknown-node")).Return(false).Once()
	suite.mockNetwork.On("GetNodeID").Return("test-node").Once()

	err = suite.expost.handleMessage(createTestPeerID("unknown-node"), msgBytes)
	suite.Error(err)
	suite.Contains(err.Error(), "sender not in neighbors list")

	// Verify mock expectations
	suite.mockNetwork.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestHandleMessageMultiple() {
	suite.expost.isRunning = true

	msg1 := &pb.TimestampMessage{
		SessionId:       suite.sid,
		VerificationKey: []byte("test-vk"),
		Value:           []byte("test-value-1"),
		Aux: &pb.Aux{
			AuxKey: &pb.AuxKeyMessage{},
		},
		MerklePath: []*pb.State{},
		Round:      1,
		Id:         "node1",
	}

	msg2 := &pb.TimestampMessage{
		SessionId:       suite.sid,
		VerificationKey: []byte("test-vk"),
		Value:           []byte("test-value-2"),
		Aux: &pb.Aux{
			AuxKey: &pb.AuxKeyMessage{},
		},
		MerklePath: []*pb.State{},
		Round:      1,
		Id:         "node1",
	}

	msgBytes1, err := proto.Marshal(msg1)
	suite.NoError(err)
	msgBytes2, err := proto.Marshal(msg2)
	suite.NoError(err)

	// Set up mock expectations for IsNeighbor calls
	suite.mockNetwork.On("IsNeighbor", createTestPeerID("node1")).Return(true).Twice()

	// Set up mock expectations for GetNodeID calls (needed for metrics)
	suite.mockNetwork.On("GetNodeID").Return("test-node").Twice()

	// First message should succeed
	err = suite.expost.handleMessage(createTestPeerID("node1"), msgBytes1)
	suite.NoError(err)

	// Second message from same sender should also be stored
	err = suite.expost.handleMessage(createTestPeerID("node1"), msgBytes2)
	suite.NoError(err)

	suite.expost.mu.Lock()
	messages := suite.expost.messages[1]
	suite.expost.mu.Unlock()

	suite.Len(messages, 2) // Both messages stored

	// Verify mock expectations
	suite.mockNetwork.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestValidateMerklePathHappyFlow() {
	merklePath := [][][]byte{
		{[]byte("path1")},
	}
	suite.expost.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("oracle-result")},
	}

	// Mock GetComputedLabel call
	suite.mockMDAG.On("GetComputedLabel", mock.Anything).Return([]byte("path1")).Once()

	isValid := suite.expost.validateMerklePath(merklePath, 1)
	suite.True(isValid)

	suite.mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestValidateMerklePathInvalidStateLength() {
	merklePath := [][][]byte{
		{[]byte("path1")},
		{[]byte("path2")},
	}
	suite.expost.state = [][][]byte{{[]byte("state0")}}

	// Mock GetComputedLabel call
	suite.mockMDAG.On("GetComputedLabel", mock.Anything).Return([]byte("path1")).Once()
	// Mock Oracle call for the validation loop (round=2, so i=1)
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result")).Once()

	isValid := suite.expost.validateMerklePath(merklePath, 2)
	suite.False(isValid)

	suite.mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestIsMessageValidHappyFlow() {
	msg := &receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),
		v:   []byte("test-value"),
		aux: &common.AuxTag{
			AuxKey: &common.AuxKey{},
		},
		merklePath: [][][]byte{
			{[]byte("test-value")},
		},
	}

	suite.expost.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("oracle-result")},
	}

	// Mock Oracle call for value check
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("test-value")).Once()
	// Mock GetComputedLabel call for validateMerklePath
	suite.mockMDAG.On("GetComputedLabel", mock.Anything).Return([]byte("test-value")).Once()

	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterTagFunc, 1)
	suite.True(isValid)

	suite.mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestIsMessageValidNilMessage() {
	isValid := suite.expost.isMessageValid(nil, 0.5, mockFilterTagFunc, 1)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestIsMessageValidGradeFunctionFails() {
	msg := &receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),
		v:   []byte("test-value"),
		aux: &common.AuxTag{AuxKey: &common.AuxKey{}},
	}

	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterTagFunc, 1)
	suite.False(isValid) // mockGradeFunc returns 0 for this case
}

func (suite *ExPostTestSuite) TestConvertTimestampToBytes() {
	msg := &pb.TimestampMessage{
		MerklePath: []*pb.State{
			{Row: [][]byte{[]byte("row1"), []byte("row2")}},
			{Row: [][]byte{[]byte("row3")}},
			nil,        // Test nil state
			{Row: nil}, // Test nil row
		},
	}

	result := convertTimestampToBytes(msg)

	suite.Len(result, 4)
	suite.Equal([][]byte{[]byte("row1"), []byte("row2")}, result[0])
	suite.Equal([][]byte{[]byte("row3")}, result[1])
	suite.Nil(result[2])
	suite.Equal([][]byte{}, result[3])
}

func (suite *ExPostTestSuite) TestSecureRandomBytes() {
	result, err := secureRandomBytes(10, "abc")
	suite.NoError(err)
	suite.Len(result, 10)

	// Check that all bytes are from the allowed charset
	for _, b := range result {
		suite.Contains("abc", string(b))
	}
}

func (suite *ExPostTestSuite) TestIsValueInState() {
	state := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
	}

	suite.True(isValueInState([]byte("value2"), state))
	suite.False(isValueInState([]byte("value4"), state))
}

func TestExPostTestSuite(t *testing.T) {
	suite.Run(t, new(ExPostTestSuite))
}

// Helper functions
func createTestSigma(length int) [][][]byte {
	sigma := make([][][]byte, length)
	for i := 0; i < length; i++ {
		sigma[i] = [][]byte{[]byte("state" + string(rune(i)))}
	}
	return sigma
}

func mockGradeFunc(_ string, vk []byte, v []byte, _ *common.AuxKey, _ float64) int {
	if string(vk) == "test-vk" && string(v) == "test-value" {
		return 5 // High grade for test values
	}
	return 0
}

func mockFilterTagFunc(_, _ string, _ []byte, _ []byte, _ *common.AuxTag) bool {
	return true
}

func mockFilterTagFuncFalse(_, _ string, _ []byte, _ []byte, _ *common.AuxTag) bool {
	return false
}

func (suite *ExPostTestSuite) TestGenerateWithEmptyCharset() {
	// This should panic due to empty charset
	suite.Panics(func() {
		_, _ = secureRandomBytes(10, "")
	})
}

func (suite *ExPostTestSuite) TestGenerateWithZeroLength() {
	result, err := secureRandomBytes(0, "abc")
	suite.NoError(err)
	suite.Len(result, 0)
}

func (suite *ExPostTestSuite) TestVerifyWithExactSigmaLength() {
	// Create a smaller ExPost instance for this test
	suite.expost.d = 2
	suite.expost.D = 3

	auxTag := &common.AuxTag{AuxKey: &common.AuxKey{}}
	suite.expost.state = createTestSigma(5)

	// Create FSigmaExp with insufficient sigma length
	fSigmaExp := &common.FSigmaExp{
		Challenge: []byte("test-challenge"),
		Sigma:     createTestSigma(5), // d*D = 2*3 = 6, so 5 < 6 should fail
	}

	// Mock GetNodeID for the verification phase
	suite.mockNetwork.On("GetNodeID").Return("test-node").Once()

	results, err := suite.expost.Verify(suite.sid, suite.vk, fSigmaExp, auxTag, 0.5, mockFilterTagFunc)

	suite.Error(err)
	suite.Nil(results)
	suite.Contains(err.Error(), "sigma length is less than required rounds")
}

func (suite *ExPostTestSuite) TestVerifyWithHighGradeProver() {
	sigma := createTestSigma(20)
	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}
	suite.expost.state = sigma
	suite.expost.gradeFunc = edgeCaseGradeFunc

	// Create FSigmaExp
	fSigmaExp := &common.FSigmaExp{
		Challenge: []byte("test-challenge"),
		Sigma:     sigma,
	}

	suite.mockNetwork.On("GetNodeID").Return("test-node").Times(32)
	suite.mockNetwork.On("SendProtocolMessage", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Once()

	results, err := suite.expost.Verify(suite.sid, []byte("high-grade-vk"), fSigmaExp, auxTag, 0.5, highGradeFilterTagFunc)

	suite.NoError(err)
	suite.NotNil(results)
}

func (suite *ExPostTestSuite) TestVerifyWithLowGradeProver() {
	sigma := createTestSigma(20)
	auxTag := &common.AuxTag{
		PiRP:   []byte("pi-rp"),
		AuxKey: &common.AuxKey{},
	}

	suite.expost.state = sigma
	suite.expost.gradeFunc = edgeCaseGradeFunc

	// Create FSigmaExp
	fSigmaExp := &common.FSigmaExp{
		Challenge: []byte("test-challenge"),
		Sigma:     sigma,
	}

	suite.mockNetwork.On("GetNodeID").Return("test-node").Times(32)

	results, err := suite.expost.Verify(suite.sid, []byte("low-grade-vk"), fSigmaExp, auxTag, 0.5, lowGradeFilterTagFunc)

	suite.NoError(err)
	suite.NotNil(results)
}

func (suite *ExPostTestSuite) TestHandleMessageWithNilAux() {
	suite.expost.isRunning = true

	msg := &pb.TimestampMessage{
		SessionId:       suite.sid,
		VerificationKey: suite.vk,
		Value:           []byte("test-value"),
		Aux:             nil,
		MerklePath:      []*pb.State{},
		Round:           1,
		Id:              "node1",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	// Set up mock expectations for IsNeighbor call
	suite.mockNetwork.On("IsNeighbor", createTestPeerID("node1")).Return(true).Once()
	suite.mockNetwork.On("GetNodeID").Return("test-node").Once()

	// This should panic due to nil aux
	suite.Panics(func() {
		_ = suite.expost.handleMessage(createTestPeerID("node1"), msgBytes)
	})

	// Verify mock expectations
	suite.mockNetwork.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestHandleMessageWithNilAuxKey() {
	suite.expost.isRunning = true

	msg := &pb.TimestampMessage{
		SessionId:       suite.sid,
		VerificationKey: suite.vk,
		Value:           []byte("test-value"),
		Aux: &pb.Aux{
			AuxKey: nil,
		},
		MerklePath: []*pb.State{},
		Round:      1,
		Id:         "node1",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	// Set up mock expectations for IsNeighbor call
	suite.mockNetwork.On("IsNeighbor", createTestPeerID("node1")).Return(true).Once()
	suite.mockNetwork.On("GetNodeID").Return("test-node").Once()

	// This should panic due to a nil aux key
	suite.Panics(func() {
		_ = suite.expost.handleMessage(createTestPeerID("node1"), msgBytes)
	})

	// Verify mock expectations
	suite.mockNetwork.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestValidateMerklePathWithEmptyPath() {
	// This test should test isMessageValid with empty path, not validateMerklePath directly
	msg := &receivedMessage{
		sid:        suite.sid,
		vk:         []byte("test-vk"),
		v:          []byte("test-value"),
		aux:        &common.AuxTag{AuxKey: &common.AuxKey{}},
		merklePath: [][][]byte{}, // Empty path
	}

	// Should fail at length check before validateMerklePath is called
	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterTagFunc, 1)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestValidateMerklePathWithEmptyState() {
	merklePath := [][][]byte{{[]byte("path1")}}
	suite.expost.state = [][][]byte{}

	// Mock GetComputedLabel call
	suite.mockMDAG.On("GetComputedLabel", mock.Anything).Return([]byte("label")).Once()

	isValid := suite.expost.validateMerklePath(merklePath, 1)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestValidateMerklePathOracleFailure() {
	merklePath := [][][]byte{{[]byte("path1")}}
	suite.expost.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("different-result")},
	}

	// Mock GetComputedLabel call
	suite.mockMDAG.On("GetComputedLabel", mock.Anything).Return([]byte("not-in-path")).Once()

	isValid := suite.expost.validateMerklePath(merklePath, 1)
	suite.False(isValid)

	suite.mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestIsValueInStateWithEmptyState() {
	var state [][]byte
	suite.False(isValueInState([]byte("value"), state))
}

func (suite *ExPostTestSuite) TestIsValueInStateWithNilValue() {
	state := [][]byte{[]byte("value1"), []byte("value2")}
	suite.False(isValueInState(nil, state))
}

func (suite *ExPostTestSuite) TestIsValueInStateWithEmptyValue() {
	state := [][]byte{[]byte(""), []byte("value2")}
	suite.True(isValueInState([]byte(""), state))
}

func (suite *ExPostTestSuite) TestConvertTimestampToBytesWithEmptyMessage() {
	msg := &pb.TimestampMessage{
		MerklePath: []*pb.State{},
	}

	result := convertTimestampToBytes(msg)
	suite.Len(result, 0)
}

func (suite *ExPostTestSuite) TestConvertTimestampToBytesWithAllNilStates() {
	msg := &pb.TimestampMessage{
		MerklePath: []*pb.State{nil, nil, nil},
	}

	result := convertTimestampToBytes(msg)
	suite.Len(result, 3)
	for _, state := range result {
		suite.Nil(state)
	}
}

func (suite *ExPostTestSuite) TestMessageValidationWithSimpleMerklePath() {
	msg := &receivedMessage{
		sid:        suite.sid,
		vk:         suite.vk,
		v:          []byte("test-value"),
		aux:        &common.AuxTag{AuxKey: &common.AuxKey{}},
		merklePath: [][][]byte{},
	}

	suite.expost.state = [][][]byte{
		{[]byte("state0")},
	}
	suite.expost.gradeFunc = edgeCaseGradeFunc

	// This should fail because the edgeCaseGradeFunc returns 0 for these values
	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterTagFunc, 0)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestConcurrentMessageHandling() {
	suite.expost.isRunning = true

	msg1 := &pb.TimestampMessage{
		SessionId:       suite.sid,
		VerificationKey: []byte("test-vk1"),
		Value:           []byte("test-value1"),
		Aux:             &pb.Aux{AuxKey: &pb.AuxKeyMessage{}},
		MerklePath:      []*pb.State{},
		Round:           1,
		Id:              "node1",
	}

	msg2 := &pb.TimestampMessage{
		SessionId:       suite.sid,
		VerificationKey: []byte("test-vk2"),
		Value:           []byte("test-value2"),
		Aux:             &pb.Aux{AuxKey: &pb.AuxKeyMessage{}},
		MerklePath:      []*pb.State{},
		Round:           1,
		Id:              "node2",
	}

	msgBytes1, _ := proto.Marshal(msg1)
	msgBytes2, _ := proto.Marshal(msg2)

	// Set up mock expectations for IsNeighbor calls
	suite.mockNetwork.On("IsNeighbor", createTestPeerID("node1")).Return(true).Once()
	suite.mockNetwork.On("IsNeighbor", createTestPeerID("node2")).Return(true).Once()

	// Set up mock expectations for GetNodeID calls (each handleMessage call needs this for metrics)
	suite.mockNetwork.On("GetNodeID").Return("test-node").Twice()

	done := make(chan bool, 2)
	go func() {
		_ = suite.expost.handleMessage(createTestPeerID("node1"), msgBytes1)
		done <- true
	}()
	go func() {
		_ = suite.expost.handleMessage(createTestPeerID("node2"), msgBytes2)
		done <- true
	}()

	<-done
	<-done

	suite.expost.mu.Lock()
	messages := suite.expost.messages[1]
	suite.expost.mu.Unlock()

	suite.Len(messages, 2)

	// Verify mock expectations
	suite.mockNetwork.AssertExpectations(suite.T())
}

// Additional helper functions for edge cases
func edgeCaseGradeFunc(_ string, vk []byte, _ []byte, _ *common.AuxKey, _ float64) int {
	if string(vk) == "high-grade-vk" {
		return 10
	}
	if string(vk) == "low-grade-vk" {
		return 1
	}
	return 0
}

func highGradeFilterTagFunc(_, _ string, vk []byte, _ []byte, _ *common.AuxTag) bool {
	return string(vk) == "high-grade-vk"
}

func lowGradeFilterTagFunc(_, _ string, vk []byte, _ []byte, _ *common.AuxTag) bool {
	return string(vk) == "low-grade-vk"
}

func (suite *ExPostTestSuite) TestGenerateNilLabel() {
	expectedState := [][][]byte{
		{[]byte("state1")},
		{[]byte("state2")},
	}

	suite.mockMDAG.On("Generate", suite.sid, suite.vk, mock.Anything).Return(expectedState, nil).Once()
	suite.mockMDAG.On("GetComputedLabel", mock.Anything).Return([]byte(nil)).Once() // Return nil label

	state, label, err := suite.expost.Generate(suite.sid, suite.vk)

	suite.Error(err)
	suite.Nil(state)
	suite.Nil(label)
	suite.Contains(err.Error(), "computed label for round R is nil")

	suite.mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestVerifyWithMessageProcessingAndPropagation() {
	sigma := createTestSigma(20)
	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	suite.expost.state = sigma

	// Add a valid message to process with sufficient merkle path layers
	validMsg := receivedMessage{
		sid: suite.sid,
		id:  "test-node",
		vk:  []byte("test-vk"),
		v:   []byte("test-value"),
		aux: &common.AuxTag{
			AuxKey: &common.AuxKey{
				PhiVRF: []byte("phi-vrf"),
				PiVRF:  []byte("pi-vrf"),
				PhiVDF: []byte("phi-vdf"),
				PiVDF:  []byte("pi-vdf"),
			},
		},
		merklePath: [][][]byte{
			{[]byte("test-value")},
			{[]byte("layer1")},
			{[]byte("layer2")},
		},
	}

	suite.expost.mu.Lock()
	suite.expost.messages[0] = []receivedMessage{validMsg}
	suite.expost.mu.Unlock()

	// Mock expectations for message validation
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("test-value")).Once()
	suite.mockMDAG.On("GetComputedLabel", mock.Anything).Return([]byte("test-value")).Once()

	// Mock network calls
	suite.mockNetwork.On("GetNodeID").Return("test-node").Times(15)
	suite.mockNetwork.On("SendProtocolMessage", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Once()

	// Create FSigmaExp
	fSigmaExp := &common.FSigmaExp{
		Challenge: []byte("test-challenge"),
		Sigma:     sigma,
	}

	results, err := suite.expost.Verify(suite.sid, suite.vk, fSigmaExp, auxTag, 0.5, mockFilterTagFunc)

	suite.NoError(err)
	suite.NotNil(results)
	suite.Equal(results.Len(), 1) // Should have one result from the processed message

	// Verify the result contains the expected data
	result, exists := results.Get(
		base64.StdEncoding.EncodeToString([]byte("test-vk")),
		base64.StdEncoding.EncodeToString([]byte("test-value")),
	)
	suite.True(exists)
	suite.Equal("test-node", result.ID)
	suite.Equal(3, result.Grade) // min(d-r/D, gradeFunc) = min(3-1/5, 5) = min(3, 5) = 3

	suite.mockMDAG.AssertExpectations(suite.T())
	suite.mockNetwork.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestVerifyWithLowerGradeMessage() {
	sigma := createTestSigma(20)
	auxTag := &common.AuxTag{
		PiRP:   []byte("pi-rp"),
		AuxKey: &common.AuxKey{},
	}

	suite.expost.state = sigma

	// Add two messages with different grades and sufficient merkle path layers
	msg1 := receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),
		v:   []byte("test-value"),
		aux: &common.AuxTag{AuxKey: &common.AuxKey{}},
		merklePath: [][][]byte{
			{[]byte("test-value")},
			{[]byte("layer1")},
			{[]byte("layer2")},
		},
	}

	msg2 := receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),    // Same VK
		v:   []byte("test-value"), // Same value
		aux: &common.AuxTag{AuxKey: &common.AuxKey{}},
		merklePath: [][][]byte{
			{[]byte("test-value")},
			{[]byte("layer1")},
			{[]byte("layer2")},
		},
	}

	suite.expost.mu.Lock()
	suite.expost.messages[0] = []receivedMessage{msg1, msg2}
	suite.expost.mu.Unlock()

	// Mock expectations
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("test-value")).Twice()
	suite.mockMDAG.On("GetComputedLabel", mock.Anything).Return([]byte("test-value")).Twice()
	suite.mockNetwork.On("GetNodeID").Return("test-node").Times(15)
	suite.mockNetwork.On("SendProtocolMessage", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Once()

	// Create FSigmaExp
	fSigmaExp := &common.FSigmaExp{
		Challenge: []byte("test-challenge"),
		Sigma:     sigma,
	}

	results, err := suite.expost.Verify(suite.sid, suite.vk, fSigmaExp, auxTag, 0.5, mockFilterTagFunc)

	suite.NoError(err)
	suite.NotNil(results)
	suite.Equal(results.Len(), 1) // Should still have only one result (higher grade wins)

	suite.mockMDAG.AssertExpectations(suite.T())
	suite.mockNetwork.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestIsMessageValidWithNilValue() {
	msg := &receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),
		v:   nil,
		aux: &common.AuxTag{AuxKey: &common.AuxKey{}},
		merklePath: [][][]byte{
			{[]byte("test-value")},
		},
	}

	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterTagFunc, 1)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestIsMessageValidFilterFnFails() {
	msg := &receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),
		v:   []byte("test-value"),
		aux: &common.AuxTag{AuxKey: &common.AuxKey{}},
		merklePath: [][][]byte{
			{[]byte("test-value")},
		},
	}

	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterTagFuncFalse, 1)
	suite.False(isValid)

	suite.mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestIsMessageValidValueNotInMerklePath() {
	msg := &receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),
		v:   []byte("test-value"),
		aux: &common.AuxTag{AuxKey: &common.AuxKey{}},
		merklePath: [][][]byte{
			{[]byte("test-value")},
		},
	}

	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("another-value")).Once()

	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterTagFunc, 1)
	suite.False(isValid)

	suite.mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestIsMessageValidWithInvalidMerklePath() {
	msg := &receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),
		v:   []byte("test-value"),
		aux: &common.AuxTag{AuxKey: &common.AuxKey{}},
		merklePath: [][][]byte{
			{[]byte("test-value")},
		},
	}

	// Mock Oracle to pass value check
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("test-value")).Once()
	// Mock GetComputedLabel to return different value so validateMerklePath fails
	suite.mockMDAG.On("GetComputedLabel", mock.Anything).Return([]byte("different-label")).Once()

	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterTagFunc, 1)
	suite.False(isValid) // Should be false because validateMerklePath returns false

	suite.mockMDAG.AssertExpectations(suite.T())
}
