package exante

import (
	"errors"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/network"
	pb "github.com/mamorski/committee-sampling/pkg/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// MockNetwork is a mock implementation of the network.Network interface
type MockNetwork struct {
	mock.Mock
}

func (m *MockNetwork) GetNodeID() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockNetwork) GetNeighbors() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockNetwork) RegisterHandler(protocolID string, handler network.MessageHandler) {
	m.Called(protocolID, handler)
}

func (m *MockNetwork) SendProtocolMessage(protocolID string, data []byte) {
	m.Called(protocolID, data)
}

func (m *MockNetwork) Close() error {
	args := m.Called()
	return args.Error(0)
}

// MockMDAG is a mock implementation of the MDAG interface
type MockMDAG struct {
	mock.Mock
}

func (m *MockMDAG) Generate(sid string, vk []byte, vi ...[]byte) ([][][]byte, error) {
	args := m.Called(sid, vk, vi)
	return args.Get(0).([][][]byte), args.Error(1)
}

func (m *MockMDAG) GetStateForRound(round int) [][]byte {
	args := m.Called(round)
	return args.Get(0).([][]byte)
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

func filterTrue(_, _ string, _ []byte, _ []byte, _ *common.AuxTag) bool {
	// Always return true for testing purposes
	return true
}

func filterFalse(_, _ string, _ []byte, _ []byte, _ *common.AuxTag) bool {
	// Always return false for testing purposes
	return false
}

// ExAnteTestSuite defines the test suite for ExAnte
type ExAnteTestSuite struct {
	suite.Suite
	mockNetwork *MockNetwork
	mockMDAG    *MockMDAG
	logger      *zap.Logger
	exante      *ExAnte

	// Test data
	testSID       string
	testStartTime time.Time
	testTimeout   time.Duration
	testD         int
	testBigD      int
	testVK        []byte
	testChallenge []byte
	testNeighbors []string
	testNodeID    string
}

func (suite *ExAnteTestSuite) SetupTest() {
	suite.mockNetwork = new(MockNetwork)
	suite.mockMDAG = new(MockMDAG)
	suite.logger = zap.NewNop()

	// Test data
	suite.testSID = "test-session-123"
	suite.testStartTime = time.Now().Add(time.Second)
	suite.testTimeout = 100 * time.Millisecond
	suite.testD = 3
	suite.testBigD = 2
	suite.testVK = []byte("test-verification-key")
	suite.testChallenge = []byte("test-challenge")
	suite.testNeighbors = []string{"node1", "node2", "node3"}
	suite.testNodeID = "test-node"
	//
	// // Setup mock expectations for constructor
	// suite.mockNetwork.On("GetNeighbors").Return(suite.testNeighbors)
	// suite.mockNetwork.On("RegisterHandler", mock.AnythingOfType("string"), mock.AnythingOfType("network.MessageHandler")).Return()

	// Create ExAnte instance manually (not using New)
	suite.exante = &ExAnte{
		network:       suite.mockNetwork,
		logger:        suite.logger.Named("exante"),
		mdag:          suite.mockMDAG,
		roundTimeout:  suite.testTimeout,
		startTime:     suite.testStartTime,
		d:             suite.testD,
		D:             suite.testBigD,
		gradeFunction: suite.mockGradeFunction,
		messages:      make(map[int]map[string]receivedMessage),
		neighbors:     make(map[string]bool),
		sid:           suite.testSID,
		isRunning:     true,
	}

	// Setup neighbors
	for _, neighbor := range suite.testNeighbors {
		suite.exante.neighbors[neighbor] = true
	}
}

func (suite *ExAnteTestSuite) TearDownTest() {
	// Reset mocks for next test
	suite.mockNetwork.ExpectedCalls = nil
	suite.mockMDAG.ExpectedCalls = nil
}

// mockGradeFunction is a simple grade function for testing
func (suite *ExAnteTestSuite) mockGradeFunction(_ string, _ []byte, _ []byte, _ *common.AuxKey, _ float64) int {
	return 5 // Return a fixed grade for testing
}

func TestExAnteTestSuite(t *testing.T) {
	suite.Run(t, new(ExAnteTestSuite))
}

// TestNew tests the New constructor function
func (suite *ExAnteTestSuite) TestNew() {
	mockNetwork := new(MockNetwork)
	mockMDAG := new(MockMDAG)
	logger := zap.NewNop()

	testSID := "test-session"
	testStartTime := time.Now()
	testTimeout := 100 * time.Millisecond
	testD := 5
	testBigD := 3
	testNeighbors := []string{"peer1", "peer2", "peer3"}

	gradeFunc := func(sid string, vk []byte, ch []byte, auxKey *common.AuxKey, auxLocal float64) int {
		return 10
	}

	// Setup mock expectations
	mockNetwork.On("GetNeighbors").Return(testNeighbors).Once()
	mockNetwork.On("RegisterHandler", "/exante/1.0.0/test-session", mock.AnythingOfType("network.MessageHandler")).Once()

	// Call New function
	exante := New(mockNetwork, mockMDAG, testSID, testStartTime, testTimeout, testD, testBigD, gradeFunc, logger)

	// Verify the instance is properly initialized
	suite.NotNil(exante)
	suite.Equal(testSID, exante.sid)
	suite.Equal(testStartTime, exante.startTime)
	suite.Equal(testTimeout, exante.roundTimeout)
	suite.Equal(testD, exante.d)
	suite.Equal(testBigD, exante.D)
	suite.True(exante.isRunning)
	suite.NotNil(exante.messages)
	suite.NotNil(exante.neighbors)
	suite.Len(exante.neighbors, len(testNeighbors))

	// Verify neighbors are properly set
	for _, neighbor := range testNeighbors {
		suite.True(exante.neighbors[neighbor])
	}

	mockNetwork.AssertExpectations(suite.T())
}

// TestGenerate_Success tests successful generation
func (suite *ExAnteTestSuite) TestGenerate_Success() {
	testPiRP := []byte("test-pi-rp")
	expectedState := [][][]byte{
		{[]byte("state1"), []byte("state2")},
		{[]byte("state3"), []byte("state4")},
	}

	suite.mockMDAG.On("Generate", suite.testSID, suite.testVK, mock.MatchedBy(func(args [][]byte) bool {
		return len(args) == 2 &&
			string(args[0]) == string(suite.testChallenge) &&
			string(args[1]) == string(testPiRP)
	})).Return(expectedState, nil).Once()
	suite.mockNetwork.On("GetNodeID").Return(suite.testNodeID).Once()

	result, err := suite.exante.Generate(suite.testSID, suite.testVK, suite.testChallenge, testPiRP)

	suite.NoError(err)
	suite.Equal(expectedState, result)
	suite.Equal(expectedState, suite.exante.state)
	suite.Equal(suite.testChallenge, suite.exante.challenge)

	suite.mockMDAG.AssertExpectations(suite.T())
	suite.mockNetwork.AssertExpectations(suite.T())
}

// TestGenerate_SessionIDMismatch tests session ID mismatch error
func (suite *ExAnteTestSuite) TestGenerate_SessionIDMismatch() {
	wrongSID := "wrong-session-id"
	testPiRP := []byte("test-pi-rp")

	result, err := suite.exante.Generate(wrongSID, suite.testVK, suite.testChallenge, testPiRP)

	suite.Error(err)
	suite.Nil(result)
	suite.Contains(err.Error(), "session ID mismatch")
}

// TestGenerate_MDAGGenerationFailure tests MDAG generation failure
func (suite *ExAnteTestSuite) TestGenerate_MDAGGenerationFailure() {
	testPiRP := []byte("test-pi-rp")
	expectedError := errors.New("MDAG generation failed")

	suite.mockMDAG.On("Generate", suite.testSID, suite.testVK, mock.MatchedBy(func(args [][]byte) bool {
		return len(args) == 2 &&
			string(args[0]) == string(suite.testChallenge) &&
			string(args[1]) == string(testPiRP)
	})).Return([][][]byte(nil), expectedError).Once()
	suite.mockNetwork.On("GetNodeID").Return(suite.testNodeID).Once()

	result, err := suite.exante.Generate(suite.testSID, suite.testVK, suite.testChallenge, testPiRP)

	suite.Error(err)
	suite.Nil(result)
	suite.Contains(err.Error(), "MDAG generation failed")
	suite.False(suite.exante.isRunning)

	suite.mockMDAG.AssertExpectations(suite.T())
	suite.mockNetwork.AssertExpectations(suite.T())
}

// TestGenerate_EmptySessionID tests generation with empty session ID in ExAnte instance
func (suite *ExAnteTestSuite) TestGenerate_EmptySessionID() {
	suite.exante.sid = "" // Empty session ID should accept any session
	testPiRP := []byte("test-pi-rp")
	newSID := "new-session-id"
	expectedState := [][][]byte{
		{[]byte("state1")},
	}

	suite.mockMDAG.On("Generate", newSID, suite.testVK, mock.MatchedBy(func(args [][]byte) bool {
		return len(args) == 2 &&
			string(args[0]) == string(suite.testChallenge) &&
			string(args[1]) == string(testPiRP)
	})).Return(expectedState, nil).Once()
	suite.mockNetwork.On("GetNodeID").Return(suite.testNodeID).Once()

	result, err := suite.exante.Generate(newSID, suite.testVK, suite.testChallenge, testPiRP)

	suite.NoError(err)
	suite.Equal(expectedState, result)

	suite.mockMDAG.AssertExpectations(suite.T())
	suite.mockNetwork.AssertExpectations(suite.T())
}

// TestHandleMessage_Success tests successful message handling
func (suite *ExAnteTestSuite) TestHandleMessage_Success() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	msg := createTestTimestampMessage(suite.testSID, suite.testVK, suite.testChallenge, testAux, 0, "node1")
	msgBytes := marshalMessage(suite.T(), msg)

	err := suite.exante.handleMessage("node1", msgBytes)

	suite.NoError(err)
	suite.Contains(suite.exante.messages, 0)
	suite.Contains(suite.exante.messages[0], "node1")

	receivedMsg := suite.exante.messages[0]["node1"]
	suite.Equal(suite.testSID, receivedMsg.sid)
	suite.Equal(suite.testVK, receivedMsg.vk)
	suite.Equal(suite.testChallenge, receivedMsg.ch)
	suite.Equal(testAux.PiRP, receivedMsg.aux.PiRP)
}

// TestHandleMessage_ProtocolNotRunning tests handling message when protocol is not running
func (suite *ExAnteTestSuite) TestHandleMessage_ProtocolNotRunning() {
	suite.exante.isRunning = false

	err := suite.exante.handleMessage("node1", []byte("test"))

	suite.Error(err)
	suite.Contains(err.Error(), "protocol not running")
}

// TestHandleMessage_UnmarshalError tests handling invalid message bytes
func (suite *ExAnteTestSuite) TestHandleMessage_UnmarshalError() {
	invalidBytes := []byte("invalid-protobuf-data")

	err := suite.exante.handleMessage("node1", invalidBytes)

	suite.Error(err)
}

// TestHandleMessage_SessionIDMismatch tests handling message with wrong session ID
func (suite *ExAnteTestSuite) TestHandleMessage_SessionIDMismatch() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	msg := createTestTimestampMessage("wrong-session", suite.testVK, suite.testChallenge, testAux, 0, "node1")
	msgBytes := marshalMessage(suite.T(), msg)

	err := suite.exante.handleMessage("node1", msgBytes)

	suite.Error(err)
	suite.Contains(err.Error(), "session id mismatch")
}

// TestHandleMessage_SenderIDMismatch tests handling message with wrong sender ID
func (suite *ExAnteTestSuite) TestHandleMessage_SenderIDMismatch() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	msg := createTestTimestampMessage(suite.testSID, suite.testVK, suite.testChallenge, testAux, 0, "wrong-sender")
	msgBytes := marshalMessage(suite.T(), msg)

	err := suite.exante.handleMessage("node1", msgBytes)

	suite.Error(err)
	suite.Contains(err.Error(), "sender id mismatch")
}

// TestHandleMessage_UnknownNeighbor tests handling message from unknown neighbor
func (suite *ExAnteTestSuite) TestHandleMessage_UnknownNeighbor() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	msg := createTestTimestampMessage(suite.testSID, suite.testVK, suite.testChallenge, testAux, 0, "unknown-node")
	msgBytes := marshalMessage(suite.T(), msg)

	err := suite.exante.handleMessage("unknown-node", msgBytes)

	suite.Error(err)
	suite.Contains(err.Error(), "sender not in neighbors list")
}

// TestHandleMessage_DuplicateMessage tests handling duplicate message from same sender
func (suite *ExAnteTestSuite) TestHandleMessage_DuplicateMessage() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	msg := createTestTimestampMessage(suite.testSID, suite.testVK, suite.testChallenge, testAux, 0, "node1")
	msgBytes := marshalMessage(suite.T(), msg)

	// Send first message
	err1 := suite.exante.handleMessage("node1", msgBytes)
	suite.NoError(err1)

	// Send duplicate message
	err2 := suite.exante.handleMessage("node1", msgBytes)
	suite.NoError(err2) // Should not error, just ignore

	// Verify only one message is stored
	suite.Len(suite.exante.messages[0], 1)
}

// TestValidateMerklePath_Success tests successful Merkle path validation
func (suite *ExAnteTestSuite) TestValidateMerklePath_Success() {
	merklePath := [][][]byte{
		{[]byte("path1"), []byte("path2")},
		{[]byte("path3"), []byte("oracle-result")},
	}
	round := 2

	// Setup state with enough rounds
	suite.exante.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("state1")},
		{[]byte("oracle-result")}, // This should contain the oracle result
	}

	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result")).Twice()

	result := suite.exante.validateMerklePath(merklePath, round)

	suite.True(result)

	suite.mockMDAG.AssertExpectations(suite.T())
}

// TestValidateMerklePath_InvalidLength tests Merkle path with invalid length
func (suite *ExAnteTestSuite) TestValidateMerklePath_InvalidLength() {
	merklePath := [][][]byte{
		{[]byte("path1")},
	}
	round := 3 // Expecting more paths than provided

	result := suite.exante.validateMerklePath(merklePath, round)

	suite.False(result)
}

// TestValidateMerklePath_InvalidStateLength tests with insufficient state length
func (suite *ExAnteTestSuite) TestValidateMerklePath_InvalidStateLength() {
	merklePath := [][][]byte{
		{[]byte("path1")},
		{[]byte("path2")},
	}
	round := 2

	// Setup state with insufficient rounds
	suite.exante.state = [][][]byte{
		{[]byte("state0")},
	}

	result := suite.exante.validateMerklePath(merklePath, round)

	suite.False(result)
}

// TestValidateMerklePath_OracleNotInNextLevel tests when oracle result is not found in next level
func (suite *ExAnteTestSuite) TestValidateMerklePath_OracleNotInNextLevel() {
	merklePath := [][][]byte{
		{[]byte("path1"), []byte("path2")},
		{[]byte("path3"), []byte("path4")}, // Oracle result will NOT be found here
		{[]byte("path5"), []byte("path6")},
	}
	round := 3

	// Setup state with enough rounds
	suite.exante.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("state1")},
		{[]byte("state2")},
		{[]byte("state3")},
	}

	// Mock Oracle calls - first call returns a value that's NOT in merklePath[1]
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result-not-found")).Once()

	result := suite.exante.validateMerklePath(merklePath, round)

	suite.False(result) // Should return false because oracle result is not in next level

	suite.mockMDAG.AssertExpectations(suite.T())
}

// TestValidateMerklePath_MultipleRoundsSuccess tests successful validation with multiple rounds
func (suite *ExAnteTestSuite) TestValidateMerklePath_MultipleRoundsSuccess() {
	merklePath := [][][]byte{
		{[]byte("path1"), []byte("path2")},
		{[]byte("oracle-result1"), []byte("path4")}, // First oracle result found here
		{[]byte("oracle-result2"), []byte("path6")}, // Second oracle result found here
	}
	round := 3

	// Setup state with enough rounds
	suite.exante.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("state1")},
		{[]byte("state2")},
		{[]byte("oracle-result2")}, // Final oracle result should be found in state[3]
	}

	// Mock Oracle calls for each level
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result1")).Once() // i=0, found in merklePath[1]
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result2")).Once() // i=1, found in merklePath[2]
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result2")).Once() // i=2 (final), found in state[3]

	result := suite.exante.validateMerklePath(merklePath, round)

	suite.True(result)

	suite.mockMDAG.AssertExpectations(suite.T())
}

// TestIsMessageValid_Success tests successful message validation
func (suite *ExAnteTestSuite) TestIsMessageValid_Success() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	msg := &receivedMessage{
		sid: suite.testSID,
		vk:  suite.testVK,
		ch:  suite.testChallenge,
		aux: testAux,
		merklePath: [][][]byte{
			{[]byte("oracle-result")},
		},
	}

	// Setup state
	suite.exante.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("oracle-result")},
	}

	// Mock Oracle and filter function
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result")).Twice()

	result := suite.exante.isMessageValid(msg, 1.0, filterTrue, 1)

	suite.True(result)
	suite.mockMDAG.AssertExpectations(suite.T())
}

// TestIsMessageValid_NilMessage tests with nil message
func (suite *ExAnteTestSuite) TestIsMessageValid_NilMessage() {

	result := suite.exante.isMessageValid(nil, 1.0, filterTrue, 1)

	suite.False(result)
}

// TestIsMessageValid_FilterFails tests when filter function returns false
func (suite *ExAnteTestSuite) TestIsMessageValid_FilterFails() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	msg := &receivedMessage{
		sid: suite.testSID,
		vk:  suite.testVK,
		ch:  suite.testChallenge,
		aux: testAux,
		merklePath: [][][]byte{
			{[]byte("path1")},
		},
	}

	result := suite.exante.isMessageValid(msg, 1.0, filterFalse, 1)

	suite.False(result)
}

// TestIsValueInState_Found tests when value is found in state
func (suite *ExAnteTestSuite) TestIsValueInState_Found() {
	value := []byte("test-value")
	state := [][]byte{
		[]byte("other-value"),
		[]byte("test-value"),
		[]byte("another-value"),
	}

	result := isValueInState(value, state)

	suite.True(result)
}

// TestIsValueInState_NotFound tests when value is not found in state
func (suite *ExAnteTestSuite) TestIsValueInState_NotFound() {
	value := []byte("missing-value")
	state := [][]byte{
		[]byte("other-value"),
		[]byte("test-value"),
		[]byte("another-value"),
	}

	result := isValueInState(value, state)

	suite.False(result)
}

// TestConvertTimestampToBytes tests conversion of protobuf message to bytes
func (suite *ExAnteTestSuite) TestConvertTimestampToBytes() {
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

// Helper functions for creating test messages

// createTestTimestampMessage creates a test TimestampMessage
//
//nolint:unparam
func createTestTimestampMessage(
	sessionID string, vk []byte, challenge []byte, aux *common.AuxTag, round uint32, from string) *pb.TimestampMessage {

	return &pb.TimestampMessage{
		SessionId:       sessionID,
		VerificationKey: vk,
		Value:           challenge,
		Aux: &pb.Aux{
			PiRP: aux.PiRP,
			AuxKey: &pb.AuxKeyMessage{
				PhiVrf: aux.AuxKey.PhiVRF,
				PiVrf:  aux.AuxKey.PiVRF,
				PhiVdf: aux.AuxKey.PhiVDF,
				PiVdf:  aux.AuxKey.PiVDF,
			},
		},
		MerklePath: []*pb.State{
			{Row: [][]byte{[]byte("test-merkle-path")}},
		},
		Round: round,
		Id:    from,
	}
}

// marshalMessage marshals a protobuf message to bytes
func marshalMessage(t *testing.T, msg *pb.TimestampMessage) []byte {
	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal message: %v", err)
	}
	return data
}

// TestVerify_SessionIDMismatch tests Verify with session ID mismatch
func (suite *ExAnteTestSuite) TestVerify_SessionIDMismatch() {
	wrongSession := "wrong-session"
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}
	sigma := [][][]byte{
		{[]byte("sigma0")},
		{[]byte("sigma1")},
		{[]byte("sigma2")},
		{[]byte("sigma3")},
		{[]byte("sigma4")},
		{[]byte("sigma5")},
		{[]byte("sigma6")}, // d * D = 3 * 2 = 6, so we need at least 7 elements
	}

	result, err := suite.exante.Verify(wrongSession, suite.testVK, sigma, testAux, 1.0, filterTrue)

	suite.Error(err)
	suite.Nil(result)
	suite.Contains(err.Error(), "session ID mismatch")
}

// TestVerify_InsufficientSigmaLength tests Verify with insufficient sigma length
func (suite *ExAnteTestSuite) TestVerify_InsufficientSigmaLength() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}
	sigma := [][][]byte{
		{[]byte("sigma0")},
		{[]byte("sigma1")},
		// Insufficient length: need d * D = 3 * 2 = 6, but only have 2
	}

	result, err := suite.exante.Verify(suite.testSID, suite.testVK, sigma, testAux, 1.0, filterTrue)

	suite.Error(err)
	suite.Nil(result)
	suite.Contains(err.Error(), "sigma length is less than required rounds")
	suite.False(suite.exante.isRunning)
}

// TestVerify_AsProver tests Verify when node acts as a prover
func (suite *ExAnteTestSuite) TestVerify_AsProver() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}
	sigma := [][][]byte{
		{[]byte("sigma0")},
		{[]byte("sigma1")},
		{[]byte("sigma2")},
		{[]byte("sigma3")},
		{[]byte("sigma4")},
		{[]byte("sigma5")},
		{[]byte("sigma6")}, // d * D = 3 * 2 = 6, so we need at least 7 elements
	}

	// Setup grade function to return high grade (>= d+1 = 4)
	suite.exante.gradeFunction = func(sid string, vk []byte, ch []byte, auxKey *common.AuxKey, auxLocal float64) int {
		return 5 // >= d+1 = 4, so node is a prover
	}

	// Mock network calls for sending initial message
	suite.mockNetwork.On("GetNodeID").Return(suite.testNodeID).Times(3) // Called for logging and message creation
	suite.mockNetwork.On("SendProtocolMessage", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Once()

	// Set start time to past so rounds execute immediately
	suite.exante.startTime = time.Now().Add(-time.Hour)

	result, err := suite.exante.Verify(suite.testSID, suite.testVK, sigma, testAux, 1.0, filterTrue)

	suite.NoError(err)
	suite.NotNil(result)
	suite.False(suite.exante.isRunning) // Should be false after completion

	suite.mockNetwork.AssertExpectations(suite.T())
}

// TestVerify_WithIncomingMessages tests Verify with simulated incoming messages
func (suite *ExAnteTestSuite) TestVerify_WithIncomingMessages() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}
	sigma := [][][]byte{
		{[]byte("sigma0")},
		{[]byte("sigma1")},
		{[]byte("sigma2")},
		{[]byte("sigma3")},
		{[]byte("sigma4")},
		{[]byte("sigma5")},
		{[]byte("sigma6")}, // d * D = 3 * 2 = 6, so we need at least 7 elements
	}

	// Setup grade function to return low grade (< d+1 = 4)
	suite.exante.gradeFunction = func(sid string, vk []byte, ch []byte, auxKey *common.AuxKey, auxLocal float64) int {
		return 2 // < d+1 = 4, so node is not a prover initially
	}

	// Pre-populate messages to simulate incoming messages
	suite.exante.messages[0] = map[string]receivedMessage{
		"node1": {
			sid: suite.testSID,
			vk:  suite.testVK,
			ch:  suite.testChallenge,
			aux: testAux,
			merklePath: [][][]byte{
				{[]byte("oracle-result")},
			},
		},
	}

	// Setup state for validation
	suite.exante.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("oracle-result")},
		{[]byte("state2")},
	}

	// Mock Oracle for message validation
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result")).Twice()

	// Mock network calls for forwarding messages
	suite.mockNetwork.On("GetNodeID").Return(suite.testNodeID).Times(2) // Called for logging and message creation
	suite.mockNetwork.On("SendProtocolMessage", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Once()

	// Set start time to past so rounds execute immediately
	suite.exante.startTime = time.Now().Add(-time.Hour)

	result, err := suite.exante.Verify(suite.testSID, suite.testVK, sigma, testAux, 1.0, filterTrue)

	suite.NoError(err)
	suite.NotNil(result)
	suite.False(suite.exante.isRunning)

	// Verify that the message was processed and included in results
	key := common.Key{VK: string(suite.testVK), Ch: string(suite.testChallenge)}
	suite.Contains(result, key)

	suite.mockMDAG.AssertExpectations(suite.T())
	suite.mockNetwork.AssertExpectations(suite.T())
}

// TestVerify_MessageGradeComparison tests message grade comparison logic
func (suite *ExAnteTestSuite) TestVerify_MessageGradeComparison() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}
	sigma := [][][]byte{
		{[]byte("sigma0")},
		{[]byte("sigma1")},
		{[]byte("sigma2")},
		{[]byte("sigma3")},
		{[]byte("sigma4")},
		{[]byte("sigma5")},
		{[]byte("sigma6")},
	}

	// Setup grade function to return different grades for different messages
	gradeCallCount := 0
	suite.exante.gradeFunction = func(sid string, vk []byte, ch []byte, auxKey *common.AuxKey, auxLocal float64) int {
		gradeCallCount++
		if gradeCallCount == 1 {
			return 1 // First call (prover check) - not a prover
		}
		return 3 // Subsequent calls (message processing) - valid grade
	}

	// Pre-populate messages with same key but different senders
	suite.exante.messages[0] = map[string]receivedMessage{
		"node1": {
			sid: suite.testSID,
			vk:  suite.testVK,
			ch:  suite.testChallenge,
			aux: testAux,
			merklePath: [][][]byte{
				{[]byte("oracle-result")},
			},
		},
		"node2": {
			sid: suite.testSID,
			vk:  suite.testVK,
			ch:  suite.testChallenge, // Same key as node1
			aux: testAux,
			merklePath: [][][]byte{
				{[]byte("oracle-result")},
			},
		},
	}

	// Setup state for validation
	suite.exante.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("oracle-result")},
		{[]byte("state2")},
	}

	// Mock Oracle calls for both messages
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result")).Times(4)

	// Mock network calls
	suite.mockNetwork.On("GetNodeID").Return(suite.testNodeID).Twice()
	suite.mockNetwork.On("SendProtocolMessage", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Once()

	// Set start time to past so rounds execute immediately
	suite.exante.startTime = time.Now().Add(-time.Hour)

	result, err := suite.exante.Verify(suite.testSID, suite.testVK, sigma, testAux, 1.0, filterTrue)
	suite.NoError(err)
	suite.NotNil(result)

	// Should only have one entry for the key (higher grade wins)
	key := common.Key{VK: string(suite.testVK), Ch: string(suite.testChallenge)}
	suite.Contains(result, key)
	suite.Len(result, 1)

	suite.mockMDAG.AssertExpectations(suite.T())
	suite.mockNetwork.AssertExpectations(suite.T())
}

// TestIsMessageValid_GradeZero tests when grade function returns 0
func (suite *ExAnteTestSuite) TestIsMessageValid_GradeZero() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	msg := &receivedMessage{
		sid: suite.testSID,
		vk:  suite.testVK,
		ch:  suite.testChallenge,
		aux: testAux,
		merklePath: [][][]byte{
			{[]byte("path1")},
		},
	}

	// Override grade function to return 0
	suite.exante.gradeFunction = func(sid string, vk []byte, ch []byte, auxKey *common.AuxKey, auxLocal float64) int {
		return 0 // Grade is not > 0
	}

	result := suite.exante.isMessageValid(msg, 1.0, filterTrue, 1)

	suite.False(result)
}

// TestIsMessageValid_OracleNotInFirstLevel tests when oracle result not found in first level
func (suite *ExAnteTestSuite) TestIsMessageValid_OracleNotInFirstLevel() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	msg := &receivedMessage{
		sid: suite.testSID,
		vk:  suite.testVK,
		ch:  suite.testChallenge,
		aux: testAux,
		merklePath: [][][]byte{
			{[]byte("different-value")}, // Oracle result will NOT be found here
		},
	}

	// Mock Oracle to return a value not in merklePath[0]
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result-not-found")).Once()

	result := suite.exante.isMessageValid(msg, 1.0, filterTrue, 1)

	suite.False(result)

	suite.mockMDAG.AssertExpectations(suite.T())
}

// TestIsMessageValid_ValidatePathFails tests when validateMerklePath fails
func (suite *ExAnteTestSuite) TestIsMessageValid_ValidatePathFails() {
	testAux := &common.AuxTag{
		PiRP: []byte("test-pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	msg := &receivedMessage{
		sid: suite.testSID,
		vk:  suite.testVK,
		ch:  suite.testChallenge,
		aux: testAux,
		merklePath: [][][]byte{
			{[]byte("oracle-result")}, // Oracle result found here
		},
	}

	// Setup state that will cause validateMerklePath to fail (insufficient length)
	suite.exante.state = [][][]byte{
		{[]byte("state0")},
		// Missing state[1] - will cause validateMerklePath to fail
	}

	// Mock Oracle calls
	suite.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result")).Once() // For isValueInState check
	// validateMerklePath will fail due to insufficient state length, so no more Oracle calls

	result := suite.exante.isMessageValid(msg, 1.0, filterTrue, 1)

	suite.False(result)

	suite.mockMDAG.AssertExpectations(suite.T())
}
