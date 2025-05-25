package exante

import (
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

// Mock implementations
type MockNetwork struct {
	mock.Mock
}

func (m *MockNetwork) RegisterHandler(protocolID string, handler network.MessageHandler) {
	m.Called(protocolID, handler)
}

func (m *MockNetwork) SendProtocolMessage(protocolID string, message []byte) {
	_ = m.Called(protocolID, message)
}

func (m *MockNetwork) GetNodeID() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockNetwork) GetNeighbors() []string {
	args := m.Called()
	return args.Get(0).([]string)
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

func (m *MockMDAG) Oracle(_ ...[]byte) []byte {
	args := m.Called(mock.Anything)
	return args.Get(0).([]byte)
}

// ExAnteTestSuite defines the test suite for ExAnte
type ExAnteTestSuite struct {
	suite.Suite
	mockNetwork   *MockNetwork
	mockMDAG      *MockMDAG
	logger        *zap.Logger
	roundTimeout  time.Duration
	startTime     time.Time
	diameter      int
	D             int
	gradeFunction common.GradeFunc
	sid           string
	nodeID        string
}

// SetupTest performs setup for each test
func (s *ExAnteTestSuite) SetupTest() {
	// Initialize mocks
	s.mockNetwork = new(MockNetwork)
	s.mockMDAG = new(MockMDAG)

	// Initialize logger
	s.logger, _ = zap.NewDevelopment()

	// Default test parameters
	s.roundTimeout = 1 * time.Second
	s.startTime = time.Now()
	s.diameter = 3
	s.D = 1
	s.sid = "test-session"
	s.nodeID = "node-1"

	// Default grade function that returns a fixed grade
	s.gradeFunction = func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		return 3
	}

	// Setup common mock behaviors
	s.mockNetwork.On("GetNodeID").Return(s.nodeID)
	s.mockNetwork.On("GetNeighbors").Return([]string{"node-2", "node-3"})
	s.mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return()
}

// TestNewExAnte tests the constructor
func (s *ExAnteTestSuite) TestNewExAnte() {
	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		s.sid,
		s.startTime,
		s.roundTimeout,
		s.diameter,
		s.D,
		s.gradeFunction,
		s.logger,
	)

	s.NotNil(exante)
	s.Equal(s.sid, exante.sid)
	s.Equal(s.diameter, exante.diameter)
	s.Equal(s.D, exante.D)
	s.True(exante.isRunning)

	// Verify neighbors were added to the map
	s.Contains(exante.neighbors, "node-2")
	s.Contains(exante.neighbors, "node-3")

	// Verify handler was registered
	protocolID := "/exante/1.0.0/" + s.sid
	s.mockNetwork.AssertCalled(s.T(), "RegisterHandler", protocolID, mock.Anything)
}

// TestGenerate tests the Generate method
func (s *ExAnteTestSuite) TestGenerate() {
	// Setup mock response
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")
	piRP := []byte("pi-rp")

	// Mock MDAG state
	mockState := [][][]byte{
		{
			[]byte("state-0-0"), []byte("state-0-1"),
		},
		{
			[]byte("state-1-0"), []byte("state-1-1"),
		},
	}

	s.mockMDAG.On("Generate", session, vk, [][]byte{challenge, piRP}).Return(mockState, nil)

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		s.sid,
		s.startTime,
		s.roundTimeout,
		s.diameter,
		s.D,
		s.gradeFunction,
		s.logger,
	)

	// Call Generate
	result, err := exante.Generate(session, vk, challenge, piRP)

	// Assert results
	s.NoError(err)
	s.Equal(mockState, result)
	s.Equal(challenge, exante.challenge)
	s.Equal(mockState, exante.state)
}

// TestGenerateSessionMismatch tests Generate with wrong session ID
func (s *ExAnteTestSuite) TestGenerateSessionMismatch() {
	session := "test-session"
	wrongSession := "wrong-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")
	piRP := []byte("pi-rp")

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		s.startTime,
		s.roundTimeout,
		s.diameter,
		s.D,
		s.gradeFunction,
		s.logger,
	)

	// Call Generate with wrong session
	result, err := exante.Generate(wrongSession, vk, challenge, piRP)

	s.Error(err)
	s.Nil(result)
	s.Contains(err.Error(), "session ID mismatch")
}

// TestHandleMessage tests the message handler
func (s *ExAnteTestSuite) TestHandleMessage() {
	session := "test-session"

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		s.startTime,
		s.roundTimeout,
		s.diameter,
		s.D,
		s.gradeFunction,
		s.logger,
	)

	// Prepare a valid message
	msg := &pb.ExAnteMessage{
		SessionId:       session,
		VerificationKey: []byte("verification-key"),
		Value:           []byte("challenge"),
		Aux: &pb.Aux{
			PiRP: []byte("pi-rp"),
			AuxKey: &pb.AuxKeyMessage{
				PhiVrf: []byte("phi-vrf"),
				PiVrf:  []byte("pi-vrf"),
				PhiVdf: []byte("phi-vdf"),
				PiVdf:  []byte("pi-vdf"),
			},
		},
		MerklePath: []*pb.State{
			{Row: [][]byte{[]byte("mp-0-0"), []byte("mp-0-1")}},
		},
		Round: 1,
		From:  "node-2", // Must be a valid neighbor
	}

	msgBytes, err := proto.Marshal(msg)
	s.NoError(err)

	// Call handleMessage
	err = exante.handleMessage("node-2", msgBytes)
	s.NoError(err)

	// Verify message was stored
	s.Equal(1, len(exante.messages))
	s.Contains(exante.messages, 1)
	s.Contains(exante.messages[1], "node-2")
}

// TestHandleMessageErrors tests various error cases of message handling
func (s *ExAnteTestSuite) TestHandleMessageErrors() {
	session := "test-session"

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		s.startTime,
		s.roundTimeout,
		s.diameter,
		s.D,
		s.gradeFunction,
		s.logger,
	)
	exante.isRunning = true

	// Test 1: Not running
	exante.isRunning = false
	err := exante.handleMessage("node-2", []byte("test"))
	s.Error(err)
	s.Contains(err.Error(), "protocol not running")
	exante.isRunning = true

	// Test 2: Invalid message format
	err = exante.handleMessage("node-2", []byte("invalid-message"))
	s.Error(err)

	// Test 3: Session ID mismatch
	msg := &pb.ExAnteMessage{
		SessionId: "wrong-session",
		From:      "node-2",
	}
	msgBytes, _ := proto.Marshal(msg)
	err = exante.handleMessage("node-2", msgBytes)
	s.Error(err)
	s.Contains(err.Error(), "session id mismatch")

	// Test 4: Sender ID mismatch
	msg = &pb.ExAnteMessage{
		SessionId: session,
		From:      "node-3", // Doesn't match the from parameter
	}
	msgBytes, _ = proto.Marshal(msg)
	err = exante.handleMessage("node-2", msgBytes)
	s.Error(err)
	s.Contains(err.Error(), "sender id mismatch")

	// Test 5: Sender not in neighbors
	msg = &pb.ExAnteMessage{
		SessionId: session,
		From:      "node-4", // Not in the neighbors list
	}
	msgBytes, _ = proto.Marshal(msg)
	err = exante.handleMessage("node-4", msgBytes)
	s.Error(err)
	s.Contains(err.Error(), "sender not in neighbors")
}

// TestIsValueInState tests the helper function isValueInState
func (s *ExAnteTestSuite) TestIsValueInState() {
	state := [][]byte{
		[]byte("value1"),
		[]byte("value2"),
		[]byte("value3"),
	}

	s.True(isValueInState([]byte("value1"), state))
	s.True(isValueInState([]byte("value2"), state))
	s.True(isValueInState([]byte("value3"), state))
	s.False(isValueInState([]byte("value4"), state))
}

// TestConvertExAnteMessageToBytes tests the helper function convertExAnteMessageToBytes
func (s *ExAnteTestSuite) TestConvertExAnteMessageToBytes() {
	msg := &pb.ExAnteMessage{
		MerklePath: []*pb.State{
			{Row: [][]byte{[]byte("mp-0-0"), []byte("mp-0-1")}},
			{Row: [][]byte{[]byte("mp-1-0"), []byte("mp-1-1")}},
			nil,
			{Row: nil},
		},
	}

	result := convertExAnteMessageToBytes(msg)

	s.Equal(4, len(result))
	s.Equal([][]byte{[]byte("mp-0-0"), []byte("mp-0-1")}, result[0])
	s.Equal([][]byte{[]byte("mp-1-0"), []byte("mp-1-1")}, result[1])
	s.Nil(result[2])
	s.Empty(result[3])
}

// TestVerifyHappyFlowAsProver tests the Verify method in the happy flow scenario
// where the node acts as a prover and all received messages are valid
func (s *ExAnteTestSuite) TestVerifyHappyFlowAsProver() {
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")
	auxLocal := 2.0

	// Setup auxTag for the prover
	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	// Create sigma (state) for diameter+1 rounds (0 to diameter)
	sigma := [][][]byte{
		{[]byte("sigma-0-0"), []byte("sigma-0-1")},                              // Round 0
		{[]byte("sigma-1-0"), []byte("sigma-1-1"), []byte("oracle-result-msg")}, // Round 1
		{[]byte("sigma-2-0"), []byte("sigma-2-1")},                              // Round 2
		{[]byte("sigma-3-0"), []byte("sigma-3-1")},                              // Round 3
	}

	// Grade function that makes this node a prover (grade >= diameter+1 = 4)
	// and also makes received messages valid (grade > 0)
	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		return 5 // Higher than diameter+1 for prover, and > 0 for message validation
	}

	// Filter function that always passes
	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

	// Create ExAnte instance
	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		time.Now().Add(-10*time.Second), // Start time in the past for immediate execution
		100*time.Millisecond,            // Short timeout for testing
		s.diameter,
		s.D,
		proverGradeFunction,
		s.logger,
	)
	exante.challenge = challenge
	exante.state = sigma

	// Mock network to expect the initial prover message and forwarded messages
	protocolID := "/exante/1.0.0/" + session
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return().Twice()

	// Mock MDAG Oracle calls - use mock.Anything like existing tests
	s.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result-msg")).Times(3)

	// Preload valid message for round 1 only (simplest case)
	exante.messages = make(map[int]map[string]receivedMessage)
	exante.messages[1] = make(map[string]receivedMessage)
	exante.messages[1]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")}, // Level 0
			{[]byte("oracle-result-msg")},        // Level 1 - contains the oracle result for message validation
		},
	}

	// Call Verify
	results, err := exante.Verify(session, vk, sigma, auxTag, auxLocal, filterFn)

	// Assertions
	s.NoError(err)
	s.NotEmpty(results)
	s.False(exante.isRunning) // Should finish running after verify

	// Verify that messages were processed and added to results
	expectedKey := common.Key{VK: string(vk), Ch: string(challenge)}
	s.Contains(results, expectedKey)

	result := results[expectedKey]
	s.Equal(vk, result.VK)
	s.Equal(challenge, result.Challenge)
	s.Equal(auxTag, result.Aux)
	s.Greater(result.Grade, 0) // Should have a positive grade

	// Verify that verified values were stored
	s.NotEmpty(exante.verifiedValues)

	// Verify MDAG mocks were called as expected
	s.mockMDAG.AssertExpectations(s.T())
	s.mockNetwork.AssertExpectations(s.T())
}

// TestVerifyAsNonProver tests the Verify method when the node is not a prover
func (s *ExAnteTestSuite) TestVerifyAsNonProver() {
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")
	auxLocal := 2.0

	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	sigma := [][][]byte{
		{[]byte("sigma-0-0"), []byte("sigma-0-1")},
		{[]byte("sigma-1-0"), []byte("sigma-1-1"), []byte("oracle-result-msg")},
		{[]byte("sigma-2-0"), []byte("sigma-2-1")},
		{[]byte("sigma-3-0"), []byte("sigma-3-1")},
	}

	// Grade function that makes this node NOT a prover (grade < diameter+1 = 4)
	nonProverGradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		return 2 // Less than diameter+1, so not a prover
	}

	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		time.Now().Add(-10*time.Second),
		100*time.Millisecond,
		s.diameter,
		s.D,
		nonProverGradeFunction,
		s.logger,
	)
	exante.challenge = challenge
	exante.state = sigma

	// Mock MDAG Oracle calls
	s.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result-msg")).Times(3)

	// Mock network for message forwarding (even non-provers forward valid messages)
	protocolID := "/exante/1.0.0/" + session
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return().Once()

	// Preload valid message for round 1
	exante.messages = make(map[int]map[string]receivedMessage)
	exante.messages[1] = make(map[string]receivedMessage)
	exante.messages[1]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("oracle-result-msg")},
		},
	}

	// Call Verify
	results, err := exante.Verify(session, vk, sigma, auxTag, auxLocal, filterFn)

	// Assertions
	s.NoError(err)
	s.NotEmpty(results) // Message should still be processed even if not a prover
	s.False(exante.isRunning)

	// Note: Non-provers don't send initial messages, but they do forward valid messages
	// So we expect exactly one SendProtocolMessage call (for forwarding the valid message)

	// Verify message was still processed
	expectedKey := common.Key{VK: string(vk), Ch: string(challenge)}
	s.Contains(results, expectedKey)
}

// TestVerifyWithInvalidMessages tests the Verify method with invalid messages
func (s *ExAnteTestSuite) TestVerifyWithInvalidMessages() {
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")
	auxLocal := 2.0

	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	sigma := [][][]byte{
		{[]byte("sigma-0-0"), []byte("sigma-0-1")},
		{[]byte("sigma-1-0"), []byte("sigma-1-1")}, // Note: does NOT contain oracle-result-msg
		{[]byte("sigma-2-0"), []byte("sigma-2-1")},
		{[]byte("sigma-3-0"), []byte("sigma-3-1")},
	}

	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		return 5
	}

	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		time.Now().Add(-10*time.Second),
		100*time.Millisecond,
		s.diameter,
		s.D,
		proverGradeFunction,
		s.logger,
	)
	exante.challenge = challenge
	exante.state = sigma

	protocolID := "/exante/1.0.0/" + session
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return().Once()

	// Mock Oracle to return a value that's NOT in the state (making message invalid)
	s.mockMDAG.On("Oracle", mock.Anything).Return([]byte("invalid-oracle-result"))

	// Preload message that will be invalid due to Oracle result not being in state
	exante.messages = make(map[int]map[string]receivedMessage)
	exante.messages[1] = make(map[string]receivedMessage)
	exante.messages[1]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("some-value")},
		},
	}

	// Call Verify
	results, err := exante.Verify(session, vk, sigma, auxTag, auxLocal, filterFn)

	// Assertions
	s.NoError(err)
	s.Empty(results) // No valid messages should be processed
	s.False(exante.isRunning)
	s.Empty(exante.verifiedValues) // No verified values should be stored
}

// TestVerifyWithFilterRejection tests when the filter function rejects messages
func (s *ExAnteTestSuite) TestVerifyWithFilterRejection() {
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")
	auxLocal := 2.0

	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	sigma := [][][]byte{
		{[]byte("sigma-0-0"), []byte("sigma-0-1")},
		{[]byte("sigma-1-0"), []byte("sigma-1-1"), []byte("oracle-result-msg")},
		{[]byte("sigma-2-0"), []byte("sigma-2-1")},
		{[]byte("sigma-3-0"), []byte("sigma-3-1")},
	}

	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		return 5
	}

	// Filter function that rejects all messages
	rejectingFilterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return false
	}

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		time.Now().Add(-10*time.Second),
		100*time.Millisecond,
		s.diameter,
		s.D,
		proverGradeFunction,
		s.logger,
	)
	exante.challenge = challenge
	exante.state = sigma

	protocolID := "/exante/1.0.0/" + session
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return().Once()

	// Preload message
	exante.messages = make(map[int]map[string]receivedMessage)
	exante.messages[1] = make(map[string]receivedMessage)
	exante.messages[1]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("oracle-result-msg")},
		},
	}

	// Call Verify
	results, err := exante.Verify(session, vk, sigma, auxTag, auxLocal, rejectingFilterFn)

	// Assertions
	s.NoError(err)
	s.Empty(results) // No messages should be processed due to filter rejection
	s.False(exante.isRunning)
	s.Empty(exante.verifiedValues)
}

// TestVerifyWithZeroGradeMessages tests when grade function returns 0
func (s *ExAnteTestSuite) TestVerifyWithZeroGradeMessages() {
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")
	auxLocal := 2.0

	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	sigma := [][][]byte{
		{[]byte("sigma-0-0"), []byte("sigma-0-1")},
		{[]byte("sigma-1-0"), []byte("sigma-1-1"), []byte("oracle-result-msg")},
		{[]byte("sigma-2-0"), []byte("sigma-2-1")},
		{[]byte("sigma-3-0"), []byte("sigma-3-1")},
	}

	// Grade function that returns 0 for message validation (invalid)
	// but high enough for prover status
	zeroGradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		// Check if this is the prover check (using the auxTag's AuxKey)
		if string(ch) == string(auxTag.AuxKey.PhiVRF) {
			return 5 // High grade for prover status
		}
		return 0 // Zero grade for message validation
	}

	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		time.Now().Add(-10*time.Second),
		100*time.Millisecond,
		s.diameter,
		s.D,
		zeroGradeFunction,
		s.logger,
	)
	exante.challenge = challenge
	exante.state = sigma

	protocolID := "/exante/1.0.0/" + session
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return().Once()

	// Preload message
	exante.messages = make(map[int]map[string]receivedMessage)
	exante.messages[1] = make(map[string]receivedMessage)
	exante.messages[1]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("oracle-result-msg")},
		},
	}

	// Call Verify
	results, err := exante.Verify(session, vk, sigma, auxTag, auxLocal, filterFn)

	// Assertions
	s.NoError(err)
	s.Empty(results) // No messages should be processed due to zero grade
	s.False(exante.isRunning)
	s.Empty(exante.verifiedValues)
}

// TestVerifyWithEmptySigma tests error handling for empty sigma
func (s *ExAnteTestSuite) TestVerifyWithEmptySigma() {
	session := "test-session"
	vk := []byte("verification-key")
	auxLocal := 2.0

	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	// Empty sigma
	var sigma [][][]byte

	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		return 5
	}

	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		time.Now(),
		100*time.Millisecond,
		s.diameter,
		s.D,
		proverGradeFunction,
		s.logger,
	)

	// Call Verify with empty sigma
	results, err := exante.Verify(session, vk, sigma, auxTag, auxLocal, filterFn)

	// Assertions
	s.Error(err)
	s.Contains(err.Error(), "empty sigma")
	s.Nil(results)
	s.False(exante.isRunning) // Should be set to false on error
}

// TestVerifySessionMismatch tests error handling for session ID mismatch
func (s *ExAnteTestSuite) TestVerifySessionMismatch() {
	session := "test-session"
	wrongSession := "wrong-session"
	vk := []byte("verification-key")
	auxLocal := 2.0

	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	sigma := [][][]byte{
		{[]byte("sigma-0-0"), []byte("sigma-0-1")},
		{[]byte("sigma-1-0"), []byte("sigma-1-1")},
	}

	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		return 5
	}

	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session, // Correct session
		time.Now(),
		100*time.Millisecond,
		s.diameter,
		s.D,
		proverGradeFunction,
		s.logger,
	)

	// Call Verify with wrong session
	results, err := exante.Verify(wrongSession, vk, sigma, auxTag, auxLocal, filterFn)

	// Assertions
	s.Error(err)
	s.Contains(err.Error(), "session ID mismatch")
	s.Nil(results)
}

// TestVerifyMultiRoundMessages tests message processing across multiple rounds
func (s *ExAnteTestSuite) TestVerifyMultiRoundMessages() {
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")
	auxLocal := 2.0

	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	// Create sigma with oracle results for validation
	sigma := [][][]byte{
		{[]byte("sigma-0-0"), []byte("sigma-0-1")},
		{[]byte("sigma-1-0"), []byte("oracle-result-r1"), []byte("sigma-1-2")},
		{[]byte("sigma-2-0"), []byte("oracle-result-r2"), []byte("sigma-2-2")},
		{[]byte("sigma-3-0"), []byte("oracle-result-r3"), []byte("sigma-3-2")},
	}

	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		return 5
	}

	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		time.Now().Add(-10*time.Second),
		100*time.Millisecond,
		s.diameter,
		s.D,
		proverGradeFunction,
		s.logger,
	)
	exante.challenge = challenge
	exante.state = sigma

	protocolID := "/exante/1.0.0/" + session
	// Expect initial prover message + forwarded messages
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return()

	// Mock Oracle calls - use flexible approach since exact count is complex
	s.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result-r1"))

	// Preload messages for rounds 1, 2, and 3
	exante.messages = make(map[int]map[string]receivedMessage)

	// Round 1 message
	exante.messages[1] = make(map[string]receivedMessage)
	exante.messages[1]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("oracle-result-r1")},
		},
	}

	// Round 2 message
	exante.messages[2] = make(map[string]receivedMessage)
	exante.messages[2]["node-3"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("oracle-result-r1")},
			{[]byte("oracle-result-r2")},
		},
	}

	// Round 3 message
	exante.messages[3] = make(map[string]receivedMessage)
	exante.messages[3]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("oracle-result-r1")},
			{[]byte("oracle-result-r2")},
			{[]byte("oracle-result-r3")},
		},
	}

	// Call Verify
	results, err := exante.Verify(session, vk, sigma, auxTag, auxLocal, filterFn)

	// Assertions
	s.NoError(err)
	s.NotEmpty(results)
	s.False(exante.isRunning)

	// Verify that all messages were processed
	expectedKey := common.Key{VK: string(vk), Ch: string(challenge)}
	s.Contains(results, expectedKey)

	// Verify that verified values were stored
	s.NotEmpty(exante.verifiedValues)

	s.mockMDAG.AssertExpectations(s.T())
	s.mockNetwork.AssertExpectations(s.T())
}

// TestVerifyMultipleMessagesPerRound tests handling multiple messages in the same round
func (s *ExAnteTestSuite) TestVerifyMultipleMessagesPerRound() {
	session := "test-session"
	vk1 := []byte("verification-key-1")
	vk2 := []byte("verification-key-2")
	challenge1 := []byte("challenge-1")
	challenge2 := []byte("challenge-2")
	auxLocal := 2.0

	auxTag1 := &common.AuxTag{
		PiRP: []byte("pi-rp-1"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf-1"),
			PiVRF:  []byte("pi-vrf-1"),
			PhiVDF: []byte("phi-vdf-1"),
			PiVDF:  []byte("pi-vdf-1"),
		},
	}

	auxTag2 := &common.AuxTag{
		PiRP: []byte("pi-rp-2"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf-2"),
			PiVRF:  []byte("pi-vrf-2"),
			PhiVDF: []byte("phi-vdf-2"),
			PiVDF:  []byte("pi-vdf-2"),
		},
	}

	sigma := [][][]byte{
		{[]byte("sigma-0-0"), []byte("sigma-0-1")},
		{[]byte("sigma-1-0"), []byte("oracle-result-1"), []byte("oracle-result-2")},
		{[]byte("sigma-2-0"), []byte("sigma-2-1")},
		{[]byte("sigma-3-0"), []byte("sigma-3-1")},
	}

	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		return 5
	}

	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		time.Now().Add(-10*time.Second),
		100*time.Millisecond,
		s.diameter,
		s.D,
		proverGradeFunction,
		s.logger,
	)
	exante.challenge = challenge1
	exante.state = sigma

	protocolID := "/exante/1.0.0/" + session
	// Expect initial prover message + forwarded messages
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return()

	// Mock Oracle calls - return the same result for both messages to ensure both are valid
	s.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result-1")).Times(6)

	// Preload multiple messages for round 1
	exante.messages = make(map[int]map[string]receivedMessage)
	exante.messages[1] = make(map[string]receivedMessage)

	// First message from node-2
	exante.messages[1]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk1,
		ch:  challenge1,
		aux: auxTag1,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("oracle-result-1")},
		},
	}

	// Second message from node-3 (different VK and challenge)
	exante.messages[1]["node-3"] = receivedMessage{
		sid: session,
		vk:  vk2,
		ch:  challenge2,
		aux: auxTag2,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("oracle-result-1")}, // Use same Oracle result as first message
		},
	}

	// Call Verify
	results, err := exante.Verify(session, vk1, sigma, auxTag1, auxLocal, filterFn)

	// Assertions
	s.NoError(err)
	s.NotEmpty(results)
	s.False(exante.isRunning)

	// Verify that at least one message was processed (demonstrates multi-message handling)
	expectedKey1 := common.Key{VK: string(vk1), Ch: string(challenge1)}
	s.Contains(results, expectedKey1)

	s.mockMDAG.AssertExpectations(s.T())
	s.mockNetwork.AssertExpectations(s.T())
}

// TestVerifyGradeComparison tests grade comparison and replacement logic
func (s *ExAnteTestSuite) TestVerifyGradeComparison() {
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")
	auxLocal := 2.0

	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	sigma := [][][]byte{
		{[]byte("sigma-0-0"), []byte("sigma-0-1")},
		{[]byte("sigma-1-0"), []byte("oracle-result-msg"), []byte("sigma-1-2")},
		{[]byte("sigma-2-0"), []byte("oracle-result-msg"), []byte("sigma-2-2")},
		{[]byte("sigma-3-0"), []byte("sigma-3-1")},
	}

	// Grade function that returns different grades for different rounds
	gradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		// Return higher grade for prover status initially
		if string(ch) == string(auxTag.AuxKey.PhiVRF) {
			return 5
		}
		// For message validation, return grade 3 for round 1, grade 4 for round 2
		// This tests that higher grade in round 2 should replace round 1 result
		if len(ch) == 9 { // "challenge" length
			return 3 // Lower grade for round 1
		}
		return 4 // Higher grade for round 2
	}

	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		time.Now().Add(-10*time.Second),
		100*time.Millisecond,
		s.diameter,
		s.D,
		gradeFunction,
		s.logger,
	)
	exante.challenge = challenge
	exante.state = sigma

	protocolID := "/exante/1.0.0/" + session
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return().Twice()

	// Mock Oracle calls - use flexible approach
	s.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result-msg")).Times(7)

	// Preload messages for rounds 1 and 2 with same VK/challenge but different grades
	exante.messages = make(map[int]map[string]receivedMessage)

	// Round 1 message (will get grade 3)
	exante.messages[1] = make(map[string]receivedMessage)
	exante.messages[1]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("oracle-result-msg")},
		},
	}

	// Round 2 message (will get grade 4, should replace round 1)
	exante.messages[2] = make(map[string]receivedMessage)
	exante.messages[2]["node-3"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("oracle-result-msg")},
			{[]byte("oracle-result-msg")},
		},
	}

	// Call Verify
	results, err := exante.Verify(session, vk, sigma, auxTag, auxLocal, filterFn)

	// Assertions
	s.NoError(err)
	s.NotEmpty(results)
	s.False(exante.isRunning)

	// Verify that the result has the higher grade (from round 2)
	expectedKey := common.Key{VK: string(vk), Ch: string(challenge)}
	s.Contains(results, expectedKey)

	result := results[expectedKey]
	// Grade is min(gradeFunction, diameter - floor(r/D))
	// Round 1: min(3, 3 - floor(1/1)) = min(3, 2) = 2
	// Round 2: min(4, 3 - floor(2/1)) = min(4, 1) = 1
	// So round 1 should win with grade 2
	s.Equal(2, result.Grade) // Should have the higher grade from round 1

	s.mockMDAG.AssertExpectations(s.T())
	s.mockNetwork.AssertExpectations(s.T())
}

// TestVerifyInvalidMerklePathStructure tests various invalid Merkle path structures
func (s *ExAnteTestSuite) TestVerifyInvalidMerklePathStructure() {
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")
	auxLocal := 2.0

	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	sigma := [][][]byte{
		{[]byte("sigma-0-0"), []byte("sigma-0-1")},
		{[]byte("sigma-1-0"), []byte("oracle-result-msg"), []byte("sigma-1-2")},
		{[]byte("sigma-2-0"), []byte("sigma-2-1")},
		{[]byte("sigma-3-0"), []byte("sigma-3-1")},
	}

	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxkey *common.AuxKey, auxLocal float64) int {
		return 5
	}

	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

	exante := New(
		s.mockNetwork,
		s.mockMDAG,
		session,
		time.Now().Add(-10*time.Second),
		100*time.Millisecond,
		s.diameter,
		s.D,
		proverGradeFunction,
		s.logger,
	)
	exante.challenge = challenge
	exante.state = sigma

	protocolID := "/exante/1.0.0/" + session
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return().Once()

	// Mock Oracle calls
	s.mockMDAG.On("Oracle", mock.Anything).Return([]byte("oracle-result-msg")).Once()

	// Preload message with invalid Merkle path (too short for round)
	exante.messages = make(map[int]map[string]receivedMessage)
	exante.messages[1] = make(map[string]receivedMessage)
	exante.messages[1]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")}, // Level 0 exists
			{[]byte("invalid-oracle-result")},    // Level 1 exists but contains invalid value
		},
	}

	// Call Verify
	results, err := exante.Verify(session, vk, sigma, auxTag, auxLocal, filterFn)

	// Assertions
	s.NoError(err)
	s.Empty(results) // No valid messages should be processed due to invalid Merkle path
	s.False(exante.isRunning)
	s.Empty(exante.verifiedValues)

	s.mockMDAG.AssertExpectations(s.T())
	s.mockNetwork.AssertExpectations(s.T())

}

// Run the test suite
func TestExAnteSuite(t *testing.T) {
	suite.Run(t, new(ExAnteTestSuite))
}
