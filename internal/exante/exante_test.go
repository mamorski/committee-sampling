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

func (m *MockMDAG) Oracle(h ...[]byte) []byte {
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
	s.mockNetwork.On("Close").Return(nil) // Add default behavior for Close method
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

// TestVerify tests the Verify method
func (s *ExAnteTestSuite) TestVerify() {
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")

	// Setup auxTag
	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	// Mock sigma
	sigma := [][][]byte{
		{
			[]byte("sigma-0-0"), []byte("sigma-0-1"),
		},
		{
			[]byte("sigma-1-0"), []byte("sigma-1-1"),
		},
		{
			[]byte("sigma-2-0"), []byte("sigma-2-1"),
		},
		{
			[]byte("sigma-3-0"), []byte("sigma-3-1"),
		},
	}

	// Configure mocks
	hashResult := []byte("hash-result")
	s.mockMDAG.On("Oracle", mock.Anything).Return(hashResult)

	// Configure network mock to allow filter function to always pass
	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

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
	exante.challenge = challenge
	exante.state = sigma

	// Mock the network to expect protocol message
	protocolID := "/exante/1.0.0/" + session
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return() // Remove error return

	// Adjust start time to ensure rounds complete immediately for testing
	exante.startTime = time.Now().Add(-10 * time.Second)

	// Call Verify
	results, err := exante.Verify(session, vk, sigma, auxTag, 1.0, filterFn)

	s.NoError(err)
	s.Empty(results)          // No messages were processed in this test setup
	s.False(exante.isRunning) // Should finish running after verify
}

// TestVerifyWithMessages tests the Verify method with pre-loaded messages
func (s *ExAnteTestSuite) TestVerifyWithMessages() {
	session := "test-session"
	vk := []byte("verification-key")
	challenge := []byte("challenge")

	// Setup auxTag
	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	// Mock sigma
	sigma := [][][]byte{
		{
			[]byte("sigma-0-0"), []byte("sigma-0-1"),
		},
		{
			[]byte("sigma-1-0"), []byte("sigma-1-1"),
		},
		{
			[]byte("sigma-2-0"), []byte("sigma-2-1"),
		},
		{
			[]byte("sigma-3-0"), []byte("sigma-3-1"),
		},
	}

	// Configure mocks
	hashResult := []byte("hash-result")
	s.mockMDAG.On("Oracle", mock.Anything).Return(hashResult)

	// Configure network mock to expect protocol messages
	protocolID := "/exante/1.0.0/" + session
	s.mockNetwork.On("SendProtocolMessage", protocolID, mock.Anything).Return() // Remove error return

	// Filter function that always passes
	filterFn := func(sid string, vk []byte, challenge []byte, auxTag *common.AuxTag) bool {
		return true
	}

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
	exante.challenge = challenge
	exante.state = sigma

	// Preload a message for round 1
	exante.messages = make(map[int]map[string]receivedMessage)
	exante.messages[1] = make(map[string]receivedMessage)
	exante.messages[1]["node-2"] = receivedMessage{
		sid: session,
		vk:  vk,
		ch:  challenge,
		aux: auxTag,
		merklePath: [][][]byte{
			{[]byte("mp-0-0"), []byte("mp-0-1")},
			{[]byte("mp-1-0"), []byte("mp-1-1")},
		},
	}

	// Setup MDAG to validate the message
	s.mockMDAG.On("Oracle", [][]byte{[]byte("mp-0-0"), []byte("mp-0-1")}).Return([]byte("hash-1"))
	s.mockMDAG.On("Oracle", []byte(session), vk, challenge, auxTag.PiRP).Return([]byte("mp-1-0"))

	// Adjust start time to ensure rounds complete immediately for testing
	exante.startTime = time.Now().Add(-10 * time.Second)

	// Call Verify
	results, err := exante.Verify(session, vk, sigma, auxTag, 1.0, filterFn)

	s.NoError(err)
	s.NotEmpty(results)       // We should have processed the preloaded message
	s.False(exante.isRunning) // Should finish running after verify
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

// Run the test suite
func TestExAnteSuite(t *testing.T) {
	suite.Run(t, new(ExAnteTestSuite))
}
