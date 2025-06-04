package mdag

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/network"
	mdagpb "github.com/mamorski/committee-sampling/pkg/proto"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// MockNetwork is a mock implementation of the network.Network interface
type MockNetwork struct {
	mock.Mock
}

func (m *MockNetwork) RegisterHandler(protocolID string, handler network.MessageHandler) {
	m.Called(protocolID, handler)
}

func (m *MockNetwork) SendProtocolMessage(protocolID string, data []byte) {
	m.Called(protocolID, data)
}

func (m *MockNetwork) GetNeighbors() []string {
	args := m.Called()
	return args.Get(0).([]string)
}

func (m *MockNetwork) GetNodeID() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockNetwork) Close() error {
	args := m.Called()
	return args.Error(0)
}

// Simple hash oracle for testing
func testOracle(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

// MDAGTestSuite defines the test suite for MDAG
type MDAGTestSuite struct {
	suite.Suite
	mockNetwork *MockNetwork
	logger      *zap.Logger
	mdag        *MDAG
}

// SetupTest runs before each test
func (suite *MDAGTestSuite) SetupTest() {
	suite.mockNetwork = new(MockNetwork)
	var err error
	suite.logger, err = zap.NewDevelopment()
	suite.Require().NoError(err)
}

// TearDownTest runs after each test
func (suite *MDAGTestSuite) TearDownTest() {
	if suite.mockNetwork != nil {
		suite.mockNetwork.AssertExpectations(suite.T())
	}
}

// setupMDAG creates a new MDAG instance with mocks
func (suite *MDAGTestSuite) setupMDAG() {
	suite.mockNetwork.On("GetNeighbors").Return([]string{"node1", "node2", "node3"}).Once()
	suite.mockNetwork.On("GetNodeID").Return("testNode").Maybe()
	suite.mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return().Once()

	startTime := time.Now().Add(100 * time.Millisecond)
	suite.mdag = New(3, "test-session", testOracle, suite.mockNetwork, 100*time.Millisecond, suite.logger, startTime, "")
	suite.Require().NotNil(suite.mdag)
}

// TestNew tests the New function
func (suite *MDAGTestSuite) TestNew() {
	suite.mockNetwork.On("GetNeighbors").Return([]string{"node1", "node2", "node3"}).Once()
	suite.mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return().Once()

	startTime := time.Now().Add(100 * time.Millisecond)
	mdagInstance := New(3, "test-session", testOracle, suite.mockNetwork, 100*time.Millisecond, suite.logger, startTime, "")

	suite.NotNil(mdagInstance)
}

// TestOracle tests the Oracle function
func (suite *MDAGTestSuite) TestOracle() {
	suite.setupMDAG()

	// Test with single input
	input1 := []byte("test-input")
	result1 := suite.mdag.Oracle(input1)
	suite.NotNil(result1)
	suite.Equal(32, len(result1)) // SHA256 produces 32 bytes

	// Test with multiple inputs
	input2 := []byte("input1")
	input3 := []byte("input2")
	result2 := suite.mdag.Oracle(input2, input3)
	suite.NotNil(result2)
	suite.Equal(32, len(result2))

	// Test that same inputs produce same output
	result3 := suite.mdag.Oracle(input1)
	suite.Equal(result1, result3)

	// Test that different inputs produce different outputs
	suite.NotEqual(result1, result2)

	// Test with empty input
	result4 := suite.mdag.Oracle()
	suite.NotNil(result4)
	suite.Equal(32, len(result4))

	// Test with nil input
	result5 := suite.mdag.Oracle(nil)
	suite.NotNil(result5)
	suite.Equal(32, len(result5))
}

// TestGenerate tests the Generate function
func (suite *MDAGTestSuite) TestGenerate() {
	suite.setupMDAG()
	suite.mockNetwork.On("SendProtocolMessage", mock.Anything, mock.Anything).Return().Times(3) // 3 rounds of broadcasting

	// Run Generate in a goroutine since it's a long-running function
	done := make(chan bool)
	var state [][][]byte
	var err error

	go func() {
		state, err = suite.mdag.Generate("test-session", []byte("vki"), []byte("vi"))
		done <- true
	}()

	// Wait for completion
	select {
	case <-done:
		suite.NoError(err)
		suite.NotNil(state)
	case <-time.After(2 * time.Second):
		suite.Fail("Protocol timed out")
	}
}

// TestGenerateSessionMismatch tests Generate with mismatched session ID
func (suite *MDAGTestSuite) TestGenerateSessionMismatch() {
	suite.setupMDAG()

	// Call Generate with a different session ID
	state, err := suite.mdag.Generate("different-session", []byte("vki"), []byte("vi"))

	// Verify that an error was returned
	suite.Error(err)
	suite.Nil(state)
	suite.Contains(err.Error(), "session ID mismatch")
}

// TestGenerateAlreadyRunning tests Generate when protocol is already running
func (suite *MDAGTestSuite) TestGenerateAlreadyRunning() {
	suite.setupMDAG()

	// Manually set the running flag to simulate protocol already running
	suite.mdag.isRunning = true

	// Try to start Generate
	state, err := suite.mdag.Generate("test-session", []byte("vki"), []byte("vi"))

	suite.Error(err)
	suite.Nil(state)
	suite.Contains(err.Error(), "protocol is already running")
}

// TestHandleMessageNotRunning tests handleMessage when protocol is not running
func (suite *MDAGTestSuite) TestHandleMessageNotRunning() {
	suite.setupMDAG()

	// Get the handler registered with the network
	var handler network.MessageHandler
	for _, call := range suite.mockNetwork.Calls {
		if call.Method == "RegisterHandler" {
			handler = call.Arguments.Get(1).(network.MessageHandler)
			break
		}
	}
	suite.Require().NotNil(handler, "Failed to get message handler")

	// Create a valid message
	validMsg := &mdagpb.MDAGMessage{
		SessionId: "test-session",
		Round:     0,
		Label:     []byte("test-label"),
		Id:        "node1",
	}
	validData, err := proto.Marshal(validMsg)
	suite.Require().NoError(err)

	// Test with protocol not running
	err = handler("node1", validData)
	suite.Error(err)
	suite.Contains(err.Error(), "protocol not running")
}

// TestHandleMessageIntegration tests the handleMessage function during protocol execution
func (suite *MDAGTestSuite) TestHandleMessageIntegration() {
	suite.setupMDAG()
	// The protocol will complete, so expect all 3 calls
	suite.mockNetwork.On("SendProtocolMessage", mock.Anything, mock.Anything).Return().Times(3)

	// Get the handler registered with the network
	var handler network.MessageHandler
	for _, call := range suite.mockNetwork.Calls {
		if call.Method == "RegisterHandler" {
			handler = call.Arguments.Get(1).(network.MessageHandler)
			break
		}
	}
	suite.Require().NotNil(handler, "Failed to get message handler")

	// Start the protocol
	done := make(chan bool)
	go func() {
		_, _ = suite.mdag.Generate("test-session", []byte("vki"), []byte("vi"))
		done <- true
	}()

	// Allow time for the protocol to start
	time.Sleep(200 * time.Millisecond)

	// Create a valid message from a known neighbor
	validMsg := &mdagpb.MDAGMessage{
		SessionId: "test-session",
		Round:     0,
		Label:     []byte("test-label"),
		Id:        "node1",
	}
	validData, err := proto.Marshal(validMsg)
	suite.Require().NoError(err)

	// Test with valid message
	err = handler("node1", validData)
	suite.NoError(err)

	// Create a message from an unknown neighbor
	unknownMsg := &mdagpb.MDAGMessage{
		SessionId: "test-session",
		Round:     0,
		Label:     []byte("test-label"),
		Id:        "unknown-node",
	}
	unknownData, err := proto.Marshal(unknownMsg)
	suite.Require().NoError(err)

	// Test with unknown neighbor
	err = handler("unknown-node", unknownData)
	suite.Error(err)
	suite.Contains(err.Error(), "unknown neighbor")

	// Create a message with mismatched session ID
	mismatchMsg := &mdagpb.MDAGMessage{
		SessionId: "wrong-session",
		Round:     0,
		Label:     []byte("test-label"),
		Id:        "node1",
	}
	mismatchData, err := proto.Marshal(mismatchMsg)
	suite.Require().NoError(err)

	// Test with mismatched session ID
	err = handler("node1", mismatchData)
	suite.Error(err)
	suite.Contains(err.Error(), "session id mismatch")

	// Create invalid message data
	invalidData := []byte("invalid-data")

	// Test with invalid data
	err = handler("node1", invalidData)
	suite.Error(err)

	// Wait for the protocol to complete
	select {
	case <-done:
		// Protocol completed
	case <-time.After(2 * time.Second):
		suite.Fail("Protocol timed out")
	}
}

// TestGetComputedLabel tests the GetComputedLabel function
func (suite *MDAGTestSuite) TestGetComputedLabel() {
	suite.setupMDAG()
	suite.mockNetwork.On("SendProtocolMessage", mock.Anything, mock.Anything).Return().Times(3)

	_, err := suite.mdag.Generate("test-session", []byte("vki"), []byte("vi"))
	suite.NoError(err)

	// Allow time for the protocol to run
	time.Sleep(500 * time.Millisecond)

	// Test getting a valid label
	label := suite.mdag.GetComputedLabel(0)
	suite.NotNil(label)

	// Test getting an invalid label (out of range)
	invalidLabel := suite.mdag.GetComputedLabel(10)
	suite.Nil(invalidLabel)

	// Test getting a negative index
	negativeLabel := suite.mdag.GetComputedLabel(-1)
	suite.Nil(negativeLabel)
}

// TestGetComputedLabelNotRunning tests GetComputedLabel when protocol hasn't run
func (suite *MDAGTestSuite) TestGetComputedLabelNotRunning() {
	suite.setupMDAG()

	// Test getting labels before protocol has run
	label := suite.mdag.GetComputedLabel(0)
	suite.Nil(label)

	label = suite.mdag.GetComputedLabel(1)
	suite.Nil(label)
}

// Run the test suite
func TestMDAGTestSuite(t *testing.T) {
	suite.Run(t, new(MDAGTestSuite))
}
