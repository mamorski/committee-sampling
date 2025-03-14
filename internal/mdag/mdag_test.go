package mdag_test

import (
	"bytes"
	"crypto/sha256"
	"sort"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/mdag"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
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

// Setup helper function to create a new MDAG instance with mocks
func setupMDAG(t *testing.T) (*mdag.MDAG, *MockNetwork, *zap.Logger) {
	mockNetwork := new(MockNetwork)
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// Setup mock expectations
	mockNetwork.On("GetNeighbors").Return([]string{"node1", "node2", "node3"})
	mockNetwork.On("GetNodeID").Return("testNode")
	mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return()

	startTime := time.Now().Add(100 * time.Millisecond)
	mdagInstance := mdag.New(3, "test-session", testOracle, mockNetwork, 100*time.Millisecond, logger, startTime)
	require.NotNil(t, mdagInstance)

	return mdagInstance, mockNetwork, logger
}

// TestNew tests the New function
func TestNew(t *testing.T) {
	// Setup
	mockNetwork := new(MockNetwork)
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// Setup mock expectations
	mockNetwork.On("GetNeighbors").Return([]string{"node1", "node2", "node3"})
	mockNetwork.On("GetNodeID").Return("testNode").Maybe() // Make this optional
	mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return()

	startTime := time.Now().Add(100 * time.Millisecond)
	mdagInstance := mdag.New(3, "test-session", testOracle, mockNetwork, 100*time.Millisecond, logger, startTime)

	// Verify the MDAG instance was created correctly
	// We can't directly access private fields, but we can test behavior
	assert.NotNil(t, mdagInstance)

	// Verify the mock was called as expected
	mockNetwork.AssertCalled(t, "GetNeighbors")
	mockNetwork.AssertCalled(t, "RegisterHandler", mock.Anything, mock.Anything)
}

// TestVerify tests the Verify function
func TestVerify(t *testing.T) {
	mdagInstance, _, _ := setupMDAG(t)

	// Create a simple path for testing
	path := make([][][]byte, 2)
	path[0] = [][]byte{[]byte("label1"), []byte("label2")}
	path[1] = [][]byte{[]byte("label3"), []byte("label4")}

	// Sort and hash the first level
	sortedL0 := make([][]byte, len(path[0]))
	copy(sortedL0, path[0])
	sort.Slice(sortedL0, func(i, j int) bool {
		return bytes.Compare(sortedL0[i], sortedL0[j]) < 0
	})
	var concatenated []byte
	for _, lab := range sortedL0 {
		concatenated = append(concatenated, lab...)
	}
	m0 := testOracle(concatenated)

	// Sort and hash the second level
	labelSet := make([][]byte, len(path[1])+1)
	labelSet[0] = m0
	copy(labelSet[1:], path[1])
	sort.Slice(labelSet, func(i, j int) bool {
		return bytes.Compare(labelSet[i], labelSet[j]) < 0
	})
	concatenated = nil
	for _, lab := range labelSet {
		concatenated = append(concatenated, lab...)
	}
	expectedLabel := testOracle(concatenated)

	// Create target labels map
	targetLabels := map[string]bool{
		string(expectedLabel): true,
	}

	// Test verification with correct target
	result := mdagInstance.Verify(path, targetLabels)
	assert.True(t, result)

	// Test verification with incorrect target
	wrongTargetLabels := map[string]bool{
		string([]byte("wrong-label")): true,
	}
	result = mdagInstance.Verify(path, wrongTargetLabels)
	assert.False(t, result)

	// Test with empty path
	result = mdagInstance.Verify([][][]byte{}, targetLabels)
	assert.False(t, result)

	// Test with empty target labels
	result = mdagInstance.Verify(path, map[string]bool{})
	assert.False(t, result)
}

// TestGenerate tests the Generate function
func TestGenerate(t *testing.T) {
	mdagInstance, mockNetwork, _ := setupMDAG(t)

	// Setup mock expectations for SendProtocolMessage
	mockNetwork.On("SendProtocolMessage", mock.Anything, mock.Anything).Return()

	// Call Generate with a short timeout to make the test run quickly
	// We need to use reflection or exported methods to set these values
	// For this test, we'll just use a short timeout in the setup

	// Create test inputs
	vki := []byte("verification-key")
	vi := [][]byte{[]byte("input1"), []byte("input2")}

	// Run Generate in a goroutine since it blocks
	resultCh := make(chan struct {
		state [][][]byte
		err   error
	})
	go func() {
		state, err := mdagInstance.Generate("test-session", vki, vi...)
		resultCh <- struct {
			state [][][]byte
			err   error
		}{state, err}
	}()

	// Wait for all rounds to complete
	select {
	case result := <-resultCh:
		// Verify the results
		assert.NoError(t, result.err)
		assert.Equal(t, 3, len(result.state))
	case <-time.After(2 * time.Second):
		t.Fatal("Generate timed out")
	}

	// Verify that SendProtocolMessage was called at least once
	mockNetwork.AssertCalled(t, "SendProtocolMessage", mock.Anything, mock.Anything)
	mockNetwork.AssertExpectations(t)
}

// TestGenerateSessionMismatch tests Generate with mismatched session ID
func TestGenerateSessionMismatch(t *testing.T) {
	// Setup
	mockNetwork := new(MockNetwork)
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// Setup mock expectations
	mockNetwork.On("GetNeighbors").Return([]string{"node1", "node2", "node3"})
	mockNetwork.On("GetNodeID").Return("testNode").Maybe() // Make this optional
	mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return()

	startTime := time.Now().Add(100 * time.Millisecond)
	mdagInstance := mdag.New(3, "test-session", testOracle, mockNetwork, 100*time.Millisecond, logger, startTime)

	// Call Generate with wrong session ID
	state, err := mdagInstance.Generate("wrong-session", []byte("vki"))
	assert.Error(t, err)
	assert.Nil(t, state)
	assert.Contains(t, err.Error(), "session ID does not match")
}

// TestHandleMessageIntegration tests the message handling functionality
func TestHandleMessageIntegration(t *testing.T) {
	// This is an integration test that simulates the message handling process
	// We'll create a real MDAG instance and test its behavior with simulated messages

	// Create a network with multiple nodes
	network1 := new(MockNetwork)
	network2 := new(MockNetwork)

	// Setup expectations
	network1.On("GetNeighbors").Return([]string{"node2"})
	network1.On("GetNodeID").Return("node1")
	network1.On("RegisterHandler", mock.Anything, mock.Anything).Return()
	network1.On("SendProtocolMessage", mock.Anything, mock.Anything).Return()

	network2.On("GetNeighbors").Return([]string{"node1"})
	network2.On("GetNodeID").Return("node2")
	network2.On("RegisterHandler", mock.Anything, mock.Anything).Return()
	network2.On("SendProtocolMessage", mock.Anything, mock.Anything).Return()

	// Create loggers
	logger1, _ := zap.NewDevelopment()
	logger2, _ := zap.NewDevelopment()

	// Create MDAG instances
	startTime := time.Now().Add(100 * time.Millisecond)
	mdag1 := mdag.New(2, "test-integration", testOracle, network1, 100*time.Millisecond, logger1, startTime)
	mdag2 := mdag.New(2, "test-integration", testOracle, network2, 100*time.Millisecond, logger2, startTime)

	// Capture the handler registered with the network
	var handler1 network.MessageHandler
	var handler2 network.MessageHandler

	for _, call := range network1.Calls {
		if call.Method == "RegisterHandler" {
			handler1 = call.Arguments.Get(1).(network.MessageHandler)
		}
	}

	for _, call := range network2.Calls {
		if call.Method == "RegisterHandler" {
			handler2 = call.Arguments.Get(1).(network.MessageHandler)
		}
	}

	require.NotNil(t, handler1)
	require.NotNil(t, handler2)

	// Start the protocol on both nodes
	go func() {
		mdag1.Generate("test-integration", []byte("vki1"), []byte("vi1"))
	}()

	go func() {
		mdag2.Generate("test-integration", []byte("vki2"), []byte("vi2"))
	}()

	// Wait for the protocol to start
	time.Sleep(200 * time.Millisecond)

	// Verify that messages were sent
	network1.AssertCalled(t, "SendProtocolMessage", mock.Anything, mock.Anything)
	network2.AssertCalled(t, "SendProtocolMessage", mock.Anything, mock.Anything)

	// Get the messages that were sent
	var message1 []byte
	var message2 []byte

	for _, call := range network1.Calls {
		if call.Method == "SendProtocolMessage" {
			message1 = call.Arguments.Get(1).([]byte)
		}
	}

	for _, call := range network2.Calls {
		if call.Method == "SendProtocolMessage" {
			message2 = call.Arguments.Get(1).([]byte)
		}
	}

	require.NotNil(t, message1)
	require.NotNil(t, message2)

	// Simulate message exchange
	// Node 1 receives message from Node 2
	err := handler1("node2", message2)
	assert.NoError(t, err)

	// Node 2 receives message from Node 1
	err = handler2("node1", message1)
	assert.NoError(t, err)

	// Wait for the protocol to complete
	time.Sleep(500 * time.Millisecond)

	// Verify expectations
	network1.AssertExpectations(t)
	network2.AssertExpectations(t)
}

// TestVerifyWithMultipleTargets tests the Verify function with multiple target labels
func TestVerifyWithMultipleTargets(t *testing.T) {
	mdagInstance, _, _ := setupMDAG(t)

	// Create a simple path for testing
	path := make([][][]byte, 2)
	path[0] = [][]byte{[]byte("label1"), []byte("label2")}
	path[1] = [][]byte{[]byte("label3"), []byte("label4")}

	// Compute the expected label
	var buffer bytes.Buffer
	buffer.Grow(1024)

	// Sort and hash the first level
	sortedL0 := make([][]byte, len(path[0]))
	copy(sortedL0, path[0])
	sort.Slice(sortedL0, func(i, j int) bool {
		return bytes.Compare(sortedL0[i], sortedL0[j]) < 0
	})
	buffer.Reset()
	for _, lab := range sortedL0 {
		buffer.Write(lab)
	}
	currentLabel := testOracle(buffer.Bytes())

	// Sort and hash the second level
	labelSet := make([][]byte, len(path[1])+1)
	labelSet[0] = currentLabel
	copy(labelSet[1:], path[1])
	sort.Slice(labelSet, func(i, j int) bool {
		return bytes.Compare(labelSet[i], labelSet[j]) < 0
	})
	buffer.Reset()
	for _, lab := range labelSet {
		buffer.Write(lab)
	}
	expectedLabel := testOracle(buffer.Bytes())

	// Create target labels map with multiple labels
	targetLabels := map[string]bool{
		string(expectedLabel):         true,
		string([]byte("wrong-label")): true,
	}

	// Test verification with multiple targets (one correct)
	result := mdagInstance.Verify(path, targetLabels)
	assert.True(t, result)

	// Create target labels map with only incorrect labels
	wrongTargetLabels := map[string]bool{
		string([]byte("wrong-label1")): true,
		string([]byte("wrong-label2")): true,
	}

	// Test verification with multiple incorrect targets
	result = mdagInstance.Verify(path, wrongTargetLabels)
	assert.False(t, result)
}

// TestVerifyManualPath tests the Verify function with a manually constructed path
func TestVerifyManualPath(t *testing.T) {
	// Setup
	mockNetwork := new(MockNetwork)
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// Setup mock expectations
	mockNetwork.On("GetNeighbors").Return([]string{"node1", "node2", "node3"})
	mockNetwork.On("GetNodeID").Return("testNode").Maybe()
	mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return()

	startTime := time.Now().Add(100 * time.Millisecond)
	mdagInstance := mdag.New(3, "test-session", testOracle, mockNetwork, 100*time.Millisecond, logger, startTime)

	// Manually construct a path
	// A path is a sequence of label sets, one for each round
	path := make([][][]byte, 3)

	// Round 0: Initial labels
	path[0] = [][]byte{
		[]byte("initial-label-1"),
		[]byte("initial-label-2"),
	}

	// Round 1: Labels computed from round 0
	path[1] = [][]byte{
		[]byte("round-1-label-1"),
		[]byte("round-1-label-2"),
	}

	// Round 2: Labels computed from round 1
	path[2] = [][]byte{
		[]byte("round-2-label-1"),
		[]byte("round-2-label-2"),
	}

	// Compute the expected final label by following the same algorithm as the Verify function

	// Sort and hash round 0
	sortedR0 := make([][]byte, len(path[0]))
	copy(sortedR0, path[0])
	sort.Slice(sortedR0, func(i, j int) bool {
		return bytes.Compare(sortedR0[i], sortedR0[j]) < 0
	})
	var concatenated []byte
	for _, lab := range sortedR0 {
		concatenated = append(concatenated, lab...)
	}
	r0Label := testOracle(concatenated)

	// Sort and hash round 1 with r0Label
	labelSet1 := make([][]byte, len(path[1])+1)
	labelSet1[0] = r0Label
	copy(labelSet1[1:], path[1])
	sort.Slice(labelSet1, func(i, j int) bool {
		return bytes.Compare(labelSet1[i], labelSet1[j]) < 0
	})
	concatenated = nil
	for _, lab := range labelSet1 {
		concatenated = append(concatenated, lab...)
	}
	r1Label := testOracle(concatenated)

	// Sort and hash round 2 with r1Label
	labelSet2 := make([][]byte, len(path[2])+1)
	labelSet2[0] = r1Label
	copy(labelSet2[1:], path[2])
	sort.Slice(labelSet2, func(i, j int) bool {
		return bytes.Compare(labelSet2[i], labelSet2[j]) < 0
	})
	concatenated = nil
	for _, lab := range labelSet2 {
		concatenated = append(concatenated, lab...)
	}
	expectedLabel := testOracle(concatenated)

	// Create target labels map with the expected label
	targetLabels := map[string]bool{
		string(expectedLabel): true,
	}

	// Test verification with the correct target
	result := mdagInstance.Verify(path, targetLabels)
	assert.True(t, result, "Verification failed with correct target label")

	// Test verification with an incorrect target
	wrongTargetLabels := map[string]bool{
		string([]byte("wrong-label")): true,
	}
	result = mdagInstance.Verify(path, wrongTargetLabels)
	assert.False(t, result, "Verification succeeded with incorrect target label")
}

// TestGetComputedLabel tests the GetComputedLabel function
func TestGetComputedLabel(t *testing.T) {
	mdagInstance, mockNetwork, _ := setupMDAG(t)

	// Setup mock expectations for SendProtocolMessage
	mockNetwork.On("SendProtocolMessage", mock.Anything, mock.Anything).Return()

	// Create test inputs
	vki := []byte("verification-key")
	vi := [][]byte{[]byte("input1"), []byte("input2")}

	// Run Generate in a goroutine since it blocks
	resultCh := make(chan struct {
		state [][][]byte
		err   error
	})
	go func() {
		state, err := mdagInstance.Generate("test-session", vki, vi...)
		resultCh <- struct {
			state [][][]byte
			err   error
		}{state, err}
	}()

	// Wait for all rounds to complete
	select {
	case result := <-resultCh:
		// Verify the results
		assert.NoError(t, result.err)
		assert.Equal(t, 3, len(result.state))

		// Test GetComputedLabel for each round
		for i := 0; i < 3; i++ {
			label := mdagInstance.GetComputedLabel(i)
			assert.NotNil(t, label, "Label for round %d should not be nil", i)

			// For round 0, the label should be the initial label
			if i == 0 {
				// The initial label is stored in messages[0][0]
				// We can't directly access it, but we can verify it's not nil
				assert.NotNil(t, label, "Initial label should not be nil")
			} else {
				// For rounds 1 to 3, the label should be the computed label
				assert.NotNil(t, label, "Computed label for round %d should not be nil", i)
			}
		}

		// Test invalid round indices
		assert.Nil(t, mdagInstance.GetComputedLabel(-1), "Label for negative round index should be nil")
		assert.Nil(t, mdagInstance.GetComputedLabel(3), "Label for round index >= rounds should be nil")
	case <-time.After(2 * time.Second):
		t.Fatal("Generate timed out")
	}

	// Verify that SendProtocolMessage was called at least once
	mockNetwork.AssertCalled(t, "SendProtocolMessage", mock.Anything, mock.Anything)
	mockNetwork.AssertExpectations(t)
}
