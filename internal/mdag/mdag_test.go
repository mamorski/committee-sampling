package mdag_test

import (
	"bytes"
	"crypto/sha256"
	"sort"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/mdag"
	"github.com/mamorski/committee-sampling/internal/network"
	mdagpb "github.com/mamorski/committee-sampling/pkg/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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

// Setup helper function to create a new MDAG instance with mocks
func setupMDAG(t *testing.T) (*mdag.MDAG, *MockNetwork, *zap.Logger) {
	mockNetwork := new(MockNetwork)
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

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
	mockNetwork := new(MockNetwork)
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	mockNetwork.On("GetNeighbors").Return([]string{"node1", "node2", "node3"})
	mockNetwork.On("GetNodeID").Return("testNode").Maybe()
	mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return()

	startTime := time.Now().Add(100 * time.Millisecond)
	mdagInstance := mdag.New(3, "test-session", testOracle, mockNetwork, 100*time.Millisecond, logger, startTime)

	assert.NotNil(t, mdagInstance)
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

	var buffer bytes.Buffer
	for _, lab := range sortedL0 {
		buffer.Write(lab)
	}
	currentLabel := testOracle(buffer.Bytes())

	// Process the second level
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
	finalLabel := testOracle(buffer.Bytes())

	// Test with correct target label
	targetLabels := map[string]bool{
		string(finalLabel): true,
	}
	assert.True(t, mdagInstance.Verify(path, targetLabels))

	// Test with incorrect target label
	wrongTargetLabels := map[string]bool{
		"wrong-label": true,
	}
	assert.False(t, mdagInstance.Verify(path, wrongTargetLabels))

	// Test with empty path
	emptyPath := [][][]byte{}
	assert.False(t, mdagInstance.Verify(emptyPath, targetLabels))

	// Test with empty target labels
	emptyTargetLabels := map[string]bool{}
	assert.False(t, mdagInstance.Verify(path, emptyTargetLabels))
}

// TestGenerate tests the Generate function
func TestGenerate(t *testing.T) {
	mdagInstance, mockNetwork, _ := setupMDAG(t)

	// Setup mock for SendProtocolMessage
	mockNetwork.On("SendProtocolMessage", mock.Anything, mock.Anything).Return()

	// Run Generate in a goroutine since it's a long-running function
	go func() {
		state, err := mdagInstance.Generate("test-session", []byte("vki"), []byte("vi"))
		assert.NoError(t, err)
		assert.NotNil(t, state)
	}()

	// Allow some time for the protocol to run
	time.Sleep(500 * time.Millisecond)

	// Verify that SendProtocolMessage was called
	mockNetwork.AssertCalled(t, "SendProtocolMessage", mock.Anything, mock.Anything)
}

// TestGenerateSessionMismatch tests Generate with mismatched session ID
func TestGenerateSessionMismatch(t *testing.T) {
	mdagInstance, _, _ := setupMDAG(t)

	// Call Generate with a different session ID
	state, err := mdagInstance.Generate("different-session", []byte("vki"), []byte("vi"))

	// Verify that an error was returned
	assert.Error(t, err)
	assert.Nil(t, state)
	assert.Contains(t, err.Error(), "session ID mismatch")
}

// TestHandleMessageIntegration tests the handleMessage function
func TestHandleMessageIntegration(t *testing.T) {
	// This test requires access to the handleMessage function, which is not exported.
	// We'll test it indirectly through the network handler.

	mdagInstance, mockNetwork, _ := setupMDAG(t)

	// Get the handler registered with the network
	var handler network.MessageHandler
	for _, call := range mockNetwork.Calls {
		if call.Method == "RegisterHandler" {
			handler = call.Arguments.Get(1).(network.MessageHandler)
			break
		}
	}

	require.NotNil(t, handler, "Failed to get message handler")

	// Add mock expectation for SendProtocolMessage
	mockNetwork.On("SendProtocolMessage", mock.Anything, mock.Anything).Return()

	// Start the protocol
	go func() {
		mdagInstance.Generate("test-session", []byte("vki"), []byte("vi"))
	}()

	// Allow time for the protocol to start
	time.Sleep(200 * time.Millisecond)

	// Create a valid message from a known neighbor
	validMsg := &mdagpb.MDAGMessage{
		SessionId: "test-session",
		Round:     0,
		Label:     []byte("test-label"),
		From:      "node1", // This is in our neighbors list
	}
	validData, err := proto.Marshal(validMsg)
	require.NoError(t, err)

	// Test with valid message
	err = handler("test-protocol", validData)
	assert.NoError(t, err)

	// Create a message from an unknown neighbor
	unknownMsg := &mdagpb.MDAGMessage{
		SessionId: "test-session",
		Round:     0,
		Label:     []byte("test-label"),
		From:      "unknown-node", // Not in our neighbors list
	}
	unknownData, err := proto.Marshal(unknownMsg)
	require.NoError(t, err)

	// Test with unknown neighbor
	err = handler("test-protocol", unknownData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown neighbor")

	// Create a message with mismatched session ID
	mismatchMsg := &mdagpb.MDAGMessage{
		SessionId: "wrong-session",
		Round:     0,
		Label:     []byte("test-label"),
		From:      "node1",
	}
	mismatchData, err := proto.Marshal(mismatchMsg)
	require.NoError(t, err)

	// Test with mismatched session ID
	err = handler("test-protocol", mismatchData)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "session id mismatch")

	// Create invalid message data
	invalidData := []byte("invalid-data")

	// Test with invalid data
	err = handler("test-protocol", invalidData)
	assert.Error(t, err)
}

// TestVerifyWithMultipleTargets tests the Verify function with multiple target labels
func TestVerifyWithMultipleTargets(t *testing.T) {
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

	var buffer bytes.Buffer
	for _, lab := range sortedL0 {
		buffer.Write(lab)
	}
	currentLabel := testOracle(buffer.Bytes())

	// Process the second level
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
	finalLabel := testOracle(buffer.Bytes())

	// Test with multiple target labels, including the correct one
	targetLabels := map[string]bool{
		string(finalLabel): true,
		"wrong-label-1":    true,
		"wrong-label-2":    true,
	}
	assert.True(t, mdagInstance.Verify(path, targetLabels))

	// Test with multiple wrong target labels
	wrongTargetLabels := map[string]bool{
		"wrong-label-1": true,
		"wrong-label-2": true,
		"wrong-label-3": true,
	}
	assert.False(t, mdagInstance.Verify(path, wrongTargetLabels))
}

// TestVerifyManualPath tests the Verify function with a manually constructed path
func TestVerifyManualPath(t *testing.T) {
	mdagInstance, _, _ := setupMDAG(t)

	// Create a more complex path for testing
	path := make([][][]byte, 3)
	path[0] = [][]byte{
		[]byte("label-0-1"),
		[]byte("label-0-2"),
		[]byte("label-0-3"),
	}
	path[1] = [][]byte{
		[]byte("label-1-1"),
		[]byte("label-1-2"),
	}
	path[2] = [][]byte{
		[]byte("label-2-1"),
	}

	// Manually compute the expected final label
	// Level 0
	sortedL0 := make([][]byte, len(path[0]))
	copy(sortedL0, path[0])
	sort.Slice(sortedL0, func(i, j int) bool {
		return bytes.Compare(sortedL0[i], sortedL0[j]) < 0
	})

	var buffer bytes.Buffer
	for _, lab := range sortedL0 {
		buffer.Write(lab)
	}
	label0 := testOracle(buffer.Bytes())

	// Level 1
	labelSet1 := make([][]byte, len(path[1])+1)
	labelSet1[0] = label0
	copy(labelSet1[1:], path[1])
	sort.Slice(labelSet1, func(i, j int) bool {
		return bytes.Compare(labelSet1[i], labelSet1[j]) < 0
	})

	buffer.Reset()
	for _, lab := range labelSet1 {
		buffer.Write(lab)
	}
	label1 := testOracle(buffer.Bytes())

	// Level 2
	labelSet2 := make([][]byte, len(path[2])+1)
	labelSet2[0] = label1
	copy(labelSet2[1:], path[2])
	sort.Slice(labelSet2, func(i, j int) bool {
		return bytes.Compare(labelSet2[i], labelSet2[j]) < 0
	})

	buffer.Reset()
	for _, lab := range labelSet2 {
		buffer.Write(lab)
	}
	finalLabel := testOracle(buffer.Bytes())

	// Test with the correct target label
	targetLabels := map[string]bool{
		string(finalLabel): true,
	}
	assert.True(t, mdagInstance.Verify(path, targetLabels))

	// Modify one label in the path and verify that it fails
	modifiedPath := make([][][]byte, len(path))
	for i := range path {
		modifiedPath[i] = make([][]byte, len(path[i]))
		for j := range path[i] {
			modifiedPath[i][j] = make([]byte, len(path[i][j]))
			copy(modifiedPath[i][j], path[i][j])
		}
	}
	modifiedPath[1][0] = []byte("modified-label")

	assert.False(t, mdagInstance.Verify(modifiedPath, targetLabels))
}

// TestGetComputedLabel tests the GetComputedLabel function
func TestGetComputedLabel(t *testing.T) {
	mdagInstance, mockNetwork, _ := setupMDAG(t)

	// Setup mock for SendProtocolMessage
	mockNetwork.On("SendProtocolMessage", mock.Anything, mock.Anything).Return()

	_, err := mdagInstance.Generate("test-session", []byte("vki"), []byte("vi"))
	assert.NoError(t, err)

	// Allow time for the protocol to run
	time.Sleep(500 * time.Millisecond)

	// Test getting a valid label
	label := mdagInstance.GetComputedLabel(0)
	assert.NotNil(t, label)

	// Test getting an invalid label (out of range)
	invalidLabel := mdagInstance.GetComputedLabel(10)
	assert.Nil(t, invalidLabel)

	// Test getting a negative index
	negativeLabel := mdagInstance.GetComputedLabel(-1)
	assert.Nil(t, negativeLabel)
}
