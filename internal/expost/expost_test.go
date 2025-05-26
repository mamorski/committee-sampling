package expost

import (
	"sync"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/network"
	pb "github.com/mamorski/committee-sampling/pkg/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

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

	neighbors := map[string]bool{
		"node1": true,
		"node2": true,
		"node3": true,
	}

	suite.expost = &ExPost{
		network:      suite.mockNetwork,
		logger:       suite.logger.Named("expost"),
		mdag:         suite.mockMDAG,
		roundTimeout: 100 * time.Millisecond,
		startTime:    suite.startTime,
		d:            3,
		D:            5,
		lambda:       32,
		gradeFunc:    mockGradeFunc,
		filterFunc:   mockFilterTagFunc,
		isRunning:    true,
		sid:          suite.sid,
		vk:           suite.vk,
		mu:           sync.Mutex{},
		messages:     make(map[int]map[string]receivedMessage),
		neighbors:    neighbors,
	}
}

func (suite *ExPostTestSuite) TearDownTest() {
	// suite.T().Logf("Total network expectations: %d", len(suite.mockNetwork.ExpectedCalls))
	// for i, call := range suite.mockNetwork.ExpectedCalls {
	// 	suite.T().Logf("Expectation %d: %s", i, call.Method)
	// }
	// suite.T().Logf("Total mockNetwork calls: %d", len(suite.mockNetwork.Calls))
	suite.mockNetwork.AssertExpectations(suite.T())
	suite.mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestNew() {
	// Test parameters
	testSid := "test-new-session"
	testVk := []byte("test-new-vk")
	testStartTime := time.Now().Add(time.Hour)
	testRoundTimeout := 200 * time.Millisecond
	testDiameter := 4
	testD := 6
	testLambda := 64
	testGradeFunc := mockGradeFunc
	testFilterFunc := mockFilterTagFunc
	testLogger := zap.NewNop()

	// Setup mock expectations
	testNeighbors := []string{"neighbor1", "neighbor2", "neighbor3", "neighbor4"}
	mockNet := new(MockNetwork)
	mockMDAG := new(MockMDAG)

	mockNet.On("GetNeighbors").Return(testNeighbors).Once()
	mockNet.On("RegisterHandler", "/expost/1.0.0/test-new-session", mock.AnythingOfType("network.MessageHandler")).Once()

	// Call the New function
	expost := New(
		mockNet,
		mockMDAG,
		testSid,
		testVk,
		testStartTime,
		testRoundTimeout,
		testDiameter,
		testD,
		testLambda,
		testGradeFunc,
		testFilterFunc,
		testLogger,
	)

	// Verify the instance is created correctly
	suite.NotNil(expost)

	// Verify all fields are set correctly
	suite.Equal(mockNet, expost.network)
	suite.Equal(mockMDAG, expost.mdag)
	suite.Equal(testSid, expost.sid)
	suite.Equal(testVk, expost.vk)
	suite.Equal(testStartTime, expost.startTime)
	suite.Equal(testRoundTimeout, expost.roundTimeout)
	suite.Equal(testDiameter, expost.d)
	suite.Equal(testD, expost.D)
	suite.Equal(testLambda, expost.lambda)
	suite.True(expost.isRunning)

	// Verify function pointers are set
	suite.NotNil(expost.gradeFunc)
	suite.NotNil(expost.filterFunc)

	// Verify logger is named correctly
	suite.NotNil(expost.logger)

	// Verify maps are initialized
	suite.NotNil(expost.messages)
	suite.NotNil(expost.neighbors)
	suite.Len(expost.messages, 0) // Should be empty initially

	// Verify neighbors are populated correctly
	suite.Len(expost.neighbors, 4)
	for _, neighbor := range testNeighbors {
		suite.True(expost.neighbors[neighbor])
	}

	// Verify initial state
	suite.Nil(expost.state)
	suite.Nil(expost.labelR)

	// Verify mock expectations
	mockNet.AssertExpectations(suite.T())
	mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestNewWithEmptyNeighbors() {
	testSid := "empty-neighbors-session"
	testVk := []byte("test-vk")
	testStartTime := time.Now()
	testRoundTimeout := 100 * time.Millisecond
	testLogger := zap.NewNop()

	mockNet := new(MockNetwork)
	mockMDAG := new(MockMDAG)

	// Test with empty neighbors list
	emptyNeighbors := []string{}
	mockNet.On("GetNeighbors").Return(emptyNeighbors).Once()
	mockNet.On("RegisterHandler", "/expost/1.0.0/empty-neighbors-session", mock.AnythingOfType("network.MessageHandler")).Once()

	expost := New(
		mockNet,
		mockMDAG,
		testSid,
		testVk,
		testStartTime,
		testRoundTimeout,
		2,  // diameter
		3,  // D
		16, // lambda
		mockGradeFunc,
		mockFilterTagFunc,
		testLogger,
	)

	suite.NotNil(expost)
	suite.Len(expost.neighbors, 0)
	suite.NotNil(expost.neighbors) // Map should still be initialized

	mockNet.AssertExpectations(suite.T())
	mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestNewWithSingleNeighbor() {
	testSid := "single-neighbor-session"
	testVk := []byte("test-vk")
	testStartTime := time.Now()
	testRoundTimeout := 50 * time.Millisecond
	testLogger := zap.NewNop()

	mockNet := new(MockNetwork)
	mockMDAG := new(MockMDAG)

	singleNeighbor := []string{"only-neighbor"}
	mockNet.On("GetNeighbors").Return(singleNeighbor).Once()
	mockNet.On("RegisterHandler", "/expost/1.0.0/single-neighbor-session", mock.AnythingOfType("network.MessageHandler")).Once()

	expost := New(
		mockNet,
		mockMDAG,
		testSid,
		testVk,
		testStartTime,
		testRoundTimeout,
		1, // diameter
		2, // D
		8, // lambda
		mockGradeFunc,
		mockFilterTagFunc,
		testLogger,
	)

	suite.NotNil(expost)
	suite.Len(expost.neighbors, 1)
	suite.True(expost.neighbors["only-neighbor"])

	mockNet.AssertExpectations(suite.T())
	mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestNewWithZeroValues() {
	testSid := ""
	testVk := []byte{}
	testStartTime := time.Time{}
	testRoundTimeout := 0 * time.Millisecond
	testLogger := zap.NewNop()

	mockNet := new(MockNetwork)
	mockMDAG := new(MockMDAG)

	neighbors := []string{"neighbor1"}
	mockNet.On("GetNeighbors").Return(neighbors).Once()
	mockNet.On("RegisterHandler", "/expost/1.0.0/", mock.AnythingOfType("network.MessageHandler")).Once()

	expost := New(
		mockNet,
		mockMDAG,
		testSid,
		testVk,
		testStartTime,
		testRoundTimeout,
		0, // diameter
		0, // D
		0, // lambda
		mockGradeFunc,
		mockFilterTagFunc,
		testLogger,
	)

	suite.NotNil(expost)
	suite.Equal("", expost.sid)
	suite.Equal([]byte{}, expost.vk)
	suite.Equal(time.Time{}, expost.startTime)
	suite.Equal(0*time.Millisecond, expost.roundTimeout)
	suite.Equal(0, expost.d)
	suite.Equal(0, expost.D)
	suite.Equal(0, expost.lambda)
	suite.True(expost.isRunning)

	mockNet.AssertExpectations(suite.T())
	mockMDAG.AssertExpectations(suite.T())
}

func (suite *ExPostTestSuite) TestNewProtocolIDGeneration() {
	testSid := "special/chars@session#123"
	testVk := []byte("test-vk")
	testStartTime := time.Now()
	testRoundTimeout := 100 * time.Millisecond
	testLogger := zap.NewNop()

	mockNet := new(MockNetwork)
	mockMDAG := new(MockMDAG)

	neighbors := []string{"neighbor1"}
	mockNet.On("GetNeighbors").Return(neighbors).Once()
	// Verify the protocol ID is generated correctly with special characters
	expectedProtocolID := "/expost/1.0.0/special/chars@session#123"
	mockNet.On("RegisterHandler", expectedProtocolID, mock.AnythingOfType("network.MessageHandler")).Once()

	expost := New(
		mockNet,
		mockMDAG,
		testSid,
		testVk,
		testStartTime,
		testRoundTimeout,
		3,  // diameter
		5,  // D
		32, // lambda
		mockGradeFunc,
		mockFilterTagFunc,
		testLogger,
	)

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

	state, label, err := suite.expost.Generate()

	suite.NoError(err)
	suite.Equal(expectedState, state)
	suite.Equal(expectedLabel, label)
	suite.Equal(expectedState, suite.expost.state)
	suite.Equal(expectedLabel, suite.expost.labelR)
}

func (suite *ExPostTestSuite) TestGenerateMDAGError() {
	suite.mockMDAG.On("Generate", suite.sid, suite.vk, mock.Anything).Return(nil, assert.AnError).Once()

	state, label, err := suite.expost.Generate()

	suite.Error(err)
	suite.Nil(state)
	suite.Nil(label)
	suite.Contains(err.Error(), "MDAG generation failed")
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

	suite.expost.labelR = []byte("test-value")
	suite.expost.state = sigma

	suite.mockNetwork.On("GetNodeID").Return("test-node").Twice()
	suite.mockNetwork.On("SendProtocolMessage", mock.Anything, mock.Anything).Once()

	results, err := suite.expost.Verify(
		suite.sid,
		suite.vk,
		sigma,
		auxTag,
		0.5,
		mockFilterFunc,
	)

	suite.NoError(err)
	suite.NotNil(results)
}

func (suite *ExPostTestSuite) TestVerifySessionMismatch() {
	sigma := createTestSigma(20)
	auxTag := &common.AuxTag{AuxKey: &common.AuxKey{}}

	results, err := suite.expost.Verify(
		"wrong-session",
		suite.vk,
		sigma,
		auxTag,
		0.5,
		mockFilterFunc,
	)

	suite.Error(err)
	suite.Nil(results)
	suite.Contains(err.Error(), "session ID mismatch")
}

func (suite *ExPostTestSuite) TestVerifyInsufficientSigmaLength() {
	sigma := createTestSigma(10) // Less than d*D = 15
	auxTag := &common.AuxTag{AuxKey: &common.AuxKey{}}

	results, err := suite.expost.Verify(
		suite.sid,
		suite.vk,
		sigma,
		auxTag,
		0.5,
		mockFilterFunc,
	)

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
		From:       "node1",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	err = suite.expost.handleMessage("node1", msgBytes)
	suite.NoError(err)

	suite.expost.mu.Lock()
	receivedMsg, exists := suite.expost.messages[1]["node1"]
	suite.expost.mu.Unlock()

	suite.True(exists)
	suite.Equal(suite.sid, receivedMsg.sid)
	suite.Equal([]byte("test-vk"), receivedMsg.vk)
	suite.Equal([]byte("test-value"), receivedMsg.v)
}

func (suite *ExPostTestSuite) TestHandleMessageNotRunning() {
	suite.expost.isRunning = false

	err := suite.expost.handleMessage("node1", []byte("test"))
	suite.Error(err)
	suite.Contains(err.Error(), "protocol not running")
}

func (suite *ExPostTestSuite) TestHandleMessageInvalidPayload() {
	suite.expost.isRunning = true

	err := suite.expost.handleMessage("node1", []byte("invalid-proto"))
	suite.Error(err)
}

func (suite *ExPostTestSuite) TestHandleMessageSessionMismatch() {
	suite.expost.isRunning = true

	msg := &pb.TimestampMessage{
		SessionId: "wrong-session",
		From:      "node1",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	err = suite.expost.handleMessage("node1", msgBytes)
	suite.Error(err)
	suite.Contains(err.Error(), "session id mismatch")
}

func (suite *ExPostTestSuite) TestHandleMessageSenderMismatch() {
	suite.expost.isRunning = true

	msg := &pb.TimestampMessage{
		SessionId: suite.sid,
		From:      "node2",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	err = suite.expost.handleMessage("node1", msgBytes)
	suite.Error(err)
	suite.Contains(err.Error(), "sender id mismatch")
}

func (suite *ExPostTestSuite) TestHandleMessageUnknownNeighbor() {
	suite.expost.isRunning = true

	msg := &pb.TimestampMessage{
		SessionId: suite.sid,
		From:      "unknown-node",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	err = suite.expost.handleMessage("unknown-node", msgBytes)
	suite.Error(err)
	suite.Contains(err.Error(), "sender not in neighbors list")
}

func (suite *ExPostTestSuite) TestHandleMessageDuplicate() {
	suite.expost.isRunning = true

	msg := &pb.TimestampMessage{
		SessionId:       suite.sid,
		VerificationKey: []byte("test-vk"),
		Value:           []byte("test-value"),
		Aux: &pb.Aux{
			AuxKey: &pb.AuxKeyMessage{},
		},
		MerklePath: []*pb.State{},
		Round:      1,
		From:       "node1",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	// First message should succeed
	err = suite.expost.handleMessage("node1", msgBytes)
	suite.NoError(err)

	// Second message should be ignored (no error, but not stored)
	err = suite.expost.handleMessage("node1", msgBytes)
	suite.NoError(err)

	suite.expost.mu.Lock()
	messages := suite.expost.messages[1]
	suite.expost.mu.Unlock()

	suite.Len(messages, 1) // Only one message stored
}

func (suite *ExPostTestSuite) TestValidateMerklePathHappyFlow() {
	merklePath := [][][]byte{
		{[]byte("path1")},
	}
	suite.expost.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("oracle-result")},
	}

	suite.mockMDAG.On("Oracle", mock.MatchedBy(func(args [][]byte) bool {
		return len(args) == 1 && string(args[0]) == "path1"
	})).Return([]byte("oracle-result")).Once()

	isValid := suite.expost.validateMerklePath(merklePath, 1)
	suite.True(isValid)
}

func (suite *ExPostTestSuite) TestValidateMerklePathInvalidLength() {
	merklePath := [][][]byte{{[]byte("path1")}}

	isValid := suite.expost.validateMerklePath(merklePath, 2)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestValidateMerklePathInvalidStateLength() {
	merklePath := [][][]byte{
		{[]byte("path1")},
		{[]byte("path2")},
	}
	suite.expost.state = [][][]byte{{[]byte("state0")}}

	isValid := suite.expost.validateMerklePath(merklePath, 2)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestIsMessageValidHappyFlow() {
	msg := &receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),
		v:   []byte("test-value"),
		aux: &common.AuxKey{},
		merklePath: [][][]byte{
			{[]byte("path1")},
		},
	}

	suite.expost.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("oracle-result")},
	}

	suite.mockMDAG.On("Oracle", mock.MatchedBy(func(args [][]byte) bool {
		return len(args) == 1 && string(args[0]) == "path1"
	})).Return([]byte("oracle-result")).Once()

	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterFunc, 1)
	suite.True(isValid)
}

func (suite *ExPostTestSuite) TestIsMessageValidNilMessage() {
	isValid := suite.expost.isMessageValid(nil, 0.5, mockFilterFunc, 1)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestIsMessageValidFilterFails() {
	msg := &receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),
		v:   []byte("test-value"),
		aux: &common.AuxKey{},
	}

	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterFuncFalse, 1)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestIsMessageValidGradeFunctionFails() {
	msg := &receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk"),
		v:   []byte("test-value"),
		aux: &common.AuxKey{},
	}

	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterFunc, 1)
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

func mockFilterFunc(_ string, _ []byte, _ []byte, _ *common.AuxKey) bool {
	return true
}

func mockFilterFuncFalse(_ string, _ []byte, _ []byte, _ *common.AuxKey) bool {
	return false
}

func mockFilterTagFunc(_ string, _ []byte, _ []byte, _ *common.AuxTag) bool {
	return true
}

func (suite *ExPostTestSuite) TestGenerateWithEmptyCharset() {
	// This should panic due to empty charset
	suite.Panics(func() {
		secureRandomBytes(10, "")
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

	sigma := createTestSigma(6) // d*D = 2*3 = 6
	auxTag := &common.AuxTag{AuxKey: &common.AuxKey{}}

	suite.expost.labelR = []byte("test-label")
	suite.expost.state = sigma

	results, err := suite.expost.Verify(
		suite.sid,
		suite.vk,
		sigma,
		auxTag,
		0.5,
		mockFilterFunc,
	)

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

	suite.expost.labelR = []byte("high-grade-label")
	suite.expost.state = sigma
	suite.expost.gradeFunc = edgeCaseGradeFunc
	suite.expost.filterFunc = edgeCaseFilterTagFunc

	suite.mockNetwork.On("GetNodeID").Return("test-node").Twice()
	suite.mockNetwork.On("SendProtocolMessage", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Once()

	results, err := suite.expost.Verify(
		suite.sid,
		[]byte("high-grade-vk"),
		sigma,
		auxTag,
		0.5,
		highGradeFilterFunc,
	)

	suite.NoError(err)
	suite.NotNil(results)
}

func (suite *ExPostTestSuite) TestVerifyWithLowGradeProver() {
	sigma := createTestSigma(20)
	auxTag := &common.AuxTag{
		PiRP:   []byte("pi-rp"),
		AuxKey: &common.AuxKey{},
	}

	suite.expost.labelR = []byte("low-grade-label")
	suite.expost.state = sigma
	suite.expost.gradeFunc = edgeCaseGradeFunc
	suite.expost.filterFunc = edgeCaseFilterTagFunc

	suite.mockNetwork.On("GetNodeID").Return("test-node").Once()

	results, err := suite.expost.Verify(
		suite.sid,
		[]byte("low-grade-vk"),
		sigma,
		auxTag,
		0.5,
		lowGradeFilterFunc,
	)

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
		From:            "node1",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	// This should panic due to nil aux
	suite.Panics(func() {
		suite.expost.handleMessage("node1", msgBytes)
	})
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
		From:       "node1",
	}

	msgBytes, err := proto.Marshal(msg)
	suite.NoError(err)

	// This should panic due to nil aux key
	suite.Panics(func() {
		suite.expost.handleMessage("node1", msgBytes)
	})
}

func (suite *ExPostTestSuite) TestValidateMerklePathWithEmptyPath() {
	merklePath := [][][]byte{}
	isValid := suite.expost.validateMerklePath(merklePath, 1)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestValidateMerklePathWithEmptyState() {
	merklePath := [][][]byte{{[]byte("path1")}}
	suite.expost.state = [][][]byte{}

	isValid := suite.expost.validateMerklePath(merklePath, 1)
	suite.False(isValid)
}

func (suite *ExPostTestSuite) TestValidateMerklePathOracleFailure() {
	merklePath := [][][]byte{{[]byte("path1")}}
	suite.expost.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("different-result")},
	}

	suite.mockMDAG.On("Oracle", mock.MatchedBy(func(args [][]byte) bool {
		return len(args) == 1 && string(args[0]) == "path1"
	})).Return([]byte("oracle-result")).Once()

	isValid := suite.expost.validateMerklePath(merklePath, 1)
	suite.False(isValid)
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
		aux:        &common.AuxKey{},
		merklePath: [][][]byte{},
	}

	suite.expost.state = [][][]byte{
		{[]byte("state0")},
	}
	suite.expost.gradeFunc = edgeCaseGradeFunc

	// This should fail because the edgeCaseGradeFunc returns 0 for these values
	isValid := suite.expost.isMessageValid(msg, 0.5, mockFilterFunc, 0)
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
		From:            "node1",
	}

	msg2 := &pb.TimestampMessage{
		SessionId:       suite.sid,
		VerificationKey: []byte("test-vk2"),
		Value:           []byte("test-value2"),
		Aux:             &pb.Aux{AuxKey: &pb.AuxKeyMessage{}},
		MerklePath:      []*pb.State{},
		Round:           1,
		From:            "node2",
	}

	msgBytes1, _ := proto.Marshal(msg1)
	msgBytes2, _ := proto.Marshal(msg2)

	done := make(chan bool, 2)
	go func() {
		suite.expost.handleMessage("node1", msgBytes1)
		done <- true
	}()
	go func() {
		suite.expost.handleMessage("node2", msgBytes2)
		done <- true
	}()

	<-done
	<-done

	suite.expost.mu.Lock()
	messages := suite.expost.messages[1]
	suite.expost.mu.Unlock()

	suite.Len(messages, 2)
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

func edgeCaseFilterTagFunc(_ string, vk []byte, _ []byte, _ *common.AuxTag) bool {
	return string(vk) != "filtered-vk"
}

func highGradeFilterFunc(_ string, vk []byte, _ []byte, _ *common.AuxKey) bool {
	return string(vk) == "high-grade-vk"
}

func lowGradeFilterFunc(_ string, vk []byte, _ []byte, _ *common.AuxKey) bool {
	return string(vk) == "low-grade-vk"
}

func (suite *ExPostTestSuite) TestVerifyMessageProcessingLoop() {
	// Setup test data with smaller d and D to avoid long waits
	suite.expost.d = 2
	suite.expost.D = 2

	sigma := createTestSigma(10) // More than d*D = 4
	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	suite.expost.labelR = []byte("test-label")
	suite.expost.state = sigma
	suite.expost.startTime = time.Now().Add(-time.Hour) // Set start time in the past to avoid waiting

	// Pre-populate messages for round 1 to test the processing loop
	// Make sure merklePath has enough elements for the round
	testMsg1 := receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk1"),
		v:   []byte("test-value1"),
		aux: &common.AuxKey{
			PhiVRF: []byte("phi-vrf1"),
			PiVRF:  []byte("pi-vrf1"),
			PhiVDF: []byte("phi-vdf1"),
			PiVDF:  []byte("pi-vdf1"),
		},
		merklePath: [][][]byte{
			{[]byte("path0")}, // index 0
			{[]byte("path1")}, // index 1 - needed for round 1
		},
	}

	testMsg2 := receivedMessage{
		sid: suite.sid,
		vk:  []byte("test-vk2"),
		v:   []byte("test-value2"),
		aux: &common.AuxKey{
			PhiVRF: []byte("phi-vrf2"),
			PiVRF:  []byte("pi-vrf2"),
			PhiVDF: []byte("phi-vdf2"),
			PiVDF:  []byte("pi-vdf2"),
		},
		merklePath: [][][]byte{
			{[]byte("path0")}, // index 0
			{[]byte("path2")}, // index 1 - needed for round 1
		},
	}

	// Add messages to round 1
	suite.expost.messages[1] = map[string]receivedMessage{
		"node1": testMsg1,
		"node2": testMsg2,
	}

	// Setup mock expectations for message validation
	// Oracle is called with merklePath[0] for round 1 validation
	suite.mockMDAG.On("Oracle", mock.MatchedBy(func(args [][]byte) bool {
		return len(args) == 1 && string(args[0]) == "path0"
	})).Return([]byte("oracle-result")).Times(2)

	// Setup network mock expectations
	suite.mockNetwork.On("GetNodeID").Return("test-node").Times(4) // Called multiple times during message propagation

	// Expect SendProtocolMessage to be called for message propagation
	suite.mockNetwork.On("SendProtocolMessage", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Times(2)

	// Create a custom grade function that returns different grades for different messages
	customGradeFunc := func(_ string, vk []byte, _ []byte, _ *common.AuxKey, _ float64) int {
		if string(vk) == "test-vk1" {
			return 5 // High grade
		}
		if string(vk) == "test-vk2" {
			return 3 // Lower grade
		}
		return 0
	}
	suite.expost.gradeFunc = customGradeFunc

	// Setup state for validation - needs to have oracle-result at index 1
	suite.expost.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("oracle-result")}, // This should match Oracle return value
	}

	// Call Verify to trigger the message processing loop
	results, err := suite.expost.Verify(
		suite.sid,
		suite.vk,
		sigma,
		auxTag,
		0.5,
		mockFilterFunc,
	)

	// Verify results
	suite.NoError(err)
	suite.NotNil(results)

	// Check that both messages were processed and stored in results
	suite.Len(results, 2)

	// Verify the results contain the expected keys and grades
	key1 := common.Key{VK: "test-vk1", Ch: "test-value1"}
	key2 := common.Key{VK: "test-vk2", Ch: "test-value2"}

	suite.Contains(results, key1)
	suite.Contains(results, key2)

	// Verify grades are calculated correctly (min of d-r/D and gradeFunc result)
	// For round 1: min(2-1/2, gradeFunc) = min(2, gradeFunc)
	suite.Equal(2, results[key1].Grade) // min(2, 5) = 2
	suite.Equal(2, results[key2].Grade) // min(2, 3) = 2

	// Verify the result structure
	suite.Equal([]byte("test-vk1"), results[key1].VK)
	suite.Equal([]byte("test-value1"), results[key1].Challenge)
	suite.NotNil(results[key1].Aux)
	suite.NotNil(results[key1].Aux.AuxKey)

	suite.Equal([]byte("test-vk2"), results[key2].VK)
	suite.Equal([]byte("test-value2"), results[key2].Challenge)
	suite.NotNil(results[key2].Aux)
	suite.NotNil(results[key2].Aux.AuxKey)
}

func (suite *ExPostTestSuite) TestVerifyMessageProcessingDebugLogging() {
	// Test the debug logging when a message with lower grade is ignored
	suite.expost.d = 2
	suite.expost.D = 2
	sigma := createTestSigma(10)
	auxTag := &common.AuxTag{
		PiRP: []byte("pi-rp"),
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("phi-vrf"),
			PiVRF:  []byte("pi-vrf"),
			PhiVDF: []byte("phi-vdf"),
			PiVDF:  []byte("pi-vdf"),
		},
	}

	suite.expost.labelR = []byte("test-label")
	suite.expost.state = sigma
	suite.expost.startTime = time.Now().Add(-time.Hour)

	// Create two messages with same key but different grades
	testMsg1 := receivedMessage{
		sid: suite.sid,
		vk:  []byte("same-vk"),
		v:   []byte("same-value"),
		aux: &common.AuxKey{PhiVRF: []byte("high-grade")},
		merklePath: [][][]byte{
			{[]byte("path0")},
			{[]byte("path1")},
		},
	}

	testMsg2 := receivedMessage{
		sid: suite.sid,
		vk:  []byte("same-vk"),
		v:   []byte("same-value"),
		aux: &common.AuxKey{PhiVRF: []byte("low-grade")},
		merklePath: [][][]byte{
			{[]byte("path0")},
			{[]byte("path2")},
		},
	}

	// Add messages to round 1
	suite.expost.messages[1] = map[string]receivedMessage{
		"node1": testMsg1,
		"node2": testMsg2,
	}

	// Setup mock expectations
	// Oracle is called with merklePath[0] for round 1 validation
	suite.mockMDAG.On("Oracle", mock.MatchedBy(func(args [][]byte) bool {
		return len(args) == 1 && string(args[0]) == "path0"
	})).Return([]byte("oracle-result")).Times(2)

	suite.mockNetwork.On("GetNodeID").Return("test-node").Times(2)
	suite.mockNetwork.On("SendProtocolMessage", mock.AnythingOfType("string"), mock.AnythingOfType("[]uint8")).Once()

	// Custom grade function
	customGradeFunc := func(_ string, _ []byte, _ []byte, aux *common.AuxKey, _ float64) int {
		if string(aux.PhiVRF) == "high-grade" {
			return 5
		}
		if string(aux.PhiVRF) == "low-grade" {
			return 2
		}
		return 0
	}
	suite.expost.gradeFunc = customGradeFunc

	suite.expost.state = [][][]byte{
		{[]byte("state0")},
		{[]byte("oracle-result")},
	}

	results, err := suite.expost.Verify(
		suite.sid,
		suite.vk,
		sigma,
		auxTag,
		0.5,
		mockFilterFunc,
	)

	suite.NoError(err)
	suite.NotNil(results)

	// Should have one result (the higher grade message)
	suite.Len(results, 1)
	key := common.Key{VK: "same-vk", Ch: "same-value"}
	suite.Contains(results, key)
	suite.Equal(2, results[key].Grade) // min(2, 5) = 2
}
