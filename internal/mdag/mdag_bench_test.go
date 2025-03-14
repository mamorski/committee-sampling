package mdag_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/mdag"
	"github.com/mamorski/committee-sampling/internal/network"
	mdagpb "github.com/mamorski/committee-sampling/pkg/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// Simple hash oracle for benchmarking
func benchOracle(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

// BenchmarkVerifySmallPath benchmarks the Verify function with a small path
func BenchmarkVerifySmallPath(b *testing.B) {
	benchmarkVerify(b, 3, 5)
}

// BenchmarkVerifyMediumPath benchmarks the Verify function with a medium path
func BenchmarkVerifyMediumPath(b *testing.B) {
	benchmarkVerify(b, 5, 10)
}

// BenchmarkVerifyLargePath benchmarks the Verify function with a large path
func BenchmarkVerifyLargePath(b *testing.B) {
	benchmarkVerify(b, 10, 20)
}

// Helper function to benchmark Verify with different path sizes
func benchmarkVerify(b *testing.B, depth, width int) {
	// Setup
	mockNetwork := new(MockNetwork)
	logger, _ := zap.NewDevelopment()
	mockNetwork.On("GetNeighbors").Return([]string{"node1", "node2", "node3"})
	mockNetwork.On("GetNodeID").Return("testNode")
	mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return()

	mdagInstance := mdag.New(3, "test-session", benchOracle, mockNetwork, 100*time.Millisecond, logger, time.Now())

	// Create a path for benchmarking
	path := make([][][]byte, depth)
	for i := 0; i < depth; i++ {
		path[i] = make([][]byte, width)
		for j := 0; j < width; j++ {
			path[i][j] = []byte(fmt.Sprintf("label-%d-%d", i, j))
		}
	}

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
	currentLabel := benchOracle(buffer.Bytes())

	// Process remaining levels
	for i := 1; i < len(path); i++ {
		labelSet := make([][]byte, len(path[i])+1)
		labelSet[0] = currentLabel
		copy(labelSet[1:], path[i])
		sort.Slice(labelSet, func(i, j int) bool {
			return bytes.Compare(labelSet[i], labelSet[j]) < 0
		})
		buffer.Reset()
		for _, lab := range labelSet {
			buffer.Write(lab)
		}
		currentLabel = benchOracle(buffer.Bytes())
	}

	targetLabels := map[string]bool{
		string(currentLabel): true,
	}

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mdagInstance.Verify(path, targetLabels)
	}
}

// BenchmarkJoinBytes benchmarks the bytes.Join function
func BenchmarkJoinBytes(b *testing.B) {
	// Create test data
	data := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		data[i] = []byte(fmt.Sprintf("data-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes.Join(data, nil)
	}
}

// BenchmarkBufferConcat benchmarks concatenation using bytes.Buffer
func BenchmarkBufferConcat(b *testing.B) {
	// Create test data
	data := make([][]byte, 100)
	for i := 0; i < 100; i++ {
		data[i] = []byte(fmt.Sprintf("data-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var buffer bytes.Buffer
		buffer.Grow(1024)
		for _, d := range data {
			buffer.Write(d)
		}
		_ = buffer.Bytes()
	}
}

// BenchmarkHandleMessage benchmarks the message handling performance
func BenchmarkHandleMessage(b *testing.B) {
	// Setup
	mockNetwork := new(MockNetwork)
	logger, _ := zap.NewDevelopment()
	mockNetwork.On("GetNeighbors").Return([]string{"node1", "node2", "node3"})
	mockNetwork.On("GetNodeID").Return("testNode")
	mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return()

	mdagInstance := mdag.New(3, "test-session", benchOracle, mockNetwork, 100*time.Millisecond, logger, time.Now())
	require.NotNil(b, mdagInstance)

	// Create a valid message
	validMsg := &mdagpb.MDAGMessage{
		SessionId: "test-session",
		Round:     1,
		Label:     []byte("test-label"),
		From:      "node1", // This is in our neighbors list
	}
	validData, _ := proto.Marshal(validMsg)

	// Get the handler registered with the network
	var handler network.MessageHandler
	for _, call := range mockNetwork.Calls {
		if call.Method == "RegisterHandler" {
			handler = call.Arguments.Get(1).(network.MessageHandler)
			break
		}
	}

	if handler == nil {
		b.Fatal("Failed to get message handler")
	}

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		handler("node1", validData)
	}
}

// BenchmarkGenerate benchmarks the Generate function
func BenchmarkGenerate(b *testing.B) {
	// This is a simplified benchmark since Generate is a long-running function
	// We'll measure the time to initialize and start the protocol

	b.Run("Initialization", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			mockNetwork := new(MockNetwork)
			logger, _ := zap.NewDevelopment()
			mockNetwork.On("GetNeighbors").Return([]string{"node1", "node2", "node3"})
			mockNetwork.On("GetNodeID").Return("testNode")
			mockNetwork.On("RegisterHandler", mock.Anything, mock.Anything).Return()
			mockNetwork.On("SendProtocolMessage", mock.Anything, mock.Anything).Return()

			mdagInstance := mdag.New(3, "test-session", benchOracle, mockNetwork, 100*time.Millisecond, logger, time.Now())

			// Just initialize the protocol but don't run it to completion
			go func() {
				mdagInstance.Generate("test-session", []byte("vki"), []byte("vi"))
			}()

			// Wait a short time for initialization
			time.Sleep(10 * time.Millisecond)
		}
	})
}
