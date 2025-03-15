package mdag_test

import (
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/mdag"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// InMemoryNetwork is a simple in-memory implementation of the network.Network interface
// for integration testing with multiple MDAG instances
type InMemoryNetwork struct {
	nodeID         string
	handlers       map[string]network.MessageHandler
	peers          map[string]*InMemoryNetwork
	neighbors      []string
	mu             sync.RWMutex
	messageCounter int
}

func NewInMemoryNetwork(nodeID string, neighbors []string) *InMemoryNetwork {
	return &InMemoryNetwork{
		nodeID:    nodeID,
		handlers:  make(map[string]network.MessageHandler),
		peers:     make(map[string]*InMemoryNetwork),
		neighbors: neighbors,
	}
}

func (n *InMemoryNetwork) RegisterHandler(protocolID string, handler network.MessageHandler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[protocolID] = handler
}

func (n *InMemoryNetwork) SendProtocolMessage(protocolID string, data []byte) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Increment message counter for testing
	n.messageCounter++

	// Send to all peers
	for _, peer := range n.peers {
		go func(p *InMemoryNetwork) {
			p.mu.RLock()
			handler, exists := p.handlers[protocolID]
			p.mu.RUnlock()

			if exists {
				handler(n.nodeID, data)
			}
		}(peer)
	}
}

func (n *InMemoryNetwork) GetNeighbors() []string {
	return n.neighbors
}

func (n *InMemoryNetwork) GetNodeID() string {
	return n.nodeID
}

func (n *InMemoryNetwork) Close() error {
	return nil
}

func (n *InMemoryNetwork) AddPeer(peer *InMemoryNetwork) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[peer.nodeID] = peer
}

func (n *InMemoryNetwork) GetMessageCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.messageCounter
}

// Simple hash oracle for testing
func testOracleIntegration(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

// TestMDAGIntegration tests the MDAG protocol with multiple nodes
func TestMDAGIntegration(t *testing.T) {
	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Create network nodes
	node1 := NewInMemoryNetwork("node1", []string{"node2", "node3"})
	node2 := NewInMemoryNetwork("node2", []string{"node1", "node3"})
	node3 := NewInMemoryNetwork("node3", []string{"node1", "node2"})

	// Connect the nodes
	node1.AddPeer(node2)
	node1.AddPeer(node3)
	node2.AddPeer(node1)
	node2.AddPeer(node3)
	node3.AddPeer(node1)
	node3.AddPeer(node2)

	// Create MDAG instances
	rounds := 3
	sessionID := "test-integration"
	roundTimeout := 100 * time.Millisecond
	startTime := time.Now().Add(200 * time.Millisecond)

	mdag1 := mdag.New(rounds, sessionID, testOracleIntegration, node1, roundTimeout, logger, startTime)
	mdag2 := mdag.New(rounds, sessionID, testOracleIntegration, node2, roundTimeout, logger, startTime)
	mdag3 := mdag.New(rounds, sessionID, testOracleIntegration, node3, roundTimeout, logger, startTime)

	// Run the protocol on all nodes
	done := make(chan bool, 3)
	var state1, state2, state3 [][][]byte
	var err1, err2, err3 error

	go func() {
		state1, err1 = mdag1.Generate(sessionID, []byte("vki"), []byte("node1"))
		done <- true
	}()

	go func() {
		state2, err2 = mdag2.Generate(sessionID, []byte("vki"), []byte("node2"))
		done <- true
	}()

	go func() {
		state3, err3 = mdag3.Generate(sessionID, []byte("vki"), []byte("node3"))
		done <- true
	}()

	// Wait for all nodes to complete
	for i := 0; i < 3; i++ {
		select {
		case <-done:
			// Node completed
		case <-time.After(2 * time.Second):
			t.Fatal("Protocol timed out")
		}
	}

	// Check for errors
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NoError(t, err3)

	// Verify that messages were exchanged
	assert.Greater(t, node1.GetMessageCount(), 0)
	assert.Greater(t, node2.GetMessageCount(), 0)
	assert.Greater(t, node3.GetMessageCount(), 0)

	// Verify that states were populated
	assert.NotNil(t, state1)
	assert.NotNil(t, state2)
	assert.NotNil(t, state3)

	// Verify that the final labels match
	finalLabel1 := mdag1.GetComputedLabel(rounds - 1)
	finalLabel2 := mdag2.GetComputedLabel(rounds - 1)
	finalLabel3 := mdag3.GetComputedLabel(rounds - 1)

	assert.NotNil(t, finalLabel1)
	assert.NotNil(t, finalLabel2)
	assert.NotNil(t, finalLabel3)

	assert.Equal(t, finalLabel1, finalLabel2)
	assert.Equal(t, finalLabel2, finalLabel3)
}

// TestMDAGNeighborFiltering tests that nodes only accept messages from known neighbors
func TestMDAGNeighborFiltering(t *testing.T) {
	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Create network nodes with restricted neighbor lists
	node1 := NewInMemoryNetwork("node1", []string{"node2"}) // Only node2 is a neighbor
	node2 := NewInMemoryNetwork("node2", []string{"node1"}) // Only node1 is a neighbor
	node3 := NewInMemoryNetwork("node3", []string{})        // No neighbors

	// Connect all nodes (physical connection)
	node1.AddPeer(node2)
	node1.AddPeer(node3)
	node2.AddPeer(node1)
	node2.AddPeer(node3)
	node3.AddPeer(node1)
	node3.AddPeer(node2)

	// Create MDAG instances
	rounds := 2
	sessionID := "test-filtering"
	roundTimeout := 100 * time.Millisecond
	startTime := time.Now().Add(200 * time.Millisecond)

	mdag1 := mdag.New(rounds, sessionID, testOracleIntegration, node1, roundTimeout, logger, startTime)
	mdag2 := mdag.New(rounds, sessionID, testOracleIntegration, node2, roundTimeout, logger, startTime)
	mdag3 := mdag.New(rounds, sessionID, testOracleIntegration, node3, roundTimeout, logger, startTime)

	// Run the protocol on all nodes
	done := make(chan bool, 3)
	var state1, state2, state3 [][][]byte
	var err1, err2, err3 error

	go func() {
		state1, err1 = mdag1.Generate(sessionID, []byte("vki"), []byte("node1"))
		done <- true
	}()

	go func() {
		state2, err2 = mdag2.Generate(sessionID, []byte("vki"), []byte("node2"))
		done <- true
	}()

	go func() {
		state3, err3 = mdag3.Generate(sessionID, []byte("vki"), []byte("node3"))
		done <- true
	}()

	// Wait for all nodes to complete
	for i := 0; i < 3; i++ {
		select {
		case <-done:
			// Node completed
		case <-time.After(2 * time.Second):
			t.Fatal("Protocol timed out")
		}
	}

	// Check for errors
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NoError(t, err3)

	// Verify that node1 and node2 exchanged messages with each other
	assert.Greater(t, node1.GetMessageCount(), 0)
	assert.Greater(t, node2.GetMessageCount(), 0)

	// Verify that node3 sent messages but they were filtered by the other nodes
	assert.Greater(t, node3.GetMessageCount(), 0)

	// Verify that node1 and node2 have each other's labels in their state
	// but not node3's labels
	assert.Equal(t, 2, len(state1[0])) // Own label + node2's label
	assert.Equal(t, 2, len(state2[0])) // Own label + node1's label
	assert.Equal(t, 1, len(state3[0])) // Only own label
}

// TestMDAGFourNodeVerification tests verification of a Merkle path with four nodes
func TestMDAGFourNodeVerification(t *testing.T) {
	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Create network nodes
	node1 := NewInMemoryNetwork("node1", []string{"node2", "node3", "node4"})
	node2 := NewInMemoryNetwork("node2", []string{"node1", "node3", "node4"})
	node3 := NewInMemoryNetwork("node3", []string{"node1", "node2", "node4"})
	node4 := NewInMemoryNetwork("node4", []string{"node1", "node2", "node3"})

	// Connect the nodes
	node1.AddPeer(node2)
	node1.AddPeer(node3)
	node1.AddPeer(node4)
	node2.AddPeer(node1)
	node2.AddPeer(node3)
	node2.AddPeer(node4)
	node3.AddPeer(node1)
	node3.AddPeer(node2)
	node3.AddPeer(node4)
	node4.AddPeer(node1)
	node4.AddPeer(node2)
	node4.AddPeer(node3)

	// Create MDAG instances
	rounds := 3
	sessionID := "test-verification"
	roundTimeout := 100 * time.Millisecond
	startTime := time.Now().Add(200 * time.Millisecond)

	mdag1 := mdag.New(rounds, sessionID, testOracleIntegration, node1, roundTimeout, logger, startTime)
	mdag2 := mdag.New(rounds, sessionID, testOracleIntegration, node2, roundTimeout, logger, startTime)
	mdag3 := mdag.New(rounds, sessionID, testOracleIntegration, node3, roundTimeout, logger, startTime)
	mdag4 := mdag.New(rounds, sessionID, testOracleIntegration, node4, roundTimeout, logger, startTime)

	// Run the protocol on all nodes
	done := make(chan bool, 4)
	var state1, state2, state3, state4 [][][]byte
	var err1, err2, err3, err4 error

	go func() {
		state1, err1 = mdag1.Generate(sessionID, []byte("vki"), []byte("node1"))
		done <- true
	}()

	go func() {
		state2, err2 = mdag2.Generate(sessionID, []byte("vki"), []byte("node2"))
		done <- true
	}()

	go func() {
		state3, err3 = mdag3.Generate(sessionID, []byte("vki"), []byte("node3"))
		done <- true
	}()

	go func() {
		state4, err4 = mdag4.Generate(sessionID, []byte("vki"), []byte("node4"))
		done <- true
	}()

	// Wait for all nodes to complete
	for i := 0; i < 4; i++ {
		select {
		case <-done:
			// Node completed
		case <-time.After(2 * time.Second):
			t.Fatal("Protocol timed out")
		}
	}

	// Check for errors
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NoError(t, err3)
	assert.NoError(t, err4)

	// Get the final labels
	finalLabel1 := mdag1.GetComputedLabel(rounds - 1)
	finalLabel2 := mdag2.GetComputedLabel(rounds - 1)
	finalLabel3 := mdag3.GetComputedLabel(rounds - 1)
	finalLabel4 := mdag4.GetComputedLabel(rounds - 1)

	// Verify that all final labels are the same
	assert.Equal(t, finalLabel1, finalLabel2)
	assert.Equal(t, finalLabel2, finalLabel3)
	assert.Equal(t, finalLabel3, finalLabel4)

	// Create a verifier
	verifier := mdag.New(rounds, sessionID, testOracleIntegration, node1, roundTimeout, logger, startTime)

	// Create a target label map
	targetLabels := map[string]bool{
		string(finalLabel1): true,
	}

	// Verify the path from node1
	assert.True(t, verifier.Verify(state1, targetLabels))

	// Verify the path from node2
	assert.True(t, verifier.Verify(state2, targetLabels))

	// Verify the path from node3
	assert.True(t, verifier.Verify(state3, targetLabels))

	// Verify the path from node4
	assert.True(t, verifier.Verify(state4, targetLabels))

	// Modify a label in the path and verify that it fails
	modifiedState := make([][][]byte, len(state1))
	for i := range state1 {
		modifiedState[i] = make([][]byte, len(state1[i]))
		for j := range state1[i] {
			modifiedState[i][j] = make([]byte, len(state1[i][j]))
			copy(modifiedState[i][j], state1[i][j])
		}
	}

	if len(modifiedState[0]) > 0 {
		modifiedState[0][0] = []byte("modified-label")
	}

	assert.False(t, verifier.Verify(modifiedState, targetLabels))
}
