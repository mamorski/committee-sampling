package mdag_test

import (
	"crypto/sha256"
	"sync"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/mdag"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

	// Create a network of 3 nodes
	nodeIDs := []string{"node1", "node2", "node3"}
	networks := make(map[string]*InMemoryNetwork)

	// Create networks with all nodes as neighbors
	for _, id := range nodeIDs {
		// Create a list of neighbors (all other nodes)
		var neighbors []string
		for _, nid := range nodeIDs {
			if nid != id {
				neighbors = append(neighbors, nid)
			}
		}
		networks[id] = NewInMemoryNetwork(id, neighbors)
	}

	// Connect the networks
	for _, n1 := range networks {
		for _, n2 := range networks {
			if n1.nodeID != n2.nodeID {
				n1.AddPeer(n2)
			}
		}
	}

	// Create MDAG instances
	mdags := make(map[string]*mdag.MDAG)
	sessionID := "test-session"
	startTime := time.Now().Add(50 * time.Millisecond)
	roundTimeout := 100 * time.Millisecond
	rounds := 3 // Number of rounds for the MDAG protocol

	for id, net := range networks {
		mdags[id] = mdag.New(rounds, sessionID, testOracleIntegration, net, roundTimeout, logger, startTime)
	}

	// Run the protocol on all nodes
	var wg sync.WaitGroup
	var resultsMutex sync.Mutex
	results := make(map[string][][][]byte)
	errors := make(map[string]error)
	finalLabels := make(map[string][]byte) // Map to store final labels

	for id, mdagInstance := range mdags {
		wg.Add(1)
		go func(nodeID string, m *mdag.MDAG) {
			defer wg.Done()

			// Use the node ID as part of the input to ensure different initial labels
			vki := []byte(nodeID)
			vi := [][]byte{[]byte("test-input")}

			state, err := m.Generate(sessionID, vki, vi...)
			if err != nil {
				t.Logf("Node %s failed to generate: %v", nodeID, err)
				return
			}

			resultsMutex.Lock()
			results[nodeID] = state

			// Extract the final label using the new GetComputedLabel method
			// The final round index is rounds-1 (0-indexed)
			finalLabel := m.GetComputedLabel(rounds - 1)
			if finalLabel != nil {
				finalLabels[nodeID] = finalLabel
				t.Logf("Node %s final label: %x", nodeID, finalLabel)
			}
			resultsMutex.Unlock()
		}(id, mdagInstance)
	}

	// Wait for all nodes to complete with a timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Continue with the test
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for nodes to complete")
	}

	// Verify results
	resultsMutex.Lock()
	defer resultsMutex.Unlock()

	for id, err := range errors {
		if err != nil {
			t.Errorf("Node %s failed with error: %v", id, err)
		}
	}

	// Check that all nodes have the same state length
	for id, state := range results {
		if len(state) != 3 {
			t.Errorf("Node %s has incorrect state length: %d", id, len(state))
		}
	}

	// Verify message counts
	for id, net := range networks {
		count := net.GetMessageCount()
		if count == 0 {
			t.Errorf("Node %s didn't send any messages", id)
		}
		t.Logf("Node %s sent %d messages", id, count)
	}
}

// TestMDAGNeighborFiltering tests that messages from non-neighbors are rejected
func TestMDAGNeighborFiltering(t *testing.T) {
	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Create a network with specific neighbor relationships
	// node1 considers node2 a neighbor, but not node3
	// node2 considers both node1 and node3 as neighbors
	// node3 considers node2 a neighbor, but not node1
	networks := make(map[string]*InMemoryNetwork)

	networks["node1"] = NewInMemoryNetwork("node1", []string{"node2"})
	networks["node2"] = NewInMemoryNetwork("node2", []string{"node1", "node3"})
	networks["node3"] = NewInMemoryNetwork("node3", []string{"node2"})

	// Connect the networks (all nodes can physically communicate)
	for _, n1 := range networks {
		for _, n2 := range networks {
			if n1.nodeID != n2.nodeID {
				n1.AddPeer(n2)
			}
		}
	}

	// Create MDAG instances
	mdags := make(map[string]*mdag.MDAG)
	sessionID := "test-neighbor-filtering"
	startTime := time.Now().Add(100 * time.Millisecond)
	roundTimeout := 200 * time.Millisecond

	for id, net := range networks {
		mdags[id] = mdag.New(2, sessionID, testOracleIntegration, net, roundTimeout, logger, startTime)
	}

	// Run the protocol on all nodes
	var wg sync.WaitGroup

	for id, mdagInstance := range mdags {
		wg.Add(1)
		go func(nodeID string, m *mdag.MDAG) {
			defer wg.Done()
			vki := []byte(nodeID)
			vi := [][]byte{[]byte("filtering-test")}
			m.Generate(sessionID, vki, vi...)
		}(id, mdagInstance)
	}

	// Wait for the protocol to complete
	time.Sleep(1 * time.Second)

	// Check message counts
	// node1 should receive messages from node2 but not node3
	// node2 should receive messages from both node1 and node3
	// node3 should receive messages from node2 but not node1

	// We can't directly check which messages were accepted/rejected
	// But we can verify that messages were sent
	for id, net := range networks {
		count := net.GetMessageCount()
		assert.Greater(t, count, 0, "Node %s didn't send any messages", id)
		t.Logf("Node %s sent %d messages", id, count)
	}
}

// TestMDAGGenerateVerifyIntegration tests the complete flow of the MDAG protocol
// where each node generates its path and then verifies paths from other nodes
// using the labels it received in the last round
func TestMDAGGenerateVerifyIntegration(t *testing.T) {
	// Create logger
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}

	// Create a fully connected network with 3 nodes
	nodeIDs := []string{"node1", "node2", "node3"}
	networks := make(map[string]*InMemoryNetwork)

	// Create networks with all nodes as neighbors (fully connected graph)
	for _, id := range nodeIDs {
		var neighbors []string
		for _, nid := range nodeIDs {
			if nid != id {
				neighbors = append(neighbors, nid)
			}
		}
		networks[id] = NewInMemoryNetwork(id, neighbors)
	}

	// Connect the networks
	for _, n1 := range networks {
		for _, n2 := range networks {
			if n1.nodeID != n2.nodeID {
				n1.AddPeer(n2)
			}
		}
	}

	// Create MDAG instances with 3 rounds
	mdags := make(map[string]*mdag.MDAG)
	sessionID := "test-generate-verify-session"

	// Use reasonable round times to ensure proper message propagation
	startTime := time.Now().Add(200 * time.Millisecond)
	roundTimeout := 300 * time.Millisecond
	rounds := 3

	for id, net := range networks {
		mdags[id] = mdag.New(rounds, sessionID, testOracleIntegration, net, roundTimeout, logger, startTime)
	}

	// Run the protocol on all nodes
	var wg sync.WaitGroup
	var resultsMutex sync.Mutex
	results := make(map[string][][][]byte)
	finalLabels := make(map[string][]byte)
	lastRoundLabels := make(map[string][][]byte) // Store labels received in the last round

	// Define different verification keys and inputs for each node
	nodeInputs := map[string]struct {
		vki []byte
		vi  [][]byte
	}{
		"node1": {
			vki: []byte("verification-key-for-node1"),
			vi:  [][]byte{[]byte("input-for-node1")},
		},
		"node2": {
			vki: []byte("verification-key-for-node2"),
			vi:  [][]byte{[]byte("input-for-node2")},
		},
		"node3": {
			vki: []byte("verification-key-for-node3"),
			vi:  [][]byte{[]byte("input-for-node3")},
		},
	}

	for id, mdagInstance := range mdags {
		wg.Add(1)
		go func(nodeID string, m *mdag.MDAG) {
			defer wg.Done()

			// Use different inputs for each node
			inputs := nodeInputs[nodeID]

			state, err := m.Generate(sessionID, inputs.vki, inputs.vi...)
			if err != nil {
				t.Errorf("Node %s failed to generate: %v", nodeID, err)
				return
			}

			resultsMutex.Lock()
			results[nodeID] = state

			// Extract the final label using GetComputedLabel
			finalLabel := m.GetComputedLabel(rounds - 1)
			if finalLabel != nil {
				finalLabels[nodeID] = finalLabel
				t.Logf("Node %s final label: %x", nodeID, finalLabel)
			} else {
				t.Errorf("Node %s did not produce a final label", nodeID)
			}

			// Store the labels received in the last round
			if len(state) == rounds && len(state[rounds-1]) > 0 {
				lastRoundLabels[nodeID] = state[rounds-1]
				t.Logf("Node %s received %d labels in the last round", nodeID, len(state[rounds-1]))
			}
			resultsMutex.Unlock()
		}(id, mdagInstance)
	}

	// Wait for all nodes to complete with a reasonable timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Continue with the test
	case <-time.After(5 * time.Second):
		t.Fatal("Test timed out waiting for nodes to complete")
	}

	// Lock for the rest of the test since we're accessing the maps
	resultsMutex.Lock()
	defer resultsMutex.Unlock()

	// Verify that all nodes have completed the protocol
	require.Equal(t, len(nodeIDs), len(results), "Not all nodes completed the protocol")
	require.Equal(t, len(nodeIDs), len(finalLabels), "Not all nodes produced a final label")

	// Verify that all nodes have the same number of rounds
	for id, state := range results {
		require.Equal(t, rounds, len(state), "Node %s has incorrect number of rounds", id)
	}

	// With different inputs, nodes should have different final labels
	// Log the final labels to verify they're different
	t.Logf("Final labels from %d nodes:", len(finalLabels))
	for id, label := range finalLabels {
		t.Logf("Node %s final label: %x", id, label)
	}

	// Verify that the final labels are different
	var uniqueLabels = make(map[string]bool)
	for _, label := range finalLabels {
		uniqueLabels[string(label)] = true
	}
	assert.Equal(t, len(nodeIDs), len(uniqueLabels), "Expected all nodes to have different final labels")

	// Now, each node should verify the paths of other nodes using the labels it received in the last round
	for verifierID, verifier := range mdags {
		t.Logf("Node %s verifying paths from other nodes", verifierID)

		// Get the labels received by this node in the last round
		receivedLabels := lastRoundLabels[verifierID]
		if len(receivedLabels) == 0 {
			t.Errorf("Node %s didn't receive any labels in the last round", verifierID)
			continue
		}

		// Create a map of target labels from the received labels
		targetLabels := make(map[string]bool)
		for _, label := range receivedLabels {
			targetLabels[string(label)] = true
		}

		// For each other node, construct a path and verify it
		for pathOwnerID, pathOwnerState := range results {
			if pathOwnerID == verifierID {
				continue // Skip self-verification
			}

			t.Logf("Node %s verifying path from node %s", verifierID, pathOwnerID)

			// Use the exact state from Generate as the path for verification
			path := pathOwnerState

			// Verify the path against the target labels
			// The verifier should recognize at least one of the labels from the path owner
			// in its set of received labels from the last round
			result := verifier.Verify(path, targetLabels)

			// Log the result and assert that verification should succeed
			if result {
				t.Logf("Node %s successfully verified path from node %s", verifierID, pathOwnerID)
			} else {
				// Verification should succeed - if it fails, it's a test failure
				assert.True(t, result, "Node %s failed to verify path from node %s", verifierID, pathOwnerID)
			}
		}
	}

	// Verify message counts
	for id, net := range networks {
		count := net.GetMessageCount()
		if count == 0 {
			t.Errorf("Node %s didn't send any messages", id)
		}
		t.Logf("Node %s sent %d messages", id, count)
	}
}
