package exante

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/mdag"
	"github.com/mamorski/committee-sampling/internal/network"
	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

// InMemoryNetwork implements network.Network for integration testing
type InMemoryNetwork struct {
	nodeID         peer.ID
	handlers       map[string]network.MessageHandler
	peers          map[string]*InMemoryNetwork
	neighbors      []string
	mu             sync.RWMutex
	messageCounter int
	messageChan    chan networkMessage
	stopChan       chan struct{}
}

type networkMessage struct {
	protocolID string
	data       []byte
	from       peer.ID
}

func NewInMemoryNetwork(nodeID string, neighbors []string) *InMemoryNetwork {
	n := &InMemoryNetwork{
		nodeID:      peer.ID(nodeID),
		handlers:    make(map[string]network.MessageHandler),
		peers:       make(map[string]*InMemoryNetwork),
		neighbors:   neighbors,
		messageChan: make(chan networkMessage, 100), // Buffered channel
		stopChan:    make(chan struct{}),
	}

	// Start a message processing goroutine
	go n.processMessages()

	return n
}

func (n *InMemoryNetwork) processMessages() {
	for {
		select {
		case msg := <-n.messageChan:
			n.mu.RLock()
			handler, exists := n.handlers[msg.protocolID]
			n.mu.RUnlock()

			if exists {
				_ = handler(msg.from, msg.data)
			} else {
				fmt.Printf("DEBUG: Node %s received message for unknown protocol %s from %s\n", n.nodeID, msg.protocolID, msg.from)
			}
		case <-n.stopChan:
			return
		}
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

	n.messageCounter++

	// Send only to neighbors (not all peers)
	neighborSet := make(map[string]bool)
	for _, neighbor := range n.neighbors {
		neighborSet[neighbor] = true
	}

	for peerID, p := range n.peers {
		if neighborSet[peerID] {
			// Send message via channel - non-blocking
			select {
			case p.messageChan <- networkMessage{
				protocolID: protocolID,
				data:       data,
				from:       n.nodeID,
			}:
			default:
				// Channel full, drop message (shouldn't happen with buffered channel)
			}
		}
	}
}

func (n *InMemoryNetwork) GetNeighbors() []string {
	return n.neighbors
}

func (n *InMemoryNetwork) IsNeighbor(peerID peer.ID) bool {
	peerIDStr := string(peerID)
	for _, neighbor := range n.neighbors {
		if neighbor == peerIDStr {
			return true
		}
	}
	return false
}

func (n *InMemoryNetwork) GetNodeID() string {
	return n.nodeID.String()
}

func (n *InMemoryNetwork) Close() error {
	close(n.stopChan)
	return nil
}

func (n *InMemoryNetwork) AddPeer(peer *InMemoryNetwork) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[peer.nodeID.String()] = peer
}

func (n *InMemoryNetwork) GetMessageCount() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.messageCounter
}

// testOracle provides a simple hash oracle for testing
func testOracle(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

// testGradeFunction provides a grade function for testing
func testGradeFunction(_ string, vk []byte, ch []byte, _ *pb.AuxKeyMessage, _ float64) int {
	// Simple grade function that returns different grades based on node ID
	hash := sha256.Sum256(append(vk, ch...))
	return int(hash[0]) % 10 // Return grade 0-9
}

// testFilterFunction provides a filter function for testing
func testFilterFunction(_, _ string, _ []byte, _ []byte, _ *pb.Aux) bool {
	return true // Accept all messages for testing
}

//nolint:funlen
func TestExAnteIntegrationTwoNodes(t *testing.T) {
	logger := zap.NewNop()

	// Create network nodes
	node1 := NewInMemoryNetwork("node1", []string{"node2"})
	node2 := NewInMemoryNetwork("node2", []string{"node1"})

	// Connect the nodes
	node1.AddPeer(node2)
	node2.AddPeer(node1)

	// Create MDAG instances
	mdagRounds := 5
	sessionID := "test-integration"
	mdagSynchronizer := syncMock{}
	collector := &CollectorMock{}
	collector.On("AddCustomMetric", mock.Anything).Return(nil)
	mdag1 := mdag.New(mdagRounds, sessionID, testOracle, node1, mdagSynchronizer, logger, common.ExAnteMDAG, "", collector)
	mdag2 := mdag.New(mdagRounds, sessionID, testOracle, node2, mdagSynchronizer, logger, common.ExAnteMDAG, "", collector)

	// Create ExAnte instances - start after MDAG generation completes
	exanteD := 3
	exanteBigD := 2
	exanteSynchronizer := syncMock{}

	exante1 := New(node1, mdag1, sessionID, exanteSynchronizer, exanteD, exanteBigD, testGradeFunction, logger, collector)
	exante2 := New(node2, mdag2, sessionID, exanteSynchronizer, exanteD, exanteBigD, testGradeFunction, logger, collector)

	// Test data
	vk := []byte("test-verification-key")
	challenge := []byte("test-challenge")
	piRP := []byte("test-pi-rp")

	// Generate phase
	var state1, state2 [][][]byte
	var err1, err2 error

	var genWg sync.WaitGroup
	genWg.Add(2)

	go func() {
		defer genWg.Done()
		state1, err1 = exante1.Generate(sessionID, vk, challenge, piRP)
	}()

	go func() {
		defer genWg.Done()
		state2, err2 = exante2.Generate(sessionID, vk, challenge, piRP)
	}()

	// Wait for generation to complete with timeout
	genDone := make(chan struct{})
	go func() {
		genWg.Wait()
		close(genDone)
	}()

	select {
	case <-genDone:
		// Both generations completed
	case <-time.After(15 * time.Second):
		t.Fatal("Generation phase timed out")
	}

	// Check for errors
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NotNil(t, state1)
	assert.NotNil(t, state2)

	// Verify that both nodes have the same final state
	assert.Equal(t, len(state1), len(state2))

	// Create sigma for verification
	sigma := make([][][]byte, exanteD*exanteBigD+1)
	for i := range sigma {
		if i < len(state1) {
			sigma[i] = state1[i]
		} else {
			sigma[i] = [][]byte{[]byte("dummy-state")}
		}
	}

	// Create aux tag
	auxTag := &common.AuxTag{
		PiRP: piRP,
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf"),
			PiVRF:  []byte("test-pi-vrf"),
			PhiVDF: []byte("test-phi-vdf"),
			PiVDF:  []byte("test-pi-vdf"),
		},
	}

	// Verification phase
	var result1, result2 *common.Committee
	var verifyErr1, verifyErr2 error

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		result1, verifyErr1 = exante1.Verify(sessionID, vk, sigma, auxTag, 1.0, testFilterFunction)
	}()

	go func() {
		defer wg.Done()
		result2, verifyErr2 = exante2.Verify(sessionID, vk, sigma, auxTag, 1.0, testFilterFunction)
	}()

	// Wait for both verifications to complete with timeout
	verifyDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(verifyDone)
	}()

	select {
	case <-verifyDone:
		// Both verifications completed
	case <-time.After(15 * time.Second):
		t.Fatal("Verification phase timed out")
	}

	// Check for errors
	assert.NoError(t, verifyErr1)
	assert.NoError(t, verifyErr2)
	assert.NotNil(t, result1)
	assert.NotNil(t, result2)

	// Verify that messages were exchanged
	assert.Greater(t, node1.GetMessageCount(), 0)
	assert.Greater(t, node2.GetMessageCount(), 0)
}

// nolint:funlen
func TestExAnteIntegrationThreeNodes(t *testing.T) {
	logger := zap.NewNop()

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
	mdagRounds := 4
	sessionID := "test-three-nodes"
	mdagSynchronizer := syncMock{}
	collector := &CollectorMock{}
	collector.On("AddCustomMetric", mock.Anything).Return(nil)
	mdag1 := mdag.New(mdagRounds, sessionID, testOracle, node1, mdagSynchronizer, logger, common.ExAnteMDAG, "", collector)
	mdag2 := mdag.New(mdagRounds, sessionID, testOracle, node2, mdagSynchronizer, logger, common.ExAnteMDAG, "", collector)
	mdag3 := mdag.New(mdagRounds, sessionID, testOracle, node3, mdagSynchronizer, logger, common.ExAnteMDAG, "", collector)

	// Create ExAnte instances - start after MDAG generation completes
	exanteD := 2
	exanteBigD := 2
	exanteSynchronizer := syncMock{}

	exante1 := New(node1, mdag1, sessionID, exanteSynchronizer, exanteD, exanteBigD, testGradeFunction, logger, collector)
	exante2 := New(node2, mdag2, sessionID, exanteSynchronizer, exanteD, exanteBigD, testGradeFunction, logger, collector)
	exante3 := New(node3, mdag3, sessionID, exanteSynchronizer, exanteD, exanteBigD, testGradeFunction, logger, collector)

	// Test data
	vk := []byte("test-vk-three-nodes")
	challenge := []byte("test-challenge-three")
	piRP := []byte("test-pi-rp-three")

	// Generate phase
	var state1, state2, state3 [][][]byte
	var err1, err2, err3 error

	var genWg sync.WaitGroup
	genWg.Add(3)

	go func() {
		defer genWg.Done()
		state1, err1 = exante1.Generate(sessionID, vk, challenge, piRP)
	}()

	go func() {
		defer genWg.Done()
		state2, err2 = exante2.Generate(sessionID, vk, challenge, piRP)
	}()

	go func() {
		defer genWg.Done()
		state3, err3 = exante3.Generate(sessionID, vk, challenge, piRP)
	}()

	// Wait for generation to complete with timeout
	genDone := make(chan struct{})
	go func() {
		genWg.Wait()
		close(genDone)
	}()

	select {
	case <-genDone:
		// All generations completed
	case <-time.After(15 * time.Second):
		t.Fatal("Generation phase timed out")
	}

	// Check for errors
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NoError(t, err3)
	assert.NotNil(t, state1)
	assert.NotNil(t, state2)
	assert.NotNil(t, state3)

	// Verify that all nodes have consistent states
	assert.Equal(t, len(state1), len(state2))
	assert.Equal(t, len(state2), len(state3))

	// Create sigma for verification
	sigma := make([][][]byte, exanteD*exanteBigD+1)
	for i := range sigma {
		if i < len(state1) {
			sigma[i] = state1[i]
		} else {
			sigma[i] = [][]byte{[]byte("dummy-state")}
		}
	}

	// Create aux tag
	auxTag := &common.AuxTag{
		PiRP: piRP,
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf-three"),
			PiVRF:  []byte("test-pi-vrf-three"),
			PhiVDF: []byte("test-phi-vdf-three"),
			PiVDF:  []byte("test-pi-vdf-three"),
		},
	}

	// Verification phase
	var result1, result2, result3 *common.Committee
	var verifyErr1, verifyErr2, verifyErr3 error

	var verifyWg sync.WaitGroup
	verifyWg.Add(3)

	go func() {
		defer verifyWg.Done()
		result1, verifyErr1 = exante1.Verify(sessionID, vk, sigma, auxTag, 1.0, testFilterFunction)
	}()

	go func() {
		defer verifyWg.Done()
		result2, verifyErr2 = exante2.Verify(sessionID, vk, sigma, auxTag, 1.0, testFilterFunction)
	}()

	go func() {
		defer verifyWg.Done()
		result3, verifyErr3 = exante3.Verify(sessionID, vk, sigma, auxTag, 1.0, testFilterFunction)
	}()

	// Wait for verification to complete with timeout
	verifyDone := make(chan struct{})
	go func() {
		verifyWg.Wait()
		close(verifyDone)
	}()

	select {
	case <-verifyDone:
		// All verifications completed
	case <-time.After(15 * time.Second):
		t.Fatal("Verification phase timed out")
	}

	// Check for errors
	assert.NoError(t, verifyErr1)
	assert.NoError(t, verifyErr2)
	assert.NoError(t, verifyErr3)
	assert.NotNil(t, result1)
	assert.NotNil(t, result2)
	assert.NotNil(t, result3)

	// Verify that messages were exchanged
	assert.Greater(t, node1.GetMessageCount(), 0)
	assert.Greater(t, node2.GetMessageCount(), 0)
	assert.Greater(t, node3.GetMessageCount(), 0)
}

// nolint:funlen
func TestExAnteIntegrationProverBehavior(t *testing.T) {
	logger := zap.NewNop()
	collector := &CollectorMock{}

	// Create network nodes
	node1 := NewInMemoryNetwork("node1", []string{"node2"})
	node2 := NewInMemoryNetwork("node2", []string{"node1"})

	// Connect the nodes
	node1.AddPeer(node2)
	node2.AddPeer(node1)

	// Create MDAG instances
	mdagRounds := 3
	sessionID := "test-prover"
	mdagSynchronizer := syncMock{}
	collector.On("AddCustomMetric", mock.Anything).Return(nil)
	mdag1 := mdag.New(mdagRounds, sessionID, testOracle, node1, mdagSynchronizer, logger, common.ExAnteMDAG, "", collector)
	mdag2 := mdag.New(mdagRounds, sessionID, testOracle, node2, mdagSynchronizer, logger, common.ExAnteMDAG, "", collector)

	// Create grade function that makes node1 a prover
	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxKey *pb.AuxKeyMessage, auxLocal float64) int {
		nodeID := string(vk) // Use VK to identify node
		if nodeID == "node1-vk" {
			return 5 // High grade, will be a prover (>= d+1 = 4)
		}
		return 2 // Low grade, not a prover
	}

	// Create ExAnte instances - start after MDAG generation completes
	exanteD := 3
	exanteBigD := 1
	exanteSynchronizer := syncMock{}
	collector.On("AddCustomMetric", mock.Anything).Return(nil)

	exante1 := New(node1, mdag1, sessionID, exanteSynchronizer, exanteD, exanteBigD, proverGradeFunction, logger, collector)
	exante2 := New(node2, mdag2, sessionID, exanteSynchronizer, exanteD, exanteBigD, proverGradeFunction, logger, collector)

	// Test data
	vk1 := []byte("node1-vk")
	vk2 := []byte("node2-vk")
	challenge := []byte("test-challenge-prover")
	piRP := []byte("test-pi-rp-prover")

	// Generate phase
	var state1, state2 [][][]byte
	var err1, err2 error

	var genWg sync.WaitGroup
	genWg.Add(2)

	go func() {
		defer genWg.Done()
		state1, err1 = exante1.Generate(sessionID, vk1, challenge, piRP)
	}()

	go func() {
		defer genWg.Done()
		state2, err2 = exante2.Generate(sessionID, vk2, challenge, piRP)
	}()

	// Wait for generation to complete with timeout
	genDone := make(chan struct{})
	go func() {
		genWg.Wait()
		close(genDone)
	}()

	select {
	case <-genDone:
		// Both generations completed
	case <-time.After(15 * time.Second):
		t.Fatal("Generation phase timed out")
	}

	// Check for errors
	assert.NoError(t, err1)
	assert.NoError(t, err2)
	assert.NotNil(t, state1)
	assert.NotNil(t, state2)

	// Create sigma for verification
	sigma := make([][][]byte, exanteD*exanteBigD+1)
	for i := range sigma {
		if i < len(state1) {
			sigma[i] = state1[i]
		} else {
			sigma[i] = [][]byte{[]byte("dummy-state")}
		}
	}

	// Create aux tag
	auxTag := &common.AuxTag{
		PiRP: piRP,
		AuxKey: &common.AuxKey{
			PhiVRF: []byte("test-phi-vrf-prover"),
			PiVRF:  []byte("test-pi-vrf-prover"),
			PhiVDF: []byte("test-phi-vdf-prover"),
			PiVDF:  []byte("test-pi-vdf-prover"),
		},
	}

	// Reset message counters
	node1.messageCounter = 0
	node2.messageCounter = 0

	// Verification phase - node1 should act as prover
	var result1, result2 *common.Committee
	var verifyErr1, verifyErr2 error

	var verifyWg sync.WaitGroup
	verifyWg.Add(2)

	go func() {
		defer verifyWg.Done()
		result1, verifyErr1 = exante1.Verify(sessionID, vk1, sigma, auxTag, 1.0, testFilterFunction)
	}()

	go func() {
		defer verifyWg.Done()
		result2, verifyErr2 = exante2.Verify(sessionID, vk2, sigma, auxTag, 1.0, testFilterFunction)
	}()

	// Wait for verification to complete with timeout
	verifyDone := make(chan struct{})
	go func() {
		verifyWg.Wait()
		close(verifyDone)
	}()

	select {
	case <-verifyDone:
		// Both verifications completed
	case <-time.After(15 * time.Second):
		t.Fatal("Verification phase timed out")
	}

	// Check for errors
	assert.NoError(t, verifyErr1)
	assert.NoError(t, verifyErr2)
	assert.NotNil(t, result1)
	assert.NotNil(t, result2)

	// Node1 should have sent initial message as prover
	assert.Greater(t, node1.GetMessageCount(), 0, "Node1 should send messages as prover")
}

// TestExAnteIntegrationLargeNetwork tests ExAnte with a larger network
func TestExAnteIntegrationLargeNetwork(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large network test in short mode")
	}

	logger := zap.NewNop()

	nodeCount := 5
	nodes := make([]*InMemoryNetwork, nodeCount)
	mdags := make([]*mdag.MDAG, nodeCount)
	exantes := make([]*ExAnte, nodeCount)

	// Create network nodes
	for i := 0; i < nodeCount; i++ {
		neighbors := make([]string, 0, nodeCount-1)
		for j := 0; j < nodeCount; j++ {
			if i != j {
				neighbors = append(neighbors, fmt.Sprintf("node%d", j))
			}
		}
		nodes[i] = NewInMemoryNetwork(fmt.Sprintf("node%d", i), neighbors)
	}

	// Connect all nodes to each other
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j {
				nodes[i].AddPeer(nodes[j])
			}
		}
	}

	// Create MDAG and ExAnte instances
	mdagRounds := 3
	sessionID := "test-large-network"
	mdagSynchronizer := syncMock{}
	exanteD := 2
	exanteBigD := 1
	exanteSynchronizer := syncMock{}
	collector := &CollectorMock{}
	collector.On("AddCustomMetric", mock.Anything).Return(nil)

	for i := 0; i < nodeCount; i++ {
		mdags[i] = mdag.New(mdagRounds, sessionID, testOracle, nodes[i], mdagSynchronizer, logger, common.ExAnteMDAG, "", collector)
		exantes[i] = New(nodes[i], mdags[i], sessionID, exanteSynchronizer, exanteD, exanteBigD, testGradeFunction, logger, collector)
	}

	// Test data
	vk := []byte("test-vk-large")
	challenge := []byte("test-challenge-large")
	piRP := []byte("test-pi-rp-large")

	// Generate phase
	states := make([][][][]byte, nodeCount)
	errors := make([]error, nodeCount)

	var genWg sync.WaitGroup
	genWg.Add(nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer genWg.Done()
			states[idx], errors[idx] = exantes[idx].Generate(sessionID, vk, challenge, piRP)
		}(i)
	}

	// Wait for generation to complete with timeout
	genDone := make(chan struct{})
	go func() {
		genWg.Wait()
		close(genDone)
	}()

	select {
	case <-genDone:
		// All generations completed
	case <-time.After(20 * time.Second):
		t.Fatal("Generation phase timed out")
	}

	// Check for errors
	for i := 0; i < nodeCount; i++ {
		assert.NoError(t, errors[i], "Node %d should not have errors", i)
		assert.NotNil(t, states[i], "Node %d should have state", i)
	}

	// Verify that all nodes have consistent final states
	for i := 1; i < nodeCount; i++ {
		assert.Equal(t, len(states[0]), len(states[i]), "All nodes should have same state length")
	}

	// Verify that messages were exchanged
	for i := 0; i < nodeCount; i++ {
		assert.Greater(t, nodes[i].GetMessageCount(), 0, "Node %d should have sent messages", i)
	}
}
