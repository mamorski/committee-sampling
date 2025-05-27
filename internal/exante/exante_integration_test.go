package exante

import (
	"crypto/sha256"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/mdag"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// InMemoryNetwork implements network.Network for integration testing
type InMemoryNetwork struct {
	nodeID         string
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
	from       string
}

func NewInMemoryNetwork(nodeID string, neighbors []string) *InMemoryNetwork {
	n := &InMemoryNetwork{
		nodeID:      nodeID,
		handlers:    make(map[string]network.MessageHandler),
		peers:       make(map[string]*InMemoryNetwork),
		neighbors:   neighbors,
		messageChan: make(chan networkMessage, 100), // Buffered channel
		stopChan:    make(chan struct{}),
	}

	// Start message processing goroutine
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

	for peerID, peer := range n.peers {
		if neighborSet[peerID] {
			// Send message via channel - non-blocking
			select {
			case peer.messageChan <- networkMessage{
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

func (n *InMemoryNetwork) GetNodeID() string {
	return n.nodeID
}

func (n *InMemoryNetwork) Close() error {
	close(n.stopChan)
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

// testOracle provides a simple hash oracle for testing
func testOracle(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

// testGradeFunction provides a grade function for testing
func testGradeFunction(sid string, vk []byte, ch []byte, auxKey *common.AuxKey, auxLocal float64) int {
	// Simple grade function that returns different grades based on node ID
	hash := sha256.Sum256(append(vk, ch...))
	return int(hash[0]) % 10 // Return grade 0-9
}

// testFilterFunction provides a filter function for testing
func testFilterFunction(sid string, vk []byte, ch []byte, aux *common.AuxTag) bool {
	return true // Accept all messages for testing
}

// TestExAnteIntegrationTwoNodes tests ExAnte with two nodes
func TestExAnteIntegrationTwoNodes(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// Create network nodes
	node1 := NewInMemoryNetwork("node1", []string{"node2"})
	node2 := NewInMemoryNetwork("node2", []string{"node1"})

	// Connect the nodes
	node1.AddPeer(node2)
	node2.AddPeer(node1)

	// Create MDAG instances
	mdagRounds := 5
	sessionID := "test-integration"
	mdagRoundTimeout := 200 * time.Millisecond
	mdagStartTime := time.Now().Add(300 * time.Millisecond)

	mdag1 := mdag.New(mdagRounds, sessionID, testOracle, node1, mdagRoundTimeout, logger, mdagStartTime)
	mdag2 := mdag.New(mdagRounds, sessionID, testOracle, node2, mdagRoundTimeout, logger, mdagStartTime)

	// Create ExAnte instances - start after MDAG generation completes
	exanteD := 3
	exanteBigD := 2
	exanteRoundTimeout := 200 * time.Millisecond
	// ExAnte starts after MDAG completes: mdagStartTime + (mdagRounds + 1) * mdagRoundTimeout + buffer
	exanteStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(500 * time.Millisecond)

	exante1 := New(node1, mdag1, sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, testGradeFunction, logger)
	exante2 := New(node2, mdag2, sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, testGradeFunction, logger)

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
	var result1, result2 map[common.Key]common.O
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

// TestExAnteIntegrationThreeNodes tests ExAnte with three nodes
func TestExAnteIntegrationThreeNodes(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

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
	mdagRoundTimeout := 200 * time.Millisecond
	mdagStartTime := time.Now().Add(300 * time.Millisecond)

	mdag1 := mdag.New(mdagRounds, sessionID, testOracle, node1, mdagRoundTimeout, logger, mdagStartTime)
	mdag2 := mdag.New(mdagRounds, sessionID, testOracle, node2, mdagRoundTimeout, logger, mdagStartTime)
	mdag3 := mdag.New(mdagRounds, sessionID, testOracle, node3, mdagRoundTimeout, logger, mdagStartTime)

	// Create ExAnte instances - start after MDAG generation completes
	exanteD := 2
	exanteBigD := 2
	exanteRoundTimeout := 200 * time.Millisecond
	// ExAnte starts after MDAG completes: mdagStartTime + (mdagRounds + 1) * mdagRoundTimeout + buffer
	exanteStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(500 * time.Millisecond)

	exante1 := New(node1, mdag1, sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, testGradeFunction, logger)
	exante2 := New(node2, mdag2, sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, testGradeFunction, logger)
	exante3 := New(node3, mdag3, sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, testGradeFunction, logger)

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
	var result1, result2, result3 map[common.Key]common.O
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

// TestExAnteIntegrationProverBehavior tests prover behavior in the protocol
func TestExAnteIntegrationProverBehavior(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// Create network nodes
	node1 := NewInMemoryNetwork("node1", []string{"node2"})
	node2 := NewInMemoryNetwork("node2", []string{"node1"})

	// Connect the nodes
	node1.AddPeer(node2)
	node2.AddPeer(node1)

	// Create MDAG instances
	mdagRounds := 3
	sessionID := "test-prover"
	mdagRoundTimeout := 200 * time.Millisecond
	mdagStartTime := time.Now().Add(300 * time.Millisecond)

	mdag1 := mdag.New(mdagRounds, sessionID, testOracle, node1, mdagRoundTimeout, logger, mdagStartTime)
	mdag2 := mdag.New(mdagRounds, sessionID, testOracle, node2, mdagRoundTimeout, logger, mdagStartTime)

	// Create grade function that makes node1 a prover
	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxKey *common.AuxKey, auxLocal float64) int {
		nodeID := string(vk) // Use VK to identify node
		if nodeID == "node1-vk" {
			return 5 // High grade, will be a prover (>= d+1 = 4)
		}
		return 2 // Low grade, not a prover
	}

	// Create ExAnte instances - start after MDAG generation completes
	exanteD := 3
	exanteBigD := 1
	exanteRoundTimeout := 200 * time.Millisecond
	// ExAnte starts after MDAG completes: mdagStartTime + (mdagRounds + 1) * mdagRoundTimeout + buffer
	exanteStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(500 * time.Millisecond)

	exante1 := New(node1, mdag1, sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, proverGradeFunction, logger)
	exante2 := New(node2, mdag2, sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, proverGradeFunction, logger)

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
	var result1, result2 map[common.Key]common.O
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

// TestExAnteIntegrationMessageFiltering tests message filtering behavior
func TestExAnteIntegrationMessageFiltering(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	// Create network nodes with restricted neighbors
	node1 := NewInMemoryNetwork("node1", []string{"node2"}) // Only node2 is neighbor
	node2 := NewInMemoryNetwork("node2", []string{"node1"}) // Only node1 is neighbor
	node3 := NewInMemoryNetwork("node3", []string{})        // No neighbors

	// Connect all nodes physically
	node1.AddPeer(node2)
	node1.AddPeer(node3)
	node2.AddPeer(node1)
	node2.AddPeer(node3)
	node3.AddPeer(node1)
	node3.AddPeer(node2)

	// Create MDAG instances
	mdagRounds := 3
	sessionID := "test-filtering"
	mdagRoundTimeout := 200 * time.Millisecond
	mdagStartTime := time.Now().Add(300 * time.Millisecond)

	mdag1 := mdag.New(mdagRounds, sessionID, testOracle, node1, mdagRoundTimeout, logger, mdagStartTime)
	mdag2 := mdag.New(mdagRounds, sessionID, testOracle, node2, mdagRoundTimeout, logger, mdagStartTime)
	mdag3 := mdag.New(mdagRounds, sessionID, testOracle, node3, mdagRoundTimeout, logger, mdagStartTime)

	// Create ExAnte instances - start after MDAG generation completes
	exanteD := 2
	exanteBigD := 1
	exanteRoundTimeout := 500 * time.Millisecond
	// ExAnte starts after MDAG completes: mdagStartTime + (mdagRounds + 1) * mdagRoundTimeout + buffer
	exanteStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(500 * time.Millisecond)

	exante1 := New(node1, mdag1, sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, testGradeFunction, logger)
	exante2 := New(node2, mdag2, sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, testGradeFunction, logger)
	exante3 := New(node3, mdag3, sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, testGradeFunction, logger)

	// Test data
	vk := []byte("test-vk-filtering")
	challenge := []byte("test-challenge-filtering")
	piRP := []byte("test-pi-rp-filtering")

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

	// Node1 and node2 should have each other's labels, but not node3's
	// Node3 should only have its own label
	assert.Equal(t, 2, len(state1[0]), "Node1 should have 2 labels (own + node2)")
	assert.Equal(t, 2, len(state2[0]), "Node2 should have 2 labels (own + node1)")
	assert.Equal(t, 1, len(state3[0]), "Node3 should have 1 label (own only)")
}

// TestExAnteIntegrationLargeNetwork tests ExAnte with a larger network
func TestExAnteIntegrationLargeNetwork(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large network test in short mode")
	}

	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

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
	mdagRoundTimeout := 200 * time.Millisecond
	mdagStartTime := time.Now().Add(500 * time.Millisecond)
	exanteD := 2
	exanteBigD := 1
	exanteRoundTimeout := 200 * time.Millisecond
	// ExAnte starts after MDAG completes: mdagStartTime + (mdagRounds + 1) * mdagRoundTimeout + buffer
	exanteStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(500 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		mdags[i] = mdag.New(mdagRounds, sessionID, testOracle, nodes[i], mdagRoundTimeout, logger, mdagStartTime)
		exantes[i] = New(nodes[i], mdags[i], sessionID, exanteStartTime, exanteRoundTimeout, exanteD, exanteBigD, testGradeFunction, logger)
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
