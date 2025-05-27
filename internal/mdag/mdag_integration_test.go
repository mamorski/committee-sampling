package mdag

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"bytes"

	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
)

// ChaosNetwork extends InMemoryNetwork with chaos engineering capabilities
type ChaosNetwork struct {
	*InMemoryNetwork
	dropRate     float64
	partitioned  bool
	allowedPeers map[string]bool
	delayRange   time.Duration
	chaosEnabled bool
	rand         *rand.Rand
}

func NewChaosNetwork(nodeID string, neighbors []string) *ChaosNetwork {
	return &ChaosNetwork{
		InMemoryNetwork: NewInMemoryNetwork(nodeID, neighbors),
		dropRate:        0.0,
		partitioned:     false,
		allowedPeers:    make(map[string]bool),
		delayRange:      0,
		chaosEnabled:    false,
		rand:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (n *ChaosNetwork) EnableChaos() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.chaosEnabled = true
}

func (n *ChaosNetwork) DisableChaos() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.chaosEnabled = false
}

func (n *ChaosNetwork) SetDropRate(rate float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dropRate = rate
}

func (n *ChaosNetwork) SetNetworkDelay(maxDelay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.delayRange = maxDelay
}

func (n *ChaosNetwork) CreatePartition(allowedPeers []string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned = true
	n.allowedPeers = make(map[string]bool)
	for _, peer := range allowedPeers {
		n.allowedPeers[peer] = true
	}
}

func (n *ChaosNetwork) RemovePartition() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.partitioned = false
	n.allowedPeers = make(map[string]bool)
}

func (n *ChaosNetwork) SendProtocolMessage(protocolID string, data []byte) {
	n.mu.RLock()
	chaosEnabled := n.chaosEnabled
	dropRate := n.dropRate
	partitioned := n.partitioned
	allowedPeers := make(map[string]bool)
	for k, v := range n.allowedPeers {
		allowedPeers[k] = v
	}
	delayRange := n.delayRange
	n.mu.RUnlock()

	if !chaosEnabled {
		n.InMemoryNetwork.SendProtocolMessage(protocolID, data)
		return
	}

	n.mu.Lock()
	n.messageCounter++
	n.mu.Unlock()

	// Send to filtered peers based on chaos conditions
	n.mu.RLock()
	defer n.mu.RUnlock()

	for peerID, peer := range n.peers {
		// Check partition constraints
		if partitioned && !allowedPeers[peerID] {
			continue
		}

		// Simulate message drops
		if n.rand.Float64() < dropRate {
			continue
		}

		// Simulate network delays
		delay := time.Duration(0)
		if delayRange > 0 {
			delay = time.Duration(n.rand.Int63n(int64(delayRange)))
		}

		go func(p *InMemoryNetwork, d time.Duration) {
			if d > 0 {
				time.Sleep(d)
			}

			p.mu.RLock()
			handler, exists := p.handlers[protocolID]
			p.mu.RUnlock()

			if exists {
				handler(n.nodeID, data)
			}
		}(peer, delay)
	}
}

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

// IntegrationTestSuite defines the test suite for MDAG integration tests
type IntegrationTestSuite struct {
	suite.Suite
	logger *zap.Logger
}

// SetupTest runs before each test
func (suite *IntegrationTestSuite) SetupTest() {
	var err error
	suite.logger, err = zap.NewDevelopment()
	suite.Require().NoError(err)
}

// NetworkTopology represents a network configuration for testing
type NetworkTopology struct {
	nodes []*InMemoryNetwork
	mdags []*MDAG
}

// ChaosTopology represents a chaos-enabled network configuration
type ChaosTopology struct {
	nodes []*ChaosNetwork
	mdags []*MDAG
}

// createFullyConnectedNetwork creates a fully connected network with n nodes
func (suite *IntegrationTestSuite) createFullyConnectedNetwork(nodeCount int) *NetworkTopology {
	// Create node IDs
	nodeIDs := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeIDs[i] = fmt.Sprintf("node%d", i+1)
	}

	// Create neighbor lists (all nodes are neighbors of each other)
	neighbors := make([][]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		neighbors[i] = make([]string, 0, nodeCount-1)
		for j := 0; j < nodeCount; j++ {
			if i != j {
				neighbors[i] = append(neighbors[i], nodeIDs[j])
			}
		}
	}

	// Create network nodes
	nodes := make([]*InMemoryNetwork, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes[i] = NewInMemoryNetwork(nodeIDs[i], neighbors[i])
	}

	// Connect all nodes to each other
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j {
				nodes[i].AddPeer(nodes[j])
			}
		}
	}

	return &NetworkTopology{nodes: nodes}
}

// createChaosNetwork creates a chaos-enabled fully connected network
func (suite *IntegrationTestSuite) createChaosNetwork(nodeCount int) *ChaosTopology {
	// Create node IDs
	nodeIDs := make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeIDs[i] = fmt.Sprintf("node%d", i+1)
	}

	// Create neighbor lists (all nodes are neighbors of each other)
	neighbors := make([][]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		neighbors[i] = make([]string, 0, nodeCount-1)
		for j := 0; j < nodeCount; j++ {
			if i != j {
				neighbors[i] = append(neighbors[i], nodeIDs[j])
			}
		}
	}

	// Create chaos network nodes
	nodes := make([]*ChaosNetwork, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodes[i] = NewChaosNetwork(nodeIDs[i], neighbors[i])
	}

	// Connect all nodes to each other
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j {
				nodes[i].AddPeer(nodes[j].InMemoryNetwork)
			}
		}
	}

	return &ChaosTopology{nodes: nodes}
}

// createMDAGInstances creates MDAG instances for all nodes in the topology
func (suite *IntegrationTestSuite) createMDAGInstances(topology *NetworkTopology, rounds int, sessionID string) {
	roundTimeout := 100 * time.Millisecond
	startTime := time.Now().Add(200 * time.Millisecond)

	topology.mdags = make([]*MDAG, len(topology.nodes))
	for i, node := range topology.nodes {
		topology.mdags[i] = New(rounds, sessionID, testOracleIntegration, node, roundTimeout, suite.logger, startTime)
	}
}

// createChaosMDAGInstances creates MDAG instances for chaos topology
func (suite *IntegrationTestSuite) createChaosMDAGInstances(topology *ChaosTopology, rounds int, sessionID string) {
	roundTimeout := 150 * time.Millisecond // Longer timeout for chaos conditions
	startTime := time.Now().Add(300 * time.Millisecond)

	topology.mdags = make([]*MDAG, len(topology.nodes))
	for i, node := range topology.nodes {
		topology.mdags[i] = New(rounds, sessionID, testOracleIntegration, node, roundTimeout, suite.logger, startTime)
	}
}

// runProtocolOnAllNodes executes the MDAG protocol on all nodes concurrently
func (suite *IntegrationTestSuite) runProtocolOnAllNodes(topology *NetworkTopology, sessionID string, vki []byte) ([][][][]byte, []error) {
	nodeCount := len(topology.mdags)
	done := make(chan int, nodeCount)
	states := make([][][][]byte, nodeCount)
	errors := make([]error, nodeCount)

	// Start protocol on all nodes
	for i, mdag := range topology.mdags {
		go func(idx int, m *MDAG) {
			nodeInput := []byte(fmt.Sprintf("node%d", idx+1))
			states[idx], errors[idx] = m.Generate(sessionID, vki, nodeInput)
			done <- idx
		}(i, mdag)
	}

	// Wait for all nodes to complete
	for i := 0; i < nodeCount; i++ {
		select {
		case <-done:
			// Node completed
		case <-time.After(3 * time.Second):
			suite.Fail("Protocol timed out")
		}
	}

	return states, errors
}

// runChaosProtocol executes the MDAG protocol on chaos topology
func (suite *IntegrationTestSuite) runChaosProtocol(topology *ChaosTopology, sessionID string, vki []byte, timeout time.Duration) ([][][][]byte, []error) {
	nodeCount := len(topology.mdags)
	done := make(chan int, nodeCount)
	states := make([][][][]byte, nodeCount)
	errors := make([]error, nodeCount)

	// Start protocol on all nodes
	for i, mdag := range topology.mdags {
		go func(idx int, m *MDAG) {
			nodeInput := []byte(fmt.Sprintf("node%d", idx+1))
			states[idx], errors[idx] = m.Generate(sessionID, vki, nodeInput)
			done <- idx
		}(i, mdag)
	}

	// Wait for all nodes to complete or timeout
	completed := 0
	for completed < nodeCount {
		select {
		case <-done:
			completed++
		case <-time.After(timeout):
			suite.T().Logf("Protocol timed out with %d/%d nodes completed", completed, nodeCount)
			return states, errors
		}
	}

	return states, errors
}

// TestThreeNodeIntegration tests the MDAG protocol with three fully connected nodes
func (suite *IntegrationTestSuite) TestThreeNodeIntegration() {
	topology := suite.createFullyConnectedNetwork(3)
	suite.createMDAGInstances(topology, 3, "test-3-nodes")

	states, errors := suite.runProtocolOnAllNodes(topology, "test-3-nodes", []byte("vki"))

	// Verify no errors occurred
	for i, err := range errors {
		suite.NoError(err, "Node %d should not have errors", i+1)
	}

	// Verify states were populated
	for i, state := range states {
		suite.NotNil(state, "Node %d state should not be nil", i+1)
		suite.Greater(len(state), 0, "Node %d state should not be empty", i+1)
	}

	// Verify message exchange
	for i, node := range topology.nodes {
		suite.Greater(node.GetMessageCount(), 0, "Node %d should have sent messages", i+1)
	}

	// Verify final labels match across all nodes
	finalLabels := make([][]byte, len(topology.mdags))
	for i, mdag := range topology.mdags {
		finalLabels[i] = mdag.GetComputedLabel(2) // rounds-1
		suite.NotNil(finalLabels[i], "Node %d final label should not be nil", i+1)
	}

	for i := 1; i < len(finalLabels); i++ {
		suite.Equal(finalLabels[0], finalLabels[i], "All nodes should have the same final label")
	}
}

// TestFiveNodeIntegration tests the MDAG protocol with five fully connected nodes
func (suite *IntegrationTestSuite) TestFiveNodeIntegration() {
	topology := suite.createFullyConnectedNetwork(5)
	suite.createMDAGInstances(topology, 2, "test-5-nodes")

	states, errors := suite.runProtocolOnAllNodes(topology, "test-5-nodes", []byte("vki"))

	// Verify no errors occurred
	for i, err := range errors {
		suite.NoError(err, "Node %d should not have errors", i+1)
	}

	// Verify final labels match across all nodes
	finalLabels := make([][]byte, len(topology.mdags))
	for i, mdag := range topology.mdags {
		finalLabels[i] = mdag.GetComputedLabel(1) // rounds-1
		suite.NotNil(finalLabels[i], "Node %d final label should not be nil", i+1)
	}

	for i := 1; i < len(finalLabels); i++ {
		suite.Equal(finalLabels[0], finalLabels[i], "All nodes should converge to the same final label")
	}

	// Verify state structure
	for i, state := range states {
		suite.Equal(2, len(state), "Node %d should have 2 rounds of state", i+1)
		suite.Equal(5, len(state[0]), "Node %d should have 5 labels in first round", i+1)
	}
}

// TestNeighborFiltering tests that nodes only accept messages from known neighbors
func (suite *IntegrationTestSuite) TestNeighborFiltering() {
	// Create nodes with restricted neighbor lists
	node1 := NewInMemoryNetwork("node1", []string{"node2"})
	node2 := NewInMemoryNetwork("node2", []string{"node1"})
	node3 := NewInMemoryNetwork("node3", []string{}) // No neighbors

	// Connect all nodes physically
	node1.AddPeer(node2)
	node1.AddPeer(node3)
	node2.AddPeer(node1)
	node2.AddPeer(node3)
	node3.AddPeer(node1)
	node3.AddPeer(node2)

	topology := &NetworkTopology{nodes: []*InMemoryNetwork{node1, node2, node3}}
	suite.createMDAGInstances(topology, 2, "test-filtering")

	states, errors := suite.runProtocolOnAllNodes(topology, "test-filtering", []byte("vki"))

	// Verify no errors occurred
	for i, err := range errors {
		suite.NoError(err, "Node %d should not have errors", i+1)
	}

	// Verify message exchange patterns
	suite.Greater(node1.GetMessageCount(), 0, "Node1 should send messages")
	suite.Greater(node2.GetMessageCount(), 0, "Node2 should send messages")
	suite.Greater(node3.GetMessageCount(), 0, "Node3 should send messages")

	// Verify filtering behavior in state
	suite.Equal(2, len(states[0][0]), "Node1 should have own + node2's label")
	suite.Equal(2, len(states[1][0]), "Node2 should have own + node1's label")
	suite.Equal(1, len(states[2][0]), "Node3 should have only own label")
}

// TestPartiallyConnectedNetwork tests a network where not all nodes are connected
func (suite *IntegrationTestSuite) TestPartiallyConnectedNetwork() {
	// Create a chain topology: node1 <-> node2 <-> node3
	node1 := NewInMemoryNetwork("node1", []string{"node2"})
	node2 := NewInMemoryNetwork("node2", []string{"node1", "node3"})
	node3 := NewInMemoryNetwork("node3", []string{"node2"})

	// Connect according to topology
	node1.AddPeer(node2)
	node2.AddPeer(node1)
	node2.AddPeer(node3)
	node3.AddPeer(node2)

	topology := &NetworkTopology{nodes: []*InMemoryNetwork{node1, node2, node3}}
	suite.createMDAGInstances(topology, 3, "test-chain")

	states, errors := suite.runProtocolOnAllNodes(topology, "test-chain", []byte("vki"))

	// Verify no errors occurred
	for i, err := range errors {
		suite.NoError(err, "Node %d should not have errors", i+1)
	}

	// Verify state structure reflects topology
	suite.Equal(2, len(states[0][0]), "Node1 should have 2 labels (own + node2)")
	suite.Equal(3, len(states[1][0]), "Node2 should have 3 labels (own + node1 + node3)")
	suite.Equal(2, len(states[2][0]), "Node3 should have 2 labels (own + node2)")

	// Node2 should be the most active (central node)
	suite.GreaterOrEqual(node2.GetMessageCount(), node1.GetMessageCount())
	suite.GreaterOrEqual(node2.GetMessageCount(), node3.GetMessageCount())
}

// TestChaosMessageDrops tests protocol resilience with message drops
func (suite *IntegrationTestSuite) TestChaosMessageDrops() {
	topology := suite.createChaosNetwork(4)
	suite.createChaosMDAGInstances(topology, 3, "test-chaos-drops")

	// Enable chaos with 20% message drop rate
	for _, node := range topology.nodes {
		node.EnableChaos()
		node.SetDropRate(0.2)
	}

	_, errors := suite.runChaosProtocol(topology, "test-chaos-drops", []byte("vki"), 5*time.Second)

	// Some nodes might have errors due to message drops, but protocol should still progress
	successfulNodes := 0
	for i, err := range errors {
		if err == nil {
			successfulNodes++
		} else {
			suite.T().Logf("Node %d had error (expected with chaos): %v", i+1, err)
		}
	}

	// At least some nodes should complete successfully
	suite.Greater(successfulNodes, 0, "At least some nodes should complete successfully despite message drops")

	// Verify message counters show activity
	totalMessages := 0
	for _, node := range topology.nodes {
		totalMessages += node.GetMessageCount()
	}
	suite.Greater(totalMessages, 0, "Network should show message activity")
}

// TestChaosNetworkPartition tests protocol behavior during network partitions
func (suite *IntegrationTestSuite) TestChaosNetworkPartition() {
	topology := suite.createChaosNetwork(6)
	suite.createChaosMDAGInstances(topology, 2, "test-partition")

	// Enable chaos and create two partitions: {node1, node2, node3} and {node4, node5, node6}
	partition1 := []string{"node1", "node2", "node3"}
	partition2 := []string{"node4", "node5", "node6"}

	for i, node := range topology.nodes {
		node.EnableChaos()
		if i < 3 {
			node.CreatePartition(partition1)
		} else {
			node.CreatePartition(partition2)
		}
	}

	_, errors := suite.runChaosProtocol(topology, "test-partition", []byte("vki"), 4*time.Second)

	// Nodes within each partition should succeed
	partition1Success := 0
	partition2Success := 0

	for i, err := range errors {
		if err == nil {
			if i < 3 {
				partition1Success++
			} else {
				partition2Success++
			}
		}
	}

	suite.Greater(partition1Success, 0, "Partition 1 should have successful nodes")
	suite.Greater(partition2Success, 0, "Partition 2 should have successful nodes")

	// Verify that nodes within the same partition converge to the same label
	if partition1Success > 1 {
		partition1Labels := make([][]byte, 0)
		for i := 0; i < 3; i++ {
			if errors[i] == nil {
				label := topology.mdags[i].GetComputedLabel(1)
				if label != nil {
					partition1Labels = append(partition1Labels, label)
				}
			}
		}
		if len(partition1Labels) > 1 {
			for i := 1; i < len(partition1Labels); i++ {
				suite.Equal(partition1Labels[0], partition1Labels[i], "Nodes in partition 1 should converge")
			}
		}
	}
}

// TestChaosNetworkDelay tests protocol behavior with network delays
func (suite *IntegrationTestSuite) TestChaosNetworkDelay() {
	topology := suite.createChaosNetwork(3)
	suite.createChaosMDAGInstances(topology, 2, "test-delays")

	// Enable chaos with random delays up to 50ms
	for _, node := range topology.nodes {
		node.EnableChaos()
		node.SetNetworkDelay(50 * time.Millisecond)
	}

	_, errors := suite.runChaosProtocol(topology, "test-delays", []byte("vki"), 6*time.Second)

	// All nodes should eventually succeed despite delays
	for i, err := range errors {
		suite.NoError(err, "Node %d should succeed despite network delays", i+1)
	}

	// Verify convergence
	finalLabels := make([][]byte, len(topology.mdags))
	for i, mdag := range topology.mdags {
		finalLabels[i] = mdag.GetComputedLabel(1)
		suite.NotNil(finalLabels[i], "Node %d should have final label", i+1)
	}

	for i := 1; i < len(finalLabels); i++ {
		suite.Equal(finalLabels[0], finalLabels[i], "All nodes should converge despite delays")
	}
}

// TestChaosPartitionHealing tests protocol recovery after partition healing
func (suite *IntegrationTestSuite) TestChaosPartitionHealing() {
	topology := suite.createChaosNetwork(4)
	suite.createChaosMDAGInstances(topology, 4, "test-healing") // Longer protocol for healing test

	// Start with partitions
	for i, node := range topology.nodes {
		node.EnableChaos()
		if i < 2 {
			node.CreatePartition([]string{"node1", "node2"})
		} else {
			node.CreatePartition([]string{"node3", "node4"})
		}
	}

	// Start protocol
	done := make(chan int, len(topology.mdags))
	states := make([][][][]byte, len(topology.mdags))
	errors := make([]error, len(topology.mdags))

	for i, mdag := range topology.mdags {
		go func(idx int, m *MDAG) {
			nodeInput := []byte(fmt.Sprintf("node%d", idx+1))
			states[idx], errors[idx] = m.Generate("test-healing", []byte("vki"), nodeInput)
			done <- idx
		}(i, mdag)
	}

	// Let protocol run for a bit with partitions
	time.Sleep(500 * time.Millisecond)

	// Heal the partition
	for _, node := range topology.nodes {
		node.RemovePartition()
	}

	// Wait for completion
	completed := 0
	for completed < len(topology.mdags) {
		select {
		case <-done:
			completed++
		case <-time.After(8 * time.Second):
			suite.T().Logf("Protocol completed with %d/%d nodes", completed, len(topology.mdags))
			break
		}
	}

	// At least some nodes should complete successfully
	successfulNodes := 0
	for i, err := range errors {
		if err == nil {
			successfulNodes++
		} else {
			suite.T().Logf("Node %d error: %v", i+1, err)
		}
	}

	suite.Greater(successfulNodes, 0, "Some nodes should complete after partition healing")
}

// TestChaosCombinedFailures tests protocol with multiple failure modes
func (suite *IntegrationTestSuite) TestChaosCombinedFailures() {
	topology := suite.createChaosNetwork(5)
	suite.createChaosMDAGInstances(topology, 3, "test-combined")

	// Enable multiple chaos conditions
	for i, node := range topology.nodes {
		node.EnableChaos()
		node.SetDropRate(0.15)                      // 15% message drops
		node.SetNetworkDelay(30 * time.Millisecond) // Up to 30ms delays

		// Create asymmetric partitions for some nodes
		if i == 0 {
			node.CreatePartition([]string{"node2", "node3"}) // node1 can only talk to node2,3
		}
	}

	_, errors := suite.runChaosProtocol(topology, "test-combined", []byte("vki"), 7*time.Second)

	// Count successful completions
	successfulNodes := 0
	for i, err := range errors {
		if err == nil {
			successfulNodes++
		} else {
			suite.T().Logf("Node %d failed with combined chaos: %v", i+1, err)
		}
	}

	// Protocol should show some resilience even under combined failures
	suite.T().Logf("Successful nodes under combined chaos: %d/%d", successfulNodes, len(topology.mdags))

	// Verify that the network showed activity despite failures
	totalMessages := 0
	for _, node := range topology.nodes {
		totalMessages += node.GetMessageCount()
	}
	suite.Greater(totalMessages, 0, "Network should show activity despite combined failures")
}

// TestFullyConnectedNetworkConvergence tests that all nodes in a fully-connected network
// converge to the same final label and have valid Merkle paths
func (suite *IntegrationTestSuite) TestFullyConnectedNetworkConvergence() {
	topology := suite.createFullyConnectedNetwork(5)
	suite.createMDAGInstances(topology, 3, "test-fully-connected-convergence")

	states, errors := suite.runProtocolOnAllNodes(topology, "test-fully-connected-convergence", []byte("vki"))

	// Verify no errors occurred
	for i, err := range errors {
		suite.NoError(err, "Node %d should not have errors", i+1)
	}

	// Verify states were populated
	for i, state := range states {
		suite.NotNil(state, "Node %d state should not be nil", i+1)
		suite.Equal(3, len(state), "Node %d should have 3 rounds of state", i+1)
	}

	// In a fully-connected network, all nodes should converge to the same final label
	finalLabels := make([][]byte, len(topology.mdags))
	for i, mdag := range topology.mdags {
		finalLabels[i] = mdag.GetComputedLabel(3) // rounds
		suite.NotNil(finalLabels[i], "Node %d final label should not be nil", i+1)
	}

	// Verify convergence - all nodes should have the same final label
	for i := 1; i < len(finalLabels); i++ {
		suite.Equal(finalLabels[0], finalLabels[i],
			"All nodes in fully-connected network should converge to the same final label")
	}

	suite.T().Logf("All nodes converged to final label: %x", finalLabels[0])

	// Create Merkle path verifier
	verifier := NewMerklePathVerifier(testOracleIntegration)

	// Test Merkle path verification for each node
	for i, state := range states {
		nodeID := i + 1
		suite.T().Logf("Verifying Merkle paths for node %d", nodeID)

		// Get computed labels from MDAG instance
		computedLabels := verifier.GetComputedLabelsFromMDAG(topology.mdags[i], 3)

		// Verify the Merkle path for this node's state
		isValid := verifier.VerifyMerklePath(state, computedLabels)
		suite.True(isValid, "Node %d Merkle path should be valid", nodeID)

		suite.T().Logf("Node %d: Merkle path verified successfully", nodeID)
	}

	// Verify that all nodes have identical states (since they're fully connected)
	for i := 1; i < len(states); i++ {
		suite.Equal(len(states[0]), len(states[i]),
			"All nodes should have same number of rounds")

		for round := 0; round < len(states[0]); round++ {
			suite.Equal(len(states[0][round]), len(states[i][round]),
				"All nodes should have same number of labels in round %d", round)

			// Sort both states for comparison (they should be identical)
			state0Sorted := make([][]byte, len(states[0][round]))
			stateISorted := make([][]byte, len(states[i][round]))
			copy(state0Sorted, states[0][round])
			copy(stateISorted, states[i][round])

			sort.Slice(state0Sorted, func(a, b int) bool {
				return bytes.Compare(state0Sorted[a], state0Sorted[b]) < 0
			})
			sort.Slice(stateISorted, func(a, b int) bool {
				return bytes.Compare(stateISorted[a], stateISorted[b]) < 0
			})

			for j := 0; j < len(state0Sorted); j++ {
				suite.Equal(state0Sorted[j], stateISorted[j],
					"Node 1 and node %d should have identical labels in round %d position %d",
					i+1, round, j)
			}
		}
	}

	// Verify that computed labels are identical across all nodes for rounds 1-3
	// (Round 0 labels are different because each node has different initial input)
	for i := 1; i < len(topology.mdags); i++ {
		for round := 1; round <= 3; round++ {
			label0 := topology.mdags[0].GetComputedLabel(round)
			labelI := topology.mdags[i].GetComputedLabel(round)
			suite.Equal(label0, labelI,
				"Node 1 and node %d should have identical computed label for round %d", i+1, round)
		}
	}

	// Verify that round 0 labels are different (as expected)
	for i := 1; i < len(topology.mdags); i++ {
		label0 := topology.mdags[0].GetComputedLabel(0)
		labelI := topology.mdags[i].GetComputedLabel(0)
		suite.NotEqual(label0, labelI,
			"Node 1 and node %d should have different initial labels (round 0)", i+1)
	}

	suite.T().Log("Fully-connected network convergence test passed successfully")
}

// MerklePathVerifier provides functionality to verify Merkle paths in MDAG state
type MerklePathVerifier struct {
	oracle HashOracle
}

func NewMerklePathVerifier(oracle HashOracle) *MerklePathVerifier {
	return &MerklePathVerifier{oracle: oracle}
}

// VerifyMerklePath verifies that a Merkle path is valid for the given state and computed labels
// state is the complete MDAG state (all rounds L_0, L_1, ..., L_n)
// computedLabels are the labels computed by the MDAG for each round
// The verification checks: hash(L_i) = computedLabels[i+1] for each round i
func (v *MerklePathVerifier) VerifyMerklePath(state [][][]byte, computedLabels [][]byte) bool {
	if len(state) == 0 || len(computedLabels) == 0 {
		return false
	}

	// For each round in state, verify that hash(L_i) equals computedLabels[i+1]
	for i := 0; i < len(state); i++ {
		if len(state[i]) == 0 {
			return false
		}

		// Hash all labels in current round L_i
		var concatenated []byte
		for _, label := range state[i] {
			concatenated = append(concatenated, label...)
		}
		roundHash := v.oracle(concatenated)

		// The hash should equal the computed label for the next round
		expectedIndex := i + 1
		if expectedIndex >= len(computedLabels) || computedLabels[expectedIndex] == nil {
			return false
		}

		if !bytes.Equal(roundHash, computedLabels[expectedIndex]) {
			return false
		}
	}

	return true
}

// GetComputedLabelsFromMDAG extracts computed labels from MDAG instance
func (v *MerklePathVerifier) GetComputedLabelsFromMDAG(mdag *MDAG, rounds int) [][]byte {
	labels := make([][]byte, rounds+1)
	for i := 0; i <= rounds; i++ {
		labels[i] = mdag.GetComputedLabel(i)
	}
	return labels
}

func (v *MerklePathVerifier) isValueInState(value []byte, state [][]byte) bool {
	for _, stateValue := range state {
		if bytes.Equal(value, stateValue) {
			return true
		}
	}
	return false
}

// createConnectedNetwork creates a connected but not fully-connected 5-node network
// Topology: node1 -- node2 -- node3 -- node4 -- node5
//
//	|                   |
//	+------- node5 -----+
func (suite *IntegrationTestSuite) createConnectedNetwork() *NetworkTopology {
	// Define the topology as an adjacency list
	topology := map[string][]string{
		"node1": {"node2"},
		"node2": {"node1", "node3", "node5"},
		"node3": {"node2", "node4"},
		"node4": {"node3", "node5"},
		"node5": {"node2", "node4"},
	}

	// Create network nodes
	nodes := make([]*InMemoryNetwork, 5)
	nodeIDs := []string{"node1", "node2", "node3", "node4", "node5"}

	for i, nodeID := range nodeIDs {
		nodes[i] = NewInMemoryNetwork(nodeID, topology[nodeID])
	}

	// Connect nodes according to topology
	for i, node := range nodes {
		nodeID := nodeIDs[i]
		for _, neighborID := range topology[nodeID] {
			for j, neighborNode := range nodes {
				if nodeIDs[j] == neighborID {
					node.AddPeer(neighborNode)
					break
				}
			}
		}
	}

	return &NetworkTopology{nodes: nodes}
}

// TestConnectedNetworkWithMerkleVerification tests MDAG on a connected 5-node network
// and verifies the generated Merkle paths
func (suite *IntegrationTestSuite) TestConnectedNetworkWithMerkleVerification() {
	topology := suite.createConnectedNetwork()
	suite.createMDAGInstances(topology, 3, "test-connected-merkle")

	states, errors := suite.runProtocolOnAllNodes(topology, "test-connected-merkle", []byte("vki"))

	// Verify no errors occurred
	for i, err := range errors {
		suite.NoError(err, "Node %d should not have errors", i+1)
	}

	// Verify states were populated
	for i, state := range states {
		suite.NotNil(state, "Node %d state should not be nil", i+1)
		suite.Equal(3, len(state), "Node %d should have 3 rounds of state", i+1)
	}

	// Create Merkle path verifier
	verifier := NewMerklePathVerifier(testOracleIntegration)

	// Test Merkle path verification for each node
	for i, state := range states {
		nodeID := i + 1
		suite.T().Logf("Verifying Merkle paths for node %d", nodeID)

		// Get computed labels from MDAG instance
		computedLabels := verifier.GetComputedLabelsFromMDAG(topology.mdags[i], 3)

		// Verify the Merkle path for this node's state
		isValid := verifier.VerifyMerklePath(state, computedLabels)
		suite.True(isValid, "Node %d Merkle path should be valid", nodeID)

		suite.T().Logf("Node %d: Merkle path verified successfully", nodeID)
	}

	// In a connected but not fully-connected network, nodes may not converge to the same final label
	// because they have different neighbors and receive different sets of messages.
	// However, we can verify that the Merkle paths are valid for each node individually.

	// Log the final labels for comparison (they may be different)
	finalLabels := make([][]byte, len(topology.mdags))
	for i, mdag := range topology.mdags {
		finalLabels[i] = mdag.GetComputedLabel(3) // rounds
		suite.NotNil(finalLabels[i], "Node %d final label should not be nil", i+1)
		suite.T().Logf("Node %d final label: %x", i+1, finalLabels[i])
	}

	// Verify that each node's state is internally consistent
	for i, state := range states {
		computedLabels := verifier.GetComputedLabelsFromMDAG(topology.mdags[i], 3)
		isValid := verifier.VerifyMerklePath(state, computedLabels)
		suite.True(isValid, "Node %d should have valid internal Merkle path", i+1)
	}

	// Test network topology properties
	suite.verifyNetworkTopology(topology)
}

// verifyNetworkTopology verifies that the network has the expected connectivity
func (suite *IntegrationTestSuite) verifyNetworkTopology(topology *NetworkTopology) {
	expectedConnections := map[string]int{
		"node1": 1, // Connected to node2 only
		"node2": 3, // Connected to node1, node3, node5
		"node3": 2, // Connected to node2, node4
		"node4": 2, // Connected to node3, node5
		"node5": 2, // Connected to node2, node4
	}

	for _, node := range topology.nodes {
		nodeID := node.GetNodeID()
		actualConnections := len(node.GetNeighbors())
		expectedCount := expectedConnections[nodeID]

		suite.Equal(expectedCount, actualConnections,
			"Node %s should have %d connections, got %d", nodeID, expectedCount, actualConnections)
	}

	// Verify the network is connected but not fully connected
	totalPossibleConnections := 5 * 4 / 2 // n*(n-1)/2 for undirected graph
	actualConnections := 0
	for _, count := range expectedConnections {
		actualConnections += count
	}
	actualConnections /= 2 // Each connection counted twice

	suite.Equal(5, actualConnections, "Network should have exactly 5 connections")
	suite.Less(actualConnections, totalPossibleConnections, "Network should not be fully connected")
}

// TestMerklePathVerificationEdgeCases tests edge cases in Merkle path verification
func (suite *IntegrationTestSuite) TestMerklePathVerificationEdgeCases() {
	verifier := NewMerklePathVerifier(testOracleIntegration)

	// Test with empty state
	emptyState := [][][]byte{}
	emptyLabels := [][]byte{}
	isValid := verifier.VerifyMerklePath(emptyState, emptyLabels)
	suite.False(isValid, "Should return false for empty state")

	// Test with mismatched state and labels
	state := [][][]byte{
		{[]byte("round0")},
		{[]byte("round1")},
	}
	invalidLabels := [][]byte{[]byte("wrong")}
	isValid = verifier.VerifyMerklePath(state, invalidLabels)
	suite.False(isValid, "Should return false for mismatched labels")

	// Test with empty round in state
	stateWithEmptyRound := [][][]byte{
		{}, // Empty round
		{[]byte("round1")},
	}
	labels := [][]byte{[]byte("label0"), []byte("label1")}
	isValid = verifier.VerifyMerklePath(stateWithEmptyRound, labels)
	suite.False(isValid, "Should return false for empty round in state")
}

// TestMerklePathConsistency tests that Merkle paths are consistent across protocol runs
func (suite *IntegrationTestSuite) TestMerklePathConsistency() {
	// Run the protocol multiple times and verify path consistency
	for run := 0; run < 3; run++ {
		topology := suite.createConnectedNetwork()
		sessionID := fmt.Sprintf("test-consistency-%d", run)
		suite.createMDAGInstances(topology, 2, sessionID)

		states, errors := suite.runProtocolOnAllNodes(topology, sessionID, []byte("vki"))

		// Verify no errors
		for i, err := range errors {
			suite.NoError(err, "Run %d: Node %d should not have errors", run, i+1)
		}

		// Verify Merkle paths for this run
		verifier := NewMerklePathVerifier(testOracleIntegration)
		for i, state := range states {
			computedLabels := verifier.GetComputedLabelsFromMDAG(topology.mdags[i], 2)
			isValid := verifier.VerifyMerklePath(state, computedLabels)
			suite.True(isValid, "Run %d: Node %d Merkle path should be valid", run, i+1)
		}

		suite.T().Logf("Run %d: All Merkle paths verified successfully", run)
	}
}

// Run the integration test suite
func TestIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(IntegrationTestSuite))
}
