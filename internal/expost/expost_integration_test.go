package expost

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
		messageChan: make(chan networkMessage, 100),
		stopChan:    make(chan struct{}),
	}

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

	neighborSet := make(map[string]bool)
	for _, neighbor := range n.neighbors {
		neighborSet[neighbor] = true
	}

	for peerID, peer := range n.peers {
		if neighborSet[peerID] {
			select {
			case peer.messageChan <- networkMessage{
				protocolID: protocolID,
				data:       data,
				from:       n.nodeID,
			}:
			default:
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

func testOracle(data []byte) []byte {
	hash := sha256.Sum256(data)
	return hash[:]
}

func testGradeFunction(_ string, vk []byte, ch []byte, _ *common.AuxKey, _ float64) int {
	hash := sha256.Sum256(append(vk, ch...))
	return int(hash[0]) % 10
}

func testFilterTagFunction(_, _ string, _ []byte, _ []byte, _ *common.AuxTag) bool {
	return true
}

//nolint:funlen,gocyclo
func TestExPostIntegrationFiveNodes(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	nodeCount := 5
	nodes := make([]*InMemoryNetwork, nodeCount)
	mdags := make([]*mdag.MDAG, nodeCount)
	exposts := make([]*ExPost, nodeCount)

	// Create network topology - fully connected
	for i := 0; i < nodeCount; i++ {
		neighbors := make([]string, 0, nodeCount-1)
		for j := 0; j < nodeCount; j++ {
			if i != j {
				neighbors = append(neighbors, fmt.Sprintf("node%d", j))
			}
		}
		nodes[i] = NewInMemoryNetwork(fmt.Sprintf("node%d", i), neighbors)
	}

	// Connect all nodes
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j {
				nodes[i].AddPeer(nodes[j])
			}
		}
	}

	// Create MDAG instances
	mdagRounds := 6
	sessionID := "test-expost-five-nodes"
	mdagRoundTimeout := 200 * time.Millisecond
	mdagStartTime := time.Now().Add(300 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		mdags[i] = mdag.New(mdagRounds, sessionID, testOracle, nodes[i], mdagRoundTimeout, logger, mdagStartTime, "")
	}

	// Create ExPost instances
	expostD := 3
	expostBigD := 2
	expostRoundTimeout := 200 * time.Millisecond
	expostStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(500 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		vk := []byte(fmt.Sprintf("node%d-vk", i))
		exposts[i] = New(nodes[i], mdags[i], sessionID, vk, expostStartTime, expostRoundTimeout, expostD,
			expostBigD, 32, testGradeFunction, logger)
	}

	// Generate phase
	states := make([][][][]byte, nodeCount)
	labels := make([][]byte, nodeCount)
	errors := make([]error, nodeCount)

	var genWg sync.WaitGroup
	genWg.Add(nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer genWg.Done()
			states[idx], labels[idx], errors[idx] = exposts[idx].Generate(sessionID, exposts[idx].vk)
		}(i)
	}

	genDone := make(chan struct{})
	go func() {
		genWg.Wait()
		close(genDone)
	}()

	select {
	case <-genDone:
	case <-time.After(20 * time.Second):
		t.Fatal("Generation phase timed out")
	}

	// Check for errors
	for i := 0; i < nodeCount; i++ {
		assert.NoError(t, errors[i], "Node %d should not have errors", i)
		assert.NotNil(t, states[i], "Node %d should have state", i)
		assert.NotNil(t, labels[i], "Node %d should have label", i)
	}

	// Verify that all nodes have consistent final states
	for i := 1; i < nodeCount; i++ {
		assert.Equal(t, len(states[0]), len(states[i]), "All nodes should have same state length")
	}

	// Verification phase
	results := make([]*common.Committee, nodeCount)
	verifyErrors := make([]error, nodeCount)

	var verifyWg sync.WaitGroup
	verifyWg.Add(nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer verifyWg.Done()
			auxTag := &common.AuxTag{
				PiRP: []byte(fmt.Sprintf("pi-rp-%d", idx)),
				AuxKey: &common.AuxKey{
					PhiVRF: []byte(fmt.Sprintf("phi-vrf-%d", idx)),
					PiVRF:  []byte(fmt.Sprintf("pi-vrf-%d", idx)),
					PhiVDF: []byte(fmt.Sprintf("phi-vdf-%d", idx)),
					PiVDF:  []byte(fmt.Sprintf("pi-vdf-%d", idx)),
				},
			}
			// Create FSigmaExp for verification
			fSigmaExp := &common.FSigmaExp{
				Challenge: []byte(fmt.Sprintf("challenge-%d", idx)),
				Sigma:     states[idx],
			}
			results[idx], verifyErrors[idx] = exposts[idx].Verify(sessionID,
				[]byte(fmt.Sprintf("node%d-vk", idx)), fSigmaExp, auxTag, 0.5, testFilterTagFunction)
		}(i)
	}

	verifyDone := make(chan struct{})
	go func() {
		verifyWg.Wait()
		close(verifyDone)
	}()

	select {
	case <-verifyDone:
	case <-time.After(30 * time.Second):
		t.Fatal("Verification phase timed out")
	}

	// Check for verification errors
	for i := 0; i < nodeCount; i++ {
		assert.NoError(t, verifyErrors[i], "Node %d should not have verification errors", i)
		assert.NotNil(t, results[i], "Node %d should have results", i)
	}

	// Verify that messages were exchanged
	for i := 0; i < nodeCount; i++ {
		assert.Greater(t, nodes[i].GetMessageCount(), 0, "Node %d should have sent messages", i)
	}

	// Cleanup
	for i := 0; i < nodeCount; i++ {
		_ = nodes[i].Close()
	}
}

//nolint:funlen,gocyclo
func TestExPostIntegrationProverBehavior(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	nodeCount := 3
	nodes := make([]*InMemoryNetwork, nodeCount)
	mdags := make([]*mdag.MDAG, nodeCount)
	exposts := make([]*ExPost, nodeCount)

	// Create network topology
	for i := 0; i < nodeCount; i++ {
		neighbors := make([]string, 0, nodeCount-1)
		for j := 0; j < nodeCount; j++ {
			if i != j {
				neighbors = append(neighbors, fmt.Sprintf("node%d", j))
			}
		}
		nodes[i] = NewInMemoryNetwork(fmt.Sprintf("node%d", i), neighbors)
	}

	// Connect all nodes
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j {
				nodes[i].AddPeer(nodes[j])
			}
		}
	}

	// Create MDAG instances
	mdagRounds := 4
	sessionID := "test-expost-prover"
	mdagRoundTimeout := 200 * time.Millisecond
	mdagStartTime := time.Now().Add(300 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		mdags[i] = mdag.New(mdagRounds, sessionID, testOracle, nodes[i], mdagRoundTimeout, logger, mdagStartTime, "")
	}

	// Create grade function that makes node0 a prover
	proverGradeFunction := func(sid string, vk []byte, ch []byte, auxKey *common.AuxKey, auxLocal float64) int {
		nodeID := string(vk)
		if nodeID == "node0-vk" {
			return 8 // High grade, will be a prover (>= d+1 = 4)
		}
		return 2 // Low grade, not a prover
	}

	// Create ExPost instances
	expostD := 3
	expostBigD := 1
	expostRoundTimeout := 200 * time.Millisecond
	expostStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(500 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		vk := []byte(fmt.Sprintf("node%d-vk", i))
		exposts[i] = New(nodes[i], mdags[i], sessionID, vk, expostStartTime, expostRoundTimeout, expostD,
			expostBigD, 32, proverGradeFunction, logger)
	}

	// Generate phase
	states := make([][][][]byte, nodeCount)
	labels := make([][]byte, nodeCount)
	errors := make([]error, nodeCount)

	var genWg sync.WaitGroup
	genWg.Add(nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer genWg.Done()
			states[idx], labels[idx], errors[idx] = exposts[idx].Generate(sessionID, exposts[idx].vk)
		}(i)
	}

	genDone := make(chan struct{})
	go func() {
		genWg.Wait()
		close(genDone)
	}()

	select {
	case <-genDone:
	case <-time.After(15 * time.Second):
		t.Fatal("Generation phase timed out")
	}

	// Check for errors
	for i := 0; i < nodeCount; i++ {
		assert.NoError(t, errors[i], "Node %d should not have errors", i)
		assert.NotNil(t, states[i], "Node %d should have state", i)
		assert.NotNil(t, labels[i], "Node %d should have label", i)
	}

	// Verification phase
	results := make([]*common.Committee, nodeCount)
	verifyErrors := make([]error, nodeCount)

	var verifyWg sync.WaitGroup
	verifyWg.Add(nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer verifyWg.Done()
			auxTag := &common.AuxTag{
				PiRP: []byte(fmt.Sprintf("pi-rp-%d", idx)),
				AuxKey: &common.AuxKey{
					PhiVRF: []byte(fmt.Sprintf("phi-vrf-%d", idx)),
					PiVRF:  []byte(fmt.Sprintf("pi-vrf-%d", idx)),
					PhiVDF: []byte(fmt.Sprintf("phi-vdf-%d", idx)),
					PiVDF:  []byte(fmt.Sprintf("pi-vdf-%d", idx)),
				},
			}
			// Create FSigmaExp for verification
			fSigmaExp := &common.FSigmaExp{
				Challenge: []byte(fmt.Sprintf("challenge-%d", idx)),
				Sigma:     states[idx],
			}
			results[idx], verifyErrors[idx] = exposts[idx].Verify(sessionID,
				[]byte(fmt.Sprintf("node%d-vk", idx)), fSigmaExp, auxTag, 0.5, testFilterTagFunction)
		}(i)
	}

	verifyDone := make(chan struct{})
	go func() {
		verifyWg.Wait()
		close(verifyDone)
	}()

	select {
	case <-verifyDone:
	case <-time.After(20 * time.Second):
		t.Fatal("Verification phase timed out")
	}

	// Check for verification errors
	for i := 0; i < nodeCount; i++ {
		assert.NoError(t, verifyErrors[i], "Node %d should not have verification errors", i)
		assert.NotNil(t, results[i], "Node %d should have results", i)
	}

	// Node0 should have sent more messages as prover
	assert.Greater(t, nodes[0].GetMessageCount(), 0, "Node0 should send messages as prover")

	// Cleanup
	for i := 0; i < nodeCount; i++ {
		_ = nodes[i].Close()
	}
}

func TestExPostIntegrationMessageFiltering(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	nodeCount := 4
	nodes := make([]*InMemoryNetwork, nodeCount)
	mdags := make([]*mdag.MDAG, nodeCount)
	exposts := make([]*ExPost, nodeCount)

	// Create network topology with restricted neighbors
	// Node0 and Node1 are neighbors, Node2 and Node3 are neighbors
	// Node0 <-> Node1, Node2 <-> Node3, but no cross connections
	neighborMap := map[int][]string{
		0: {"node1"},
		1: {"node0"},
		2: {"node3"},
		3: {"node2"},
	}

	for i := 0; i < nodeCount; i++ {
		nodes[i] = NewInMemoryNetwork(fmt.Sprintf("node%d", i), neighborMap[i])
	}

	// Connect all nodes physically but only neighbors can communicate
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j {
				nodes[i].AddPeer(nodes[j])
			}
		}
	}

	// Create MDAG instances
	mdagRounds := 3
	sessionID := "test-expost-filtering"
	mdagRoundTimeout := 200 * time.Millisecond
	mdagStartTime := time.Now().Add(300 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		mdags[i] = mdag.New(mdagRounds, sessionID, testOracle, nodes[i], mdagRoundTimeout, logger, mdagStartTime, "")
	}

	// Create ExPost instances
	expostD := 2
	expostBigD := 1
	expostRoundTimeout := 300 * time.Millisecond
	expostStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(500 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		vk := []byte(fmt.Sprintf("node%d-vk", i))
		exposts[i] = New(nodes[i], mdags[i], sessionID, vk, expostStartTime, expostRoundTimeout,
			expostD, expostBigD, 32, testGradeFunction, logger)
	}

	// Generate phase
	states := make([][][][]byte, nodeCount)
	labels := make([][]byte, nodeCount)
	errors := make([]error, nodeCount)

	var genWg sync.WaitGroup
	genWg.Add(nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer genWg.Done()
			states[idx], labels[idx], errors[idx] = exposts[idx].Generate(sessionID, exposts[idx].vk)
		}(i)
	}

	genDone := make(chan struct{})
	go func() {
		genWg.Wait()
		close(genDone)
	}()

	select {
	case <-genDone:
	case <-time.After(15 * time.Second):
		t.Fatal("Generation phase timed out")
	}

	// Check for errors
	for i := 0; i < nodeCount; i++ {
		assert.NoError(t, errors[i], "Node %d should not have errors", i)
		assert.NotNil(t, states[i], "Node %d should have state", i)
		assert.NotNil(t, labels[i], "Node %d should have label", i)
	}

	// Verification phase
	results := make([]*common.Committee, nodeCount)
	verifyErrors := make([]error, nodeCount)

	var verifyWg sync.WaitGroup
	verifyWg.Add(nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer verifyWg.Done()
			auxTag := &common.AuxTag{
				PiRP: []byte(fmt.Sprintf("pi-rp-%d", idx)),
				AuxKey: &common.AuxKey{
					PhiVRF: []byte(fmt.Sprintf("phi-vrf-%d", idx)),
					PiVRF:  []byte(fmt.Sprintf("pi-vrf-%d", idx)),
					PhiVDF: []byte(fmt.Sprintf("phi-vdf-%d", idx)),
					PiVDF:  []byte(fmt.Sprintf("pi-vdf-%d", idx)),
				},
			}
			// Create FSigmaExp for verification
			fSigmaExp := &common.FSigmaExp{
				Challenge: []byte(fmt.Sprintf("challenge-%d", idx)),
				Sigma:     states[idx],
			}
			results[idx], verifyErrors[idx] = exposts[idx].Verify(sessionID,
				[]byte(fmt.Sprintf("node%d-vk", idx)), fSigmaExp, auxTag, 0.5, testFilterTagFunction)
		}(i)
	}

	verifyDone := make(chan struct{})
	go func() {
		verifyWg.Wait()
		close(verifyDone)
	}()

	select {
	case <-verifyDone:
	case <-time.After(20 * time.Second):
		t.Fatal("Verification phase timed out")
	}

	// Check for verification errors
	for i := 0; i < nodeCount; i++ {
		assert.NoError(t, verifyErrors[i], "Node %d should not have verification errors", i)
		assert.NotNil(t, results[i], "Node %d should have results", i)
	}

	// Verify message filtering worked - nodes should only communicate with neighbors
	// Node0 and Node1 should have exchanged messages
	// Node2 and Node3 should have exchanged messages
	// But no cross-group communication should occur

	// Cleanup
	for i := 0; i < nodeCount; i++ {
		_ = nodes[i].Close()
	}
}

func TestExPostIntegrationConcurrentExecution(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	nodeCount := 5
	nodes := make([]*InMemoryNetwork, nodeCount)
	mdags := make([]*mdag.MDAG, nodeCount)
	exposts := make([]*ExPost, nodeCount)

	// Create fully connected network
	for i := 0; i < nodeCount; i++ {
		neighbors := make([]string, 0, nodeCount-1)
		for j := 0; j < nodeCount; j++ {
			if i != j {
				neighbors = append(neighbors, fmt.Sprintf("node%d", j))
			}
		}
		nodes[i] = NewInMemoryNetwork(fmt.Sprintf("node%d", i), neighbors)
	}

	// Connect all nodes
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j {
				nodes[i].AddPeer(nodes[j])
			}
		}
	}

	// Create MDAG instances
	mdagRounds := 5
	sessionID := "test-expost-concurrent"
	mdagRoundTimeout := 150 * time.Millisecond
	mdagStartTime := time.Now().Add(300 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		mdags[i] = mdag.New(mdagRounds, sessionID, testOracle, nodes[i], mdagRoundTimeout, logger, mdagStartTime, "")
	}

	// Create ExPost instances with different parameters
	expostD := 2
	expostBigD := 2
	expostRoundTimeout := 150 * time.Millisecond
	expostStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(300 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		vk := []byte(fmt.Sprintf("node%d-vk", i))
		exposts[i] = New(nodes[i], mdags[i], sessionID, vk, expostStartTime, expostRoundTimeout,
			expostD, expostBigD, 16, testGradeFunction, logger)
	}

	// Run generation and verification concurrently
	var wg sync.WaitGroup
	wg.Add(nodeCount)

	results := make([]*common.Committee, nodeCount)
	errors := make([]error, nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer wg.Done()

			// Generate
			state, label, genErr := exposts[idx].Generate(sessionID, exposts[idx].vk)
			if genErr != nil {
				errors[idx] = genErr
				return
			}

			// Verify
			auxTag := &common.AuxTag{
				PiRP: []byte(fmt.Sprintf("pi-rp-%d", idx)),
				AuxKey: &common.AuxKey{
					PhiVRF: []byte(fmt.Sprintf("phi-vrf-%d", idx)),
					PiVRF:  []byte(fmt.Sprintf("pi-vrf-%d", idx)),
					PhiVDF: []byte(fmt.Sprintf("phi-vdf-%d", idx)),
					PiVDF:  []byte(fmt.Sprintf("pi-vdf-%d", idx)),
				},
			}

			// Create FSigmaExp for verification
			fSigmaExp := &common.FSigmaExp{
				Challenge: []byte(fmt.Sprintf("challenge-%d", idx)),
				Sigma:     state,
			}
			result, verifyErr := exposts[idx].Verify(sessionID, []byte(fmt.Sprintf("node%d-vk", idx)), fSigmaExp, auxTag, 0.5, testFilterTagFunction)

			if verifyErr != nil {
				errors[idx] = verifyErr
				return
			}

			results[idx] = result
			_ = label // Use label to avoid unused variable warning
		}(i)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("Concurrent execution timed out")
	}

	// Check for errors
	for i := 0; i < nodeCount; i++ {
		assert.NoError(t, errors[i], "Node %d should not have errors", i)
		assert.NotNil(t, results[i], "Node %d should have results", i)
	}

	// Verify that messages were exchanged
	totalMessages := 0
	for i := 0; i < nodeCount; i++ {
		messageCount := nodes[i].GetMessageCount()
		assert.Greater(t, messageCount, 0, "Node %d should have sent messages", i)
		totalMessages += messageCount
	}

	assert.Greater(t, totalMessages, nodeCount, "Total messages should be greater than node count")

	// Cleanup
	for i := 0; i < nodeCount; i++ {
		_ = nodes[i].Close()
	}
}

func TestExPostIntegrationErrorHandling(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	nodeCount := 3
	nodes := make([]*InMemoryNetwork, nodeCount)
	mdags := make([]*mdag.MDAG, nodeCount)
	exposts := make([]*ExPost, nodeCount)

	// Create network topology
	for i := 0; i < nodeCount; i++ {
		neighbors := make([]string, 0, nodeCount-1)
		for j := 0; j < nodeCount; j++ {
			if i != j {
				neighbors = append(neighbors, fmt.Sprintf("node%d", j))
			}
		}
		nodes[i] = NewInMemoryNetwork(fmt.Sprintf("node%d", i), neighbors)
	}

	// Connect all nodes
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j {
				nodes[i].AddPeer(nodes[j])
			}
		}
	}

	// Create MDAG instances with insufficient rounds
	mdagRounds := 3
	sessionID := "test-expost-errors"
	mdagRoundTimeout := 200 * time.Millisecond
	mdagStartTime := time.Now().Add(300 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		mdags[i] = mdag.New(mdagRounds, sessionID, testOracle, nodes[i], mdagRoundTimeout, logger, mdagStartTime, "")
	}

	// Create ExPost instances where R = d * D > mdagRounds
	// This will cause GetComputedLabel(R) to return nil during Generate()
	expostD := 5    // d = 5
	expostBigD := 3 // D = 3, so R = d * D = 15 > mdagRounds = 3
	expostRoundTimeout := 200 * time.Millisecond
	expostStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(500 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		vk := []byte(fmt.Sprintf("node%d-vk", i))
		exposts[i] = New(nodes[i], mdags[i], sessionID, vk, expostStartTime, expostRoundTimeout,
			expostD, expostBigD, 32, testGradeFunction, logger)
	}

	// Generate phase - this should fail because R = 15 > mdagRounds = 3
	states := make([][][][]byte, nodeCount)
	labels := make([][]byte, nodeCount)
	genErrors := make([]error, nodeCount)

	var genWg sync.WaitGroup
	genWg.Add(nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer genWg.Done()
			states[idx], labels[idx], genErrors[idx] = exposts[idx].Generate(sessionID, exposts[idx].vk)
		}(i)
	}

	genDone := make(chan struct{})
	go func() {
		genWg.Wait()
		close(genDone)
	}()

	select {
	case <-genDone:
	case <-time.After(15 * time.Second):
		t.Fatal("Generation phase timed out")
	}

	// Check that generation failed with expected error
	for i := 0; i < nodeCount; i++ {
		assert.Error(t, genErrors[i], "Node %d should have generation error", i)
		assert.Contains(t, genErrors[i].Error(), "computed label for round R is nil", "Error should mention nil label")
		assert.Nil(t, states[i], "Node %d should have nil state on error", i)
		assert.Nil(t, labels[i], "Node %d should have nil label on error", i)
	}

	// Cleanup
	for i := 0; i < nodeCount; i++ {
		_ = nodes[i].Close()
	}
}

//nolint:funlen,gocyclo
func TestExPostIntegrationVerificationErrorHandling(t *testing.T) {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)

	nodeCount := 3
	nodes := make([]*InMemoryNetwork, nodeCount)
	mdags := make([]*mdag.MDAG, nodeCount)
	exposts := make([]*ExPost, nodeCount)

	// Create network topology
	for i := 0; i < nodeCount; i++ {
		neighbors := make([]string, 0, nodeCount-1)
		for j := 0; j < nodeCount; j++ {
			if i != j {
				neighbors = append(neighbors, fmt.Sprintf("node%d", j))
			}
		}
		nodes[i] = NewInMemoryNetwork(fmt.Sprintf("node%d", i), neighbors)
	}

	// Connect all nodes
	for i := 0; i < nodeCount; i++ {
		for j := 0; j < nodeCount; j++ {
			if i != j {
				nodes[i].AddPeer(nodes[j])
			}
		}
	}

	// Create MDAG instances with sufficient rounds for generation
	mdagRounds := 6
	sessionID := "test-expost-verify-errors"
	mdagRoundTimeout := 200 * time.Millisecond
	mdagStartTime := time.Now().Add(300 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		mdags[i] = mdag.New(mdagRounds, sessionID, testOracle, nodes[i], mdagRoundTimeout, logger, mdagStartTime, "")
	}

	// Create ExPost instances with parameters that will work for generation
	// but will fail verification due to insufficient sigma length
	expostD := 3
	expostBigD := 2
	expostRoundTimeout := 200 * time.Millisecond
	expostStartTime := mdagStartTime.Add(time.Duration(mdagRounds+1) * mdagRoundTimeout).Add(500 * time.Millisecond)

	for i := 0; i < nodeCount; i++ {
		vk := []byte(fmt.Sprintf("node%d-vk", i))
		exposts[i] = New(nodes[i], mdags[i], sessionID, vk, expostStartTime, expostRoundTimeout,
			expostD, expostBigD, 32, testGradeFunction, logger)
	}

	// Generate phase - this should succeed
	states := make([][][][]byte, nodeCount)
	labels := make([][]byte, nodeCount)
	genErrors := make([]error, nodeCount)

	var genWg sync.WaitGroup
	genWg.Add(nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer genWg.Done()
			states[idx], labels[idx], genErrors[idx] = exposts[idx].Generate(sessionID, exposts[idx].vk)
		}(i)
	}

	genDone := make(chan struct{})
	go func() {
		genWg.Wait()
		close(genDone)
	}()

	select {
	case <-genDone:
	case <-time.After(15 * time.Second):
		t.Fatal("Generation phase timed out")
	}

	// Check that generation succeeded
	for i := 0; i < nodeCount; i++ {
		assert.NoError(t, genErrors[i], "Node %d should not have generation errors", i)
		assert.NotNil(t, states[i], "Node %d should have state", i)
		assert.NotNil(t, labels[i], "Node %d should have label", i)
	}

	// Create insufficient sigma for verification (less than R = d * D = 6)
	insufficientSigma := make([][][]byte, 3) // Only 3 rounds, but need 6
	for i := 0; i < 3; i++ {
		insufficientSigma[i] = [][]byte{[]byte(fmt.Sprintf("insufficient-state-%d", i))}
	}

	// Verification phase - this should fail due to insufficient sigma length
	verifyErrors := make([]error, nodeCount)

	var verifyWg sync.WaitGroup
	verifyWg.Add(nodeCount)

	for i := 0; i < nodeCount; i++ {
		go func(idx int) {
			defer verifyWg.Done()
			auxTag := &common.AuxTag{
				PiRP: []byte(fmt.Sprintf("pi-rp-%d", idx)),
				AuxKey: &common.AuxKey{
					PhiVRF: []byte(fmt.Sprintf("phi-vrf-%d", idx)),
					PiVRF:  []byte(fmt.Sprintf("pi-vrf-%d", idx)),
					PhiVDF: []byte(fmt.Sprintf("phi-vdf-%d", idx)),
					PiVDF:  []byte(fmt.Sprintf("pi-vdf-%d", idx)),
				},
			}
			// Create FSigmaExp for verification
			fSigmaExp := &common.FSigmaExp{
				Challenge: []byte(fmt.Sprintf("challenge-%d", idx)),
				Sigma:     insufficientSigma,
			}
			_, verifyErrors[idx] = exposts[idx].Verify(sessionID, []byte(fmt.Sprintf("node%d-vk", idx)),
				fSigmaExp, auxTag, 0.5, testFilterTagFunction)
		}(i)
	}

	verifyDone := make(chan struct{})
	go func() {
		verifyWg.Wait()
		close(verifyDone)
	}()

	select {
	case <-verifyDone:
	case <-time.After(10 * time.Second):
		t.Fatal("Verification phase timed out")
	}

	// Check that verification failed with expected error
	for i := 0; i < nodeCount; i++ {
		assert.Error(t, verifyErrors[i], "Node %d should have verification error", i)
		assert.Contains(t, verifyErrors[i].Error(), "sigma length is less than required rounds", "Error should mention sigma length")
	}

	// Cleanup
	for i := 0; i < nodeCount; i++ {
		_ = nodes[i].Close()
	}
}
