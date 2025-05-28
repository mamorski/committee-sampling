package tests

import (
	"math/rand"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/mamorski/committee-sampling/internal/network"
)

type MockNetwork struct {
	mocknet   mocknet.Mocknet
	host      host.Host
	nodeID    string
	neighbors []string
	handlers  map[string]network.MessageHandler
	mu        sync.RWMutex
	closed    bool
}

func NewMockNetwork() *MockNetwork {
	mn := mocknet.New()
	peer, _ := mn.GenPeer()

	return &MockNetwork{
		mocknet:   mn,
		host:      peer,
		nodeID:    peer.ID().String(),
		neighbors: make([]string, 0),
		handlers:  make(map[string]network.MessageHandler),
	}
}

func (m *MockNetwork) RegisterHandler(protocolID string, handler network.MessageHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handlers[protocolID] = handler
}

func (m *MockNetwork) SendProtocolMessage(protocolID string, data []byte) {
	m.mu.RLock()
	neighbors := make([]string, len(m.neighbors))
	copy(neighbors, m.neighbors)
	m.mu.RUnlock()

	for _, neighborID := range neighbors {
		go func(nID string) {
			if handler, exists := m.handlers[protocolID]; exists {
				_ = handler(nID, data)
			}
		}(neighborID)
	}
}

func (m *MockNetwork) GetNeighbors() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	neighbors := make([]string, len(m.neighbors))
	copy(neighbors, m.neighbors)
	return neighbors
}

func (m *MockNetwork) GetNodeID() string {
	return m.nodeID
}

func (m *MockNetwork) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true
	return m.host.Close()
}

func (m *MockNetwork) AddNeighbor(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, existing := range m.neighbors {
		if existing == peerID {
			return
		}
	}

	m.neighbors = append(m.neighbors, peerID)
}

func (m *MockNetwork) RemoveNeighbor(peerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, neighbor := range m.neighbors {
		if neighbor == peerID {
			m.neighbors = append(m.neighbors[:i], m.neighbors[i+1:]...)
			return
		}
	}
}

func (m *MockNetwork) ConnectTo(other *MockNetwork) error {
	// If they're from different mocknets, we can't use the mocknet connection
	// Instead, we'll just simulate the connection by adding neighbors
	if m.mocknet != other.mocknet {
		m.AddNeighbor(other.GetNodeID())
		other.AddNeighbor(m.GetNodeID())
		return nil
	}

	_, err := m.mocknet.LinkPeers(m.host.ID(), other.host.ID())
	if err != nil {
		return err
	}

	_, err = m.mocknet.ConnectPeers(m.host.ID(), other.host.ID())
	if err != nil {
		return err
	}

	m.AddNeighbor(other.GetNodeID())
	other.AddNeighbor(m.GetNodeID())

	return nil
}

func (m *MockNetwork) GetHost() host.Host {
	return m.host
}

func (m *MockNetwork) GetMocknet() mocknet.Mocknet {
	return m.mocknet
}

func (m *MockNetwork) SimulateMessage(fromPeerID, protocolID string, data []byte) error {
	m.mu.RLock()
	handler, exists := m.handlers[protocolID]
	m.mu.RUnlock()

	if !exists {
		return nil
	}

	return handler(fromPeerID, data)
}

type MockNetworkCluster struct {
	networks []*MockNetwork
	mocknet  mocknet.Mocknet
}

func NewMockNetworkCluster(size int) *MockNetworkCluster {
	mn := mocknet.New()
	networks := make([]*MockNetwork, size)

	for i := 0; i < size; i++ {
		peer, _ := mn.GenPeer()
		networks[i] = &MockNetwork{
			mocknet:   mn,
			host:      peer,
			nodeID:    peer.ID().String(),
			neighbors: make([]string, 0),
			handlers:  make(map[string]network.MessageHandler),
		}
	}

	return &MockNetworkCluster{
		networks: networks,
		mocknet:  mn,
	}
}

func (c *MockNetworkCluster) GetNetwork(index int) *MockNetwork {
	if index < 0 || index >= len(c.networks) {
		return nil
	}
	return c.networks[index]
}

func (c *MockNetworkCluster) ConnectAll() error {
	for i := 0; i < len(c.networks); i++ {
		for j := i + 1; j < len(c.networks); j++ {
			err := c.networks[i].ConnectTo(c.networks[j])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *MockNetworkCluster) ConnectRing() error {
	for i := 0; i < len(c.networks); i++ {
		next := (i + 1) % len(c.networks)

		// Link and connect peers in the mocknet
		_, err := c.mocknet.LinkPeers(c.networks[i].host.ID(), c.networks[next].host.ID())
		if err != nil {
			return err
		}

		_, err = c.mocknet.ConnectPeers(c.networks[i].host.ID(), c.networks[next].host.ID())
		if err != nil {
			return err
		}

		// Add neighbors (only one direction to create a ring)
		c.networks[i].AddNeighbor(c.networks[next].GetNodeID())
	}
	return nil
}

func (c *MockNetworkCluster) Size() int {
	return len(c.networks)
}

func (c *MockNetworkCluster) Close() error {
	for _, n := range c.networks {
		if err := n.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (c *MockNetworkCluster) ConnectRandom(maxNeighbors int) error {
	if maxNeighbors <= 0 {
		return nil
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec

	for i := 0; i < len(c.networks); i++ {
		currentNeighbors := len(c.networks[i].GetNeighbors())

		// Calculate how many more neighbors this node can have
		remainingSlots := maxNeighbors - currentNeighbors
		if remainingSlots <= 0 {
			continue
		}

		// Get list of potential neighbors (nodes not already connected)
		potentialNeighbors := make([]int, 0)
		currentNeighborIDs := make(map[string]bool)

		for _, neighborID := range c.networks[i].GetNeighbors() {
			currentNeighborIDs[neighborID] = true
		}

		for j := 0; j < len(c.networks); j++ {
			if i != j && !currentNeighborIDs[c.networks[j].GetNodeID()] {
				// Also check if the potential neighbor has room for more connections
				if len(c.networks[j].GetNeighbors()) < maxNeighbors {
					potentialNeighbors = append(potentialNeighbors, j)
				}
			}
		}

		// Randomly select neighbors up to the remaining slots
		numToConnect := remainingSlots
		if numToConnect > len(potentialNeighbors) {
			numToConnect = len(potentialNeighbors)
		}

		// Shuffle potential neighbors and take the first numToConnect
		rng.Shuffle(len(potentialNeighbors), func(a, b int) {
			potentialNeighbors[a], potentialNeighbors[b] = potentialNeighbors[b], potentialNeighbors[a]
		})

		for k := 0; k < numToConnect; k++ {
			j := potentialNeighbors[k]

			// Link and connect peers in the mocknet
			_, err := c.mocknet.LinkPeers(c.networks[i].host.ID(), c.networks[j].host.ID())
			if err != nil {
				return err
			}

			_, err = c.mocknet.ConnectPeers(c.networks[i].host.ID(), c.networks[j].host.ID())
			if err != nil {
				return err
			}

			// Add bidirectional neighbors
			c.networks[i].AddNeighbor(c.networks[j].GetNodeID())
			c.networks[j].AddNeighbor(c.networks[i].GetNodeID())
		}
	}

	return nil
}
