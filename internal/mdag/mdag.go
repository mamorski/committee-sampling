package mdag

import (
	"bytes"
	"fmt"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/mamorski/committee-sampling/internal/network"
	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

const protocolID = "/mdag/1.0.0"

// HashOracle defines the interface for the hashing function
type HashOracle func([]byte) []byte

type MerkleDAG interface {
	Gen() error
	Verify() bool
}

type MDAG struct {
	network network.Network
	oracle  HashOracle
	lock    sync.Mutex
	logger  *zap.Logger

	round  int
	rounds int

	currentLabel []byte
	roundLabels  map[int][]byte
	receivedMsgs map[int]map[string][]byte
	roundDone    map[int]chan struct{}
	roundTimeout time.Duration
	neighbors    []string
}

func New(rounds int, oracle HashOracle, network network.Network, timeout time.Duration, logger *zap.Logger) *MDAG {
	m := &MDAG{
		round:        0,
		rounds:       rounds,
		oracle:       oracle,
		network:      network,
		roundLabels:  make(map[int][]byte),
		receivedMsgs: make(map[int]map[string][]byte),
		roundDone:    make(map[int]chan struct{}),
		roundTimeout: timeout,
		neighbors:    network.GetNeighbors(),
		logger:       logger,
	}

	// Initialize round completion channels
	for i := 0; i <= rounds; i++ {
		m.roundDone[i] = make(chan struct{})
	}

	// Register message handler
	network.RegisterHandler(protocolID, m.handleMessage)

	return m
}

// TODO: Return not sorted map, and not concatanated. Consider using strings instead of bytes
func (m *MDAG) Gen(sid, vki []byte, vi ...[]byte) (map[int][]byte, error) {
	input := append(append(append([]byte{}, sid...), vki...))
	for _, v := range vi {
		input = append(input, v...)
	}
	m.currentLabel = m.oracle(input)
	m.roundLabels[m.round] = m.currentLabel

	// Broadcast the label to all neighbors
	if err := m.broadcast(m.round, m.network.GetNodeID(), sid, m.currentLabel); err != nil {
		m.logger.Error("failed to broadcast label", zap.Error(err))
		return nil, fmt.Errorf("failed to broadcast label: %v", err)
	}

	for r := 1; r <= m.rounds; r++ {
		m.round = r

		// Wait for all neighbors to respond
		select {
		case <-m.roundDone[r-1]:
			// Round is complete
			m.logger.Info("round complete", zap.Int("round", r-1))
		case <-time.After(m.roundTimeout):
			m.logger.Error("round timeout", zap.Int("round", r))
		}

		// Get messages from previous round
		prevMsgs := sortMapToSlice(m.receivedMsgs[r-1])
		m.currentLabel = make([]byte, 0)
		for _, msg := range prevMsgs {
			m.currentLabel = append(m.currentLabel, msg...)
		}

		m.currentLabel = m.oracle(m.currentLabel)
		m.roundLabels[m.round] = m.currentLabel

		// Broadcast the label to all neighbors
		if err := m.broadcast(m.round, m.network.GetNodeID(), sid, m.currentLabel); err != nil {
			m.logger.Error("failed to broadcast label", zap.Error(err))
			return nil, fmt.Errorf("failed to broadcast label: %v", err)
		}
	}

	return m.roundLabels, nil
}

func (m *MDAG) GetLastLabel() []byte {
	return m.currentLabel
}

// Verify implements the verification functionality from the paper
func (m *MDAG) Verify(startLabel []byte, path [][]byte, endLabel []byte) bool {
	if len(path) == 0 {
		return false
	}

	// First element in path should match startLabel
	if !bytes.Equal(path[0], startLabel) {
		return false
	}

	currentLabel := startLabel
	// For each set of labels in the path
	for i := 1; i < len(path); i++ {
		labels := path[i]

		// Add currentLabel and all path labels
		input := append(append([]byte{}, currentLabel...), labels...)
		currentLabel = m.oracle(input)
	}

	// Final label should match endLabel
	return bytes.Equal(currentLabel, endLabel)
}

func (m *MDAG) handleMessage(from string, data []byte) error {
	var msg pb.MDAGMessage
	if err := proto.Unmarshal(data, &msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %v", err)
	}

	// Validate that the From field matches the network-level from
	if msg.From != from {
		return fmt.Errorf("message From field %s does not match network from %s", msg.From, from)
	}

	round := int(msg.Round)

	m.lock.Lock()
	defer m.lock.Unlock()

	// Store the message
	if m.receivedMsgs[round] == nil {
		m.receivedMsgs[round] = make(map[string][]byte)
	}
	if _, ok := m.receivedMsgs[round][msg.From]; ok {
		m.logger.Warn("duplicate message received", zap.String("from", msg.From), zap.Int("round", round))
		return nil
	}

	m.receivedMsgs[round][msg.From] = msg.Label

	// Check if round is complete (received from all neighbors)
	if len(m.receivedMsgs[round]) >= len(m.neighbors) {
		select {
		case <-m.roundDone[round]:
			// Channel already closed
		default:
			close(m.roundDone[round])
		}
	}

	return nil
}

func (m *MDAG) broadcast(round int, from string, sid, label []byte) error {
	msg := &pb.MDAGMessage{
		From:      from,
		Round:     uint32(round),
		Label:     label,
		SessionId: sid,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	m.network.SendProtocolMessage(protocolID, data)
	return nil
}

func sortMapToSlice(m map[string][]byte) [][]byte {
	s := make([][]byte, len(m))
	i := 0
	for _, v := range m {
		s[i] = v
		i++
	}
	sort.Slice(s, func(i, j int) bool {
		return bytes.Compare(s[i], s[j]) < 0
	})

	return s
}
