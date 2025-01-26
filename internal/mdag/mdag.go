package mdag

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
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
	Gen(sid, vki string, vi ...string) (map[int][]string, error)
	Verify() bool
}

type MDAG struct {
	network network.Network
	oracle  HashOracle
	lock    sync.Mutex
	logger  *zap.Logger

	round  int
	rounds int

	currentLabel string
	roundLabels  map[int]string
	receivedMsgs map[int]map[string]string
	roundDone    map[int]chan struct{}
	roundTimeout time.Duration
	neighbors    []string

	state  map[string][][]string // Stores received messages for each peer and round.
	labels map[string][]string   // Stores computed labels for each round.
}

func New(rounds int, oracle HashOracle, network network.Network, timeout time.Duration, logger *zap.Logger) *MDAG {
	m := &MDAG{
		round:        0,
		rounds:       rounds,
		oracle:       oracle,
		network:      network,
		roundLabels:  make(map[int]string),
		receivedMsgs: make(map[int]map[string]string),
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

// Gen TODO: Return not sorted map, and not concatenated.
func (m *MDAG) Gen(sid, vki string, vi ...string) (map[int]string, error) {
	input := sid + vki
	for _, v := range vi {
		input += v
	}

	m.currentLabel = string(m.oracle([]byte(input)))
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
		prevMsgs := make([]string, 0, len(m.receivedMsgs[r-1]))
		for _, msg := range m.receivedMsgs[r-1] {
			prevMsgs = append(prevMsgs, msg)
		}
		sort.Strings(prevMsgs)
		m.currentLabel = string(m.oracle([]byte(strings.Join(prevMsgs, ""))))
		m.roundLabels[m.round] = m.currentLabel

		// Broadcast the label to all neighbors
		if err := m.broadcast(m.round, m.network.GetNodeID(), sid, m.currentLabel); err != nil {
			m.logger.Error("failed to broadcast label", zap.Error(err))
			return nil, fmt.Errorf("failed to broadcast label: %v", err)
		}
	}

	return m.roundLabels, nil
}

func (m *MDAG) GetLastLabel() string {
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

// handleMessage handles incoming messages.
func (m *MDAG) handleMessage(from string, payload []byte) error {
	var msg pb.MDAGMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return fmt.Errorf("failed to unmarshal message: %v", err)
	}

	// Validate that the From field matches the network-level from
	if msg.From != from {
		return fmt.Errorf("message From field %s does not match network from %s", msg.From, from)
	}

	round := int(msg.Round)

	m.lock.Lock()
	defer m.lock.Unlock()

	if len(m.state[from]) <= round {
		m.state[from] = append(m.state[from], make([][]string, round-len(m.state[from])+1)...) // Extend state.
	}

	m.state[from][msg.Round] = msg.Label

	// Check if the round is complete (all peers have sent their messages).
	if m.isRoundComplete(msg.Round) {
		close(m.roundEnd[msg.Round])
	}

	return nil
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

func (m *MDAG) broadcast(round int, from, sid, label string) error {
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
