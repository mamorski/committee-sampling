package mdag

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mamorski/committee-sampling/internal/network"
	mdagpb "github.com/mamorski/committee-sampling/pkg/proto"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const protocolID = "/mdag/1.0.0"

// HashOracle defines the interface for the hashing function
type HashOracle func([]byte) []byte

type MDAG struct {
	rounds       int             // total number of rounds (round 1...rounds; round 0 is initialization)
	oracle       HashOracle      // hash oracle (random oracle)
	network      network.Network // network interface for asynchronous messaging
	roundTimeout time.Duration   // time to wait each round before computing the next label
	logger       *zap.Logger     // zap logger for logging events
	isRunning    bool            // flag indicating if the protocol is running
	startTime    time.Time       // time when the protocol should start
	neighbors    map[string]bool // set of allowed neighbor node IDs

	mu             sync.Mutex
	messages       map[int][][]byte // messages received from the network, keyed by round number
	state          [][][]byte       // state: bucket S_{i,r} of labels received in round (r-1)
	computedLabels [][]byte         // computed label for each round (r >= 1)
	currentLabel   []byte           // label computed in the most recent round
	sessionID      string           // current protocol session id
}

// New creates a new MDAG instance with the specified parameters.
//
// Parameters:
//   - rounds: Number of rounds to run in the protocol (excluding initialization round 0)
//   - sid: Session identifier for this protocol instance
//   - oracle: Hash oracle function used for computing labels
//   - network: Network interface for message passing
//   - roundTimeout: Duration to wait for each round before proceeding
//   - logger: Structured logger for recording events
//   - startTime: Time when the protocol should start execution
//
// Returns a configured MDAG instance ready to run the protocol.
func New(rounds int, sid string, oracle HashOracle, network network.Network, roundTimeout time.Duration, logger *zap.Logger, startTime time.Time) *MDAG {
	neighborsList := network.GetNeighbors()
	neighbors := make(map[string]bool, len(neighborsList))
	for _, neighbor := range neighborsList {
		neighbors[neighbor] = true
	}

	m := &MDAG{
		rounds:         rounds,
		oracle:         oracle,
		network:        network,
		roundTimeout:   roundTimeout,
		logger:         logger,
		messages:       make(map[int][][]byte),
		computedLabels: make([][]byte, rounds),
		state:          make([][][]byte, rounds),
		isRunning:      false,
		sessionID:      sid,
		startTime:      startTime,
		neighbors:      neighbors,
	}

	network.RegisterHandler(fmt.Sprintf("%s/%s", protocolID, sid), m.handleMessage)
	m.logger.Info("MDAG instance created",
		zap.Int("rounds", rounds),
		zap.Int("neighbors", len(neighbors)))
	return m
}

// Generate runs the MDAG protocol and produces a sequence of labels.
//
// The protocol follows these steps:
// 1. Compute initial label from session ID, verification key, and additional inputs
// 2. For each round, collect messages from previous round and compute new label
// 3. Broadcast the new label to all peers
//
// Parameters:
//   - sid: Session identifier for this run
//   - vki: Verification key
//   - vi: Additional inputs for the initial label
//
// Returns the state (sequence of labels received in each round) and any error.
func (m *MDAG) Generate(sid string, vki []byte, vi ...[]byte) ([][][]byte, error) {
	m.mu.Lock()
	if m.isRunning {
		m.mu.Unlock()
		return nil, fmt.Errorf("protocol is already running")
	}

	if m.sessionID != "" && m.sessionID != sid {
		m.mu.Unlock()
		return nil, fmt.Errorf("session ID mismatch: expected %s, got %s", m.sessionID, sid)
	}

	m.isRunning = true
	m.mu.Unlock()

	m.logger.Info("Starting MDAG protocol", zap.String("session_id", sid), zap.String("node_id", m.network.GetNodeID()))

	// Compute the initial label: sid || vki || (concatenation of vi)
	var buffer bytes.Buffer
	buffer.WriteString(sid)
	buffer.Write(vki)
	for _, v := range vi {
		buffer.Write(v)
	}
	m.currentLabel = m.oracle(buffer.Bytes())

	m.mu.Lock()
	// Send to itself
	m.messages[0] = append(m.messages[0], m.currentLabel)
	m.mu.Unlock()

	// Broadcast the initial label
	m.broadcast(0, m.currentLabel)

	// Wait for the protocol to start
	time.Sleep(time.Until(m.startTime))

	// Run the protocol for rounds 1 to m.rounds
	for r := 1; r <= m.rounds; r++ {
		m.logger.Info("Starting round", zap.Int("round", r))

		// Wait for messages to arrive for this round
		time.Sleep(m.roundTimeout)

		// Lock to safely access messages
		m.mu.Lock()
		// Get messages received in the previous round
		prevRoundMsgs, exists := m.messages[r-1]
		if !exists || len(prevRoundMsgs) == 0 {
			m.logger.Warn("No messages received in previous round", zap.Int("round", r-1))
		}
		m.mu.Unlock()

		// Create a bucket for this round's state
		bucket := make([][]byte, len(prevRoundMsgs))
		copy(bucket, prevRoundMsgs)
		m.state[r-1] = bucket

		var sortedLabels [][]byte
		if r == 1 {
			// For round 1, compute m0 ← H(sort(L0))
			sortedLabels = make([][]byte, len(prevRoundMsgs))
			copy(sortedLabels, prevRoundMsgs)
		} else {
			// For rounds > 1, compute mi ← H(sort({mi-1} ∪ Li))
			sortedLabels = make([][]byte, len(prevRoundMsgs)+1)
			sortedLabels[0] = m.currentLabel
			copy(sortedLabels[1:], prevRoundMsgs)
		}

		// Sort the labels
		sort.Slice(sortedLabels, func(i, j int) bool {
			return bytes.Compare(sortedLabels[i], sortedLabels[j]) < 0
		})

		// Concatenate and hash
		var concatenated []byte
		for _, lab := range sortedLabels {
			concatenated = append(concatenated, lab...)
		}
		newLabel := m.oracle(concatenated)
		m.computedLabels[r-1] = newLabel
		m.currentLabel = newLabel

		m.logger.Info("Completed round", zap.Int("round", r), zap.String("new_label", base64.StdEncoding.EncodeToString(m.currentLabel)))

		// Broadcast the new label if not the last round
		if r < m.rounds {
			m.broadcast(r, m.currentLabel)
		}
	}

	// Mark the protocol as completed
	m.mu.Lock()
	m.isRunning = false
	m.mu.Unlock()

	m.logger.Info("MDAG protocol completed")
	return m.state, nil
}

// handleMessage processes incoming messages from the network.
//
// The function validates that the message is from a known neighbor, has a matching session ID,
// and is received while the protocol is running.
//
// Parameters:
//   - _: Protocol ID (unused but required by the handler interface)
//   - payload: Raw message bytes received from the network
//
// Returns an error if validation fails, nil otherwise.
func (m *MDAG) handleMessage(_ string, payload []byte) error {
	if !m.isRunning {
		return errors.New("protocol not running")
	}

	var pbMsg mdagpb.MDAGMessage
	if err := proto.Unmarshal(payload, &pbMsg); err != nil {
		m.logger.Error("Failed to unmarshal message", zap.Error(err))
		return err
	}

	if !m.neighbors[pbMsg.From] {
		err := errors.New("message from unknown neighbor")
		m.logger.Warn("Received message from unknown neighbor",
			zap.String("from", pbMsg.From))
		return err
	}

	if m.sessionID != "" && pbMsg.SessionId != m.sessionID {
		err := errors.New("session id mismatch")
		m.logger.Warn("Received message with mismatched session id",
			zap.String("expected", m.sessionID),
			zap.String("received", pbMsg.SessionId))
		return err
	}

	m.mu.Lock()
	if !m.isRunning {
		m.mu.Unlock()
		m.logger.Warn("Received message while protocol is not running")
		return errors.New("protocol not running")
	}

	round := int(pbMsg.Round)

	if _, exists := m.messages[round]; !exists {
		m.messages[round] = make([][]byte, 0, 16)
	}
	m.messages[round] = append(m.messages[round], pbMsg.Label)
	m.mu.Unlock()

	m.logger.Debug("Received message",
		zap.String("from", pbMsg.From),
		zap.Int("round", round),
		zap.Binary("label", pbMsg.Label))
	return nil
}

// broadcast sends a message to all network peers.
func (m *MDAG) broadcast(round int, label []byte) {
	pbMsg := &mdagpb.MDAGMessage{
		SessionId: m.sessionID,
		Round:     uint32(round),
		Label:     label,
		From:      m.network.GetNodeID(),
	}

	data, err := proto.Marshal(pbMsg)
	if err != nil {
		m.logger.Error("Failed to marshal message", zap.Error(err))
		return
	}

	fullProtocolID := fmt.Sprintf("%s/%s", protocolID, m.sessionID)
	m.network.SendProtocolMessage(fullProtocolID, data)

	if m.logger.Core().Enabled(zap.DebugLevel) {
		m.logger.Debug("Broadcast message",
			zap.Int("round", round),
			zap.Binary("label", label))
	}
}

// Verify checks if a Merkle path is valid with respect to any of the target labels.
//
// Algorithm:
// 1. Compute m0 ← H(sort(L0)).
// 2. For all i in {1, ..., n} compute mi ← H(sort({mi−1} ∪ Li)).
// 3. Return true if the final computed label is in the set of target labels.
//
// Parameters:
//   - path: A sequence of label sets, each containing labels from one level of the path
//   - targetLabels: Map of target labels to verify against
//
// Returns true if the path is valid, false otherwise.
func (m *MDAG) Verify(path [][][]byte, targetLabels map[string]bool) bool {
	if len(path) == 0 {
		m.logger.Error("Empty path provided for verification")
		return false
	}

	if len(targetLabels) == 0 {
		m.logger.Error("No target labels provided for verification")
		return false
	}

	var buffer bytes.Buffer
	buffer.Grow(1024)

	// Step 1: Compute m0 ← H(sort(L0))
	L0 := path[0]
	sortedL0 := make([][]byte, len(L0))
	copy(sortedL0, L0)
	sort.Slice(sortedL0, func(i, j int) bool {
		return bytes.Compare(sortedL0[i], sortedL0[j]) < 0
	})

	buffer.Reset()
	for _, lab := range sortedL0 {
		buffer.Write(lab)
	}

	currentLabel := m.oracle(buffer.Bytes())

	// Step 2: For all i in {1, ..., n} compute mi ← H(sort({mi−1} ∪ Li))
	for i := 1; i < len(path); i++ {
		Li := path[i]

		labelSet := make([][]byte, len(Li)+1)
		labelSet[0] = currentLabel
		copy(labelSet[1:], Li)

		sort.Slice(labelSet, func(i, j int) bool {
			return bytes.Compare(labelSet[i], labelSet[j]) < 0
		})

		buffer.Reset()
		for _, lab := range labelSet {
			buffer.Write(lab)
		}

		currentLabel = m.oracle(buffer.Bytes())
	}

	// Step 3: Return true if the final computed label is in the set of target labels
	result := targetLabels[string(currentLabel)]

	if result {
		m.logger.Info("Merkle path verification successful",
			zap.Binary("computed_label", currentLabel))
	} else {
		m.logger.Warn("Merkle path verification failed",
			zap.Binary("computed_label", currentLabel))
	}

	return result
}

// GetComputedLabel returns the computed label for a specific round.
//
// Parameters:
//   - roundIndex: The round index (0-based)
//
// Returns the label for the specified round or nil if the index is invalid.
func (m *MDAG) GetComputedLabel(roundIndex int) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()

	if roundIndex < 0 || roundIndex >= m.rounds {
		m.logger.Warn("Invalid round index for GetComputedLabel",
			zap.Int("requested_index", roundIndex),
			zap.Int("max_valid_index", m.rounds-1))
		return nil
	}

	if roundIndex < len(m.computedLabels) {
		return m.computedLabels[roundIndex]
	}

	return nil
}
