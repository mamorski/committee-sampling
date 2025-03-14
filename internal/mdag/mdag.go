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

// New constructs and returns a new MDAG instance.
// It initializes the MDAG protocol with the specified parameters and registers a message handler.
//
// Parameters:
//   - rounds: The total number of rounds to run in the protocol (excluding initialization round 0)
//   - sid: Session identifier for this protocol instance
//   - oracle: Hash oracle function that implements the random oracle model
//   - network: Network interface for sending and receiving messages
//   - roundTimeout: Duration to wait for each round before proceeding
//   - logger: Structured logger for recording protocol events
//   - startTime: Time when the protocol should start execution
//
// The function also initializes internal data structures and registers a message handler
// with the network interface. It stores the list of neighbors from the network for
// message validation.
//
// Returns a configured MDAG instance ready to run the protocol.
func New(rounds int, sid string, oracle HashOracle, network network.Network, roundTimeout time.Duration, logger *zap.Logger, startTime time.Time) *MDAG {
	// Get the list of neighbors from the network
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
		isRunning:      true,
		sessionID:      sid,
		startTime:      startTime,
		neighbors:      neighbors,
	}

	// Register the message handler for the "mdag-protocol".
	network.RegisterHandler(fmt.Sprintf("%s/%s", protocolID, sid), m.handleMessage)
	m.logger.Info("MDAG instance created",
		zap.Int("rounds", rounds),
		zap.Int("neighbors", len(neighbors)))
	return m
}

// Generate runs the MDAG protocol for the specified session ID and inputs.
// It returns the state (sequence of labels received from neighbors during all rounds)
// and any error that occurred during the protocol.
func (m *MDAG) Generate(sid string, vki []byte, vi ...[]byte) ([][][]byte, error) {
	// Check if the protocol is already running
	m.mu.Lock()
	if m.isRunning {
		m.mu.Unlock()
		return nil, fmt.Errorf("protocol is already running")
	}

	// Check if the session ID matches
	if m.sessionID != "" && m.sessionID != sid {
		m.mu.Unlock()
		return nil, fmt.Errorf("session ID mismatch: expected %s, got %s", m.sessionID, sid)
	}

	// Initialize the protocol state
	m.isRunning = true
	m.sessionID = sid
	m.messages = make(map[int][][]byte)
	m.state = make([][][]byte, m.rounds)
	m.computedLabels = make([][]byte, 0, m.rounds)
	m.mu.Unlock()

	// Log the start of the protocol
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
			m.mu.Unlock()
			m.logger.Warn("No messages received in previous round", zap.Int("round", r-1))
			continue
		}

		// Create a bucket for this round's state
		bucket := make([][]byte, len(prevRoundMsgs))
		copy(bucket, prevRoundMsgs)
		m.state[r-1] = bucket

		// MODIFIED: Compute the new label using the Verify algorithm approach
		// For round 1, compute m0 ← H(sort(L0))
		if r == 1 {
			// Sort the labels from round 0
			sortedLabels := make([][]byte, len(prevRoundMsgs))
			copy(sortedLabels, prevRoundMsgs)
			sort.Slice(sortedLabels, func(i, j int) bool {
				return bytes.Compare(sortedLabels[i], sortedLabels[j]) < 0
			})

			// Concatenate and hash
			var concatenated []byte
			for _, lab := range sortedLabels {
				concatenated = append(concatenated, lab...)
			}
			newLabel := m.oracle(concatenated)
			m.computedLabels = append(m.computedLabels, newLabel)
			m.currentLabel = newLabel
		} else {
			// For rounds > 1, compute mi ← H(sort({mi-1} ∪ Li))
			// Include the previous round's computed label
			labelSet := make([][]byte, len(prevRoundMsgs)+1)
			labelSet[0] = m.currentLabel
			copy(labelSet[1:], prevRoundMsgs)

			// Sort the labels
			sort.Slice(labelSet, func(i, j int) bool {
				return bytes.Compare(labelSet[i], labelSet[j]) < 0
			})

			// Concatenate and hash
			var concatenated []byte
			for _, lab := range labelSet {
				concatenated = append(concatenated, lab...)
			}
			newLabel := m.oracle(concatenated)
			m.computedLabels = append(m.computedLabels, newLabel)
			m.currentLabel = newLabel
		}

		m.mu.Unlock()

		// Log the completion of this round
		m.logger.Info("Completed round", zap.Int("round", r), zap.String("new_label", base64.StdEncoding.EncodeToString(m.currentLabel)))

		// Broadcast the new label if not the last round
		if r < m.rounds {
			m.broadcast(r, m.currentLabel)
		}
	}

	// Mark the protocol as completed
	m.mu.Lock()
	m.isRunning = false

	// Create a copy of the state to return
	stateCopy := make([][][]byte, len(m.state))
	for i, bucket := range m.state {
		if bucket != nil {
			stateCopy[i] = make([][]byte, len(bucket))
			copy(stateCopy[i], bucket)
		}
	}
	m.mu.Unlock()

	m.logger.Info("MDAG protocol completed")
	return stateCopy, nil
}

// handleMessage processes incoming messages from the network.
// It validates and stores messages received from neighbors for use in the protocol.
//
// The function performs several validation steps:
// 1. Verifies the protocol is running
// 2. Validates the message format via protobuf unmarshaling
// 3. Confirms the message is from a known neighbor
// 4. Checks that the session ID matches
//
// The implementation uses optimized lock management to reduce contention:
// - Quick checks are performed without holding locks
// - The lock is only acquired when updating shared state
// - Pre-allocation is used to reduce memory allocations
//
// Parameters:
//   - _: Protocol ID (unused but required by the handler interface)
//   - payload: Raw message bytes received from the network
//
// Returns an error if any validation fails, nil otherwise.
func (m *MDAG) handleMessage(_ string, payload []byte) error {
	// Quick check if the protocol is running without holding the lock
	// This is a performance optimization that avoids lock contention
	// It's safe because isRunning only transitions from true to false, never back
	if !m.isRunning {
		return errors.New("protocol not running")
	}

	var pbMsg mdagpb.MDAGMessage
	if err := proto.Unmarshal(payload, &pbMsg); err != nil {
		m.logger.Error("Failed to unmarshal message", zap.Error(err))
		return err
	}

	// Check if the message is from a known neighbor
	// This is a fast check that doesn't require locking
	if !m.neighbors[pbMsg.From] {
		err := errors.New("message from unknown neighbor")
		m.logger.Warn("Received message from unknown neighbor",
			zap.String("from", pbMsg.From))
		return err
	}

	// Check that the session id matches (if already set).
	// This is also a fast check that doesn't require locking
	if m.sessionID != "" && pbMsg.SessionId != m.sessionID {
		err := errors.New("session id mismatch")
		m.logger.Warn("Received message with mismatched session id",
			zap.String("expected", m.sessionID),
			zap.String("received", pbMsg.SessionId))
		return err
	}

	// Now do a proper check with the lock to ensure the protocol is still running
	m.mu.Lock()
	if !m.isRunning {
		m.mu.Unlock()
		m.logger.Warn("Received message while protocol is not running")
		return errors.New("protocol not running")
	}

	// Process the message
	round := int(pbMsg.Round)

	// Append the message to the appropriate round
	// Pre-allocate the slice if it doesn't exist to reduce allocations
	if _, exists := m.messages[round]; !exists {
		m.messages[round] = make([][]byte, 0, 16) // Initial capacity of 16
	}
	m.messages[round] = append(m.messages[round], pbMsg.Label)
	m.mu.Unlock()

	m.logger.Debug("Received message",
		zap.String("from", pbMsg.From),
		zap.Int("round", round),
		zap.Binary("label", pbMsg.Label))
	return nil
}

// broadcast sends a message over the network using protobuf.
func (m *MDAG) broadcast(round int, label []byte) {
	// Pre-allocate the message structure to reduce allocations
	pbMsg := &mdagpb.MDAGMessage{
		SessionId: m.sessionID,
		Round:     uint32(round),
		Label:     label,
		From:      m.network.GetNodeID(),
	}

	// Marshal the message once
	data, err := proto.Marshal(pbMsg)
	if err != nil {
		m.logger.Error("Failed to marshal message", zap.Error(err))
		return
	}

	// Send the message
	m.network.SendProtocolMessage(protocolID, data)

	// Log at debug level to reduce overhead in production
	if m.logger.Core().Enabled(zap.DebugLevel) {
		m.logger.Debug("Broadcast message",
			zap.Int("round", round),
			zap.Binary("label", label))
	}
}

// Verify checks if a given Merkle path is valid with respect to any of the target labels.
// The path P is encoded as a sequence of label sets P = (L0, ..., Ln),
// where Li = {ℓi,1, ..., ℓi,ki} are the labels of the leaves adjacent to the i-th vertex on the central path.
// The algorithm verifies if P encodes a valid Merkle path with respect to any of the target labels.
//
// Algorithm:
// 1. Compute m0 ← H(sort(L0)).
// 2. For all i in {1, ..., n+1} compute mi ← H(sort({mi−1} ∪ Li)).
// 3. Return true iff mn+1 is in the set of target labels.
//
// Parameters:
// - path: A sequence of label sets, where each set contains the labels of adjacent leaves
// - targetLabels: A map of target labels to verify against (using map for O(1) lookup)
//
// Returns:
// - true if the path is valid with respect to any of the target labels, false otherwise
func (m *MDAG) Verify(path [][][]byte, targetLabels map[string]bool) bool {
	if len(path) == 0 {
		m.logger.Error("Empty path provided for verification")
		return false
	}

	if len(targetLabels) == 0 {
		m.logger.Error("No target labels provided for verification")
		return false
	}

	// Pre-allocate a buffer for concatenation with a reasonable initial capacity
	var buffer bytes.Buffer
	buffer.Grow(1024) // Initial capacity - adjust based on expected label sizes

	// Step 1: Compute m0 ← H(sort(L0))
	L0 := path[0]
	sortedL0 := make([][]byte, len(L0))
	copy(sortedL0, L0)
	sort.Slice(sortedL0, func(i, j int) bool {
		return bytes.Compare(sortedL0[i], sortedL0[j]) < 0
	})

	// Concatenate the sorted labels efficiently
	buffer.Reset()
	for _, lab := range sortedL0 {
		buffer.Write(lab)
	}

	// Compute m0
	currentLabel := m.oracle(buffer.Bytes())

	// Step 2: For all i in {1, ..., n+1} compute mi ← H(sort({mi−1} ∪ Li))
	for i := 1; i < len(path); i++ {
		Li := path[i]

		// Create a new set with mi-1 and all labels in Li
		labelSet := make([][]byte, len(Li)+1)
		labelSet[0] = currentLabel
		copy(labelSet[1:], Li)

		// Sort the combined set
		sort.Slice(labelSet, func(i, j int) bool {
			return bytes.Compare(labelSet[i], labelSet[j]) < 0
		})

		// Concatenate the sorted labels efficiently
		buffer.Reset()
		for _, lab := range labelSet {
			buffer.Write(lab)
		}

		// Compute mi
		currentLabel = m.oracle(buffer.Bytes())
	}

	// Step 3: Return true iff mn+1 is in the set of target labels
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

// GetComputedLabel returns the computed label for a specific round index.
// Round index should be between 0 and rounds-1, where 0 corresponds to the initial label
// and rounds-1 corresponds to the final label.
//
// Parameters:
//   - roundIndex: The round index for which to retrieve the computed label (0-based)
//
// Returns:
//   - The computed label for the specified round, or nil if the round index is invalid
//     or the label has not been computed yet
func (m *MDAG) GetComputedLabel(roundIndex int) []byte {
	// Acquire lock to safely access the computed labels
	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if the round index is valid
	if roundIndex < 0 || roundIndex >= m.rounds {
		m.logger.Warn("Invalid round index for GetComputedLabel",
			zap.Int("requested_index", roundIndex),
			zap.Int("max_valid_index", m.rounds-1))
		return nil
	}

	// Return the computed label for the requested round
	if roundIndex < len(m.computedLabels) {
		return m.computedLabels[roundIndex]
	}

	return nil
}
