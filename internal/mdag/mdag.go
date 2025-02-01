package mdag

import (
	"bytes"
	"encoding/hex"
	"errors"
	"sort"
	"strings"
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

type MerkleDAG interface {
	Gen(sid, vki string, vi ...string) (map[int][][]byte, error)
}

type MDAG struct {
	rounds       int             // total number of rounds (round 1...rounds; round 0 is initialization)
	oracle       HashOracle      // hash oracle (random oracle)
	network      network.Network // network interface for asynchronous messaging
	roundTimeout time.Duration   // time to wait each round before computing the next label
	logger       *zap.Logger     // zap logger for logging events
	isRunning    bool            // flag indicating if the protocol is running

	mu             sync.Mutex
	messages       map[int][][]byte // messages received from the network, keyed by round number
	state          [][][]byte       // state: bucket S_{i,r} of labels received in round (r-1)
	computedLabels [][]byte         // computed label for each round (r >= 1)
	currentLabel   []byte           // label computed in the most recent round
	sessionID      string           // current protocol session id
}

// New constructs and returns a new MDAG instance.
// The MDAG instance is initialized with the given number of rounds, hash oracle, network interface, round timeout, and logger.
func New(rounds int, oracle HashOracle, network network.Network, roundTimeout time.Duration, logger *zap.Logger) *MDAG {
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
	}
	// Register the message handler for the "mdag-protocol".
	network.RegisterHandler(protocolID, m.handleMessage)
	m.logger.Info("MDAG instance created", zap.Int("rounds", rounds))
	return m
}

// Gen implements the MerkleDAG interface.
// It runs the protocol, broadcasting the initial label (round 0) and then for each round r = 1...rounds:
//   - waits for incoming messages from round (r–1)
//   - sorts and concatenates them
//   - applies the hash oracle to compute a new label
//   - stores the bucket (state) and broadcasts the new label
//
// It returns the state as a map from round number to the slice of raw labels ([]byte).
func (m *MDAG) Gen(sid, vki string, vi ...string) ([][][]byte, error) {
	// Save the session id.
	m.sessionID = sid

	// Compute the initial label: sid || vki || (concatenation of vi)
	input := sid + vki + strings.Join(vi, "")
	m.currentLabel = []byte(input)
	m.mu.Lock()
	m.messages[0] = append(m.messages[0], m.currentLabel)
	m.mu.Unlock()

	m.logger.Info("Starting MDAG protocol",
		zap.String("session_id", sid),
		zap.String("node_id", m.network.GetNodeID()))

	// Broadcast the initial label (round 0).
	m.broadcast(0, m.currentLabel)

	// Process rounds 1 through m.rounds.
	for r := 1; r <= m.rounds; r++ {
		m.logger.Info("Starting round", zap.Int("round", r))
		// Wait for messages to arrive for the previous round.
		time.Sleep(m.roundTimeout)

		// Retrieve and remove the bucket of messages from round r-1.
		m.mu.Lock()
		bucket, found := m.messages[r-1]
		if !found {
			m.logger.Warn("No messages received for round", zap.Int("round", r-1))
			bucket = [][]byte{}
		}
		m.mu.Unlock()

		// Sort the received labels in canonical order.
		sortedLabels := make([][]byte, len(bucket))
		copy(sortedLabels, bucket)
		sort.Slice(sortedLabels, func(i, j int) bool {
			return bytes.Compare(sortedLabels[i], sortedLabels[j]) < 0
		})

		// Concatenate the sorted labels.
		var concatenated []byte
		for _, lab := range sortedLabels {
			concatenated = append(concatenated, lab...)
		}

		// Compute the new label using the oracle.
		newLabel := m.oracle(concatenated)

		// Save the bucket (state) and computed label.
		m.state[r-1] = bucket
		m.computedLabels[r-1] = newLabel
		m.currentLabel = newLabel

		// Broadcast the new label with round r.
		if r < m.rounds {
			m.broadcast(r, newLabel)
		}
		m.logger.Info("Completed round",
			zap.Int("round", r),
			zap.String("new_label", string(newLabel)))
	}

	// Mark the protocol as completed.
	m.mu.Lock()
	m.isRunning = false
	m.mu.Unlock()
	m.logger.Info("MDAG protocol completed")
	return m.state, nil
}

// handleMessage processes incoming messages from the network.
func (m *MDAG) handleMessage(_ string, payload []byte) error {
	// Check if the protocol is running to prevent processing messages after completion.
	// Adversaries may still send messages after the protocol has completed.
	m.mu.Lock()
	if !m.isRunning {
		m.logger.Warn("Received message while protocol is not running")
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	var pbMsg mdagpb.MDAGMessage
	if err := proto.Unmarshal(payload, &pbMsg); err != nil {
		m.logger.Error("Failed to unmarshal message", zap.Error(err))
		return err
	}

	// Check that the session id matches (if already set).
	if m.sessionID != "" && pbMsg.SessionId != m.sessionID {
		err := errors.New("session id mismatch")
		m.logger.Warn("Received message with mismatched session id",
			zap.String("expected", m.sessionID),
			zap.String("received", pbMsg.SessionId))
		return err
	}

	// Decode the hex-encoded label.
	lab := []byte(pbMsg.Label)
	round := int(pbMsg.Round)
	m.mu.Lock()
	m.messages[round] = append(m.messages[round], lab)
	m.mu.Unlock()
	m.logger.Debug("Received message",
		zap.String("from", pbMsg.From),
		zap.Int("round", round),
		zap.String("label", pbMsg.Label))
	return nil
}

// broadcast sends a message over the network using protobuf.
func (m *MDAG) broadcast(round int, label []byte) {
	pbMsg := &mdagpb.MDAGMessage{
		SessionId: m.sessionID,
		Round:     uint32(round),
		Label:     hex.EncodeToString(label),
		From:      m.network.GetNodeID(),
	}
	data, err := proto.Marshal(pbMsg)
	if err != nil {
		m.logger.Error("Failed to marshal message", zap.Error(err))
		return
	}
	m.network.SendProtocolMessage(protocolID, data)
	m.logger.Debug("Broadcast message",
		zap.Int("round", round),
		zap.String("label", pbMsg.Label))
}
