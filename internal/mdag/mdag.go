package mdag

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/network"
	mdagpb "github.com/mamorski/committee-sampling/pkg/proto"

	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const protocolID = "/mdag/1.0.0"

// HashOracle defines the interface for the hashing function
type HashOracle func([]byte) []byte

type MDAG struct {
	rounds         int                 // total number of rounds
	oracle         HashOracle          // hash oracle (random oracle)
	network        network.Network     // network interface for asynchronous messaging
	synchronizer   common.Synchronizer // synchronizer for round timing
	logger         *zap.Logger         // zap logger for logging events
	mu             sync.Mutex
	messages       map[int][][]byte // messages received from the network, keyed by a round number
	state          [][][]byte       // state: bucket S_{i,r} of labels received in round (r-1)
	computedLabels [][]byte         // computed label for each round (r >= 1)
	currentLabel   []byte           // label computed in the most recent round
	sessionID      string           // current protocol session id
	protocolType   string           // type of the protocol (e.g., "ExPost", "ExAnte")
	step           common.Step      // synchronizer step (ExPostMDAG or ExAnteMDAG)

	stats     common.StatsRecorder // sink for message/round statistics
	isRunning bool                 // flag indicating if the protocol is running
}

// New creates a new MDAG instance with the specified parameters.
//
// Parameters:
//   - rounds: Number of rounds to run in the protocol (excluding initialization round 0)
//   - sid: Session identifier for this protocol instance
//   - oracle: Hash oracle function used for computing labels
//   - network: Network interface for message passing
//   - synchronizer: Synchronizer for round timing
//   - logger: Structured logger for recording events
//   - step: A synchronizer step (common.ExPostMDAG or common.ExAnteMDAG)
//
// Returns a configured MDAG instance ready to run the protocol.
func New(
	rounds int,
	sid string,
	oracle HashOracle,
	network network.Network,
	synchronizer common.Synchronizer,
	logger *zap.Logger,
	step common.Step,
	protocolType string,
	statsRec common.StatsRecorder,
) *MDAG {

	if statsRec == nil {
		statsRec = common.NoopRecorder{}
	}

	m := &MDAG{
		rounds:         rounds,
		oracle:         oracle,
		network:        network,
		synchronizer:   synchronizer,
		logger:         logger.Named(protocolType),
		messages:       make(map[int][][]byte),
		computedLabels: make([][]byte, rounds+1),
		state:          make([][][]byte, rounds),
		isRunning:      false,
		sessionID:      sid,
		step:           step,
		protocolType:   protocolType,
		stats:          statsRec,
	}

	network.RegisterHandler(fmt.Sprintf("%s/%s/%s", protocolID, protocolType, sid), m.handleMessage)
	m.logger.Info(
		"MDAG instance created", zap.Int("rounds", rounds),
	)
	return m
}

// Generate runs the MDAG protocol and produces a sequence of labels.
//
// The protocol follows these steps:
// 1. Compute the initial label from session ID, verification key, and additional inputs
// 2. For each round, collect messages from the previous round and compute a new label
// 3. Broadcast the new label to all peers
//
// Parameters:
//   - sid: Session identifier for this run
//   - vki: Verification key
//   - vi: Additional inputs for the initial label
//
// Returns the state (sequence of labels received in each round) and any error.
func (m *MDAG) Generate(sid string, vki []byte, vi ...[]byte) ([][][]byte, error) {
	m.logger.Info("Generate started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		m.logger.Info(
			"Generate completed", zap.Duration("elapsed", elapsed),
		)
	}()

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

	// Compute the initial label: sid || vki || (concatenation of vi)
	var buffer bytes.Buffer
	buffer.WriteString(sid)
	buffer.Write(vki)
	for _, v := range vi {
		buffer.Write(v)
	}
	m.currentLabel = m.oracle(buffer.Bytes())
	m.computedLabels[0] = m.currentLabel

	// Wait for round 0 synchronization before broadcasting
	waitChan, err := m.synchronizer.WaitForRound(m.step, 0)
	if err != nil {
		m.mu.Lock()
		m.isRunning = false
		m.mu.Unlock()
		return nil, fmt.Errorf("failed to wait for round 0: %w", err)
	}
	<-waitChan

	// Broadcast the initial label
	m.broadcast(0, m.currentLabel)

	// Run the protocol for rounds 1 to m.rounds
	for r := 1; r <= m.rounds; r++ {
		// Wait for round r synchronization
		waitChan, err := m.synchronizer.WaitForRound(m.step, r)
		if err != nil {
			m.mu.Lock()
			m.isRunning = false
			m.mu.Unlock()
			return nil, fmt.Errorf("failed to wait for round %d: %w", r, err)
		}
		<-waitChan

		start := time.Now()
		m.logger.Info("Starting round", zap.Int("round", r))

		// Lock to safely access messages
		m.mu.Lock()
		// Get messages received in the previous round
		prevRoundMsgs, exists := m.messages[r-1]
		if !exists || len(prevRoundMsgs) == 0 {
			m.logger.Warn("No messages received in previous round", zap.Int("round", r-1))
		}
		m.mu.Unlock()

		sortedLabels := make([][]byte, len(prevRoundMsgs)+1)
		sortedLabels[0] = m.currentLabel
		copy(sortedLabels[1:], prevRoundMsgs)

		// Sort the labels
		sort.Slice(
			sortedLabels, func(i, j int) bool {
				return bytes.Compare(sortedLabels[i], sortedLabels[j]) < 0
			},
		)
		m.state[r-1] = sortedLabels

		// Concatenate and hash
		var concatenated []byte
		for _, lab := range sortedLabels {
			concatenated = append(concatenated, lab...)
		}
		newLabel := m.oracle(concatenated)
		m.currentLabel = newLabel
		m.computedLabels[r] = newLabel

		if r < m.rounds {
			m.broadcast(r, m.currentLabel)
		}
		elapsed := time.Since(start)
		m.logger.Info(
			"Finished processing messages for round",
			zap.Int("round", r),
			zap.Duration("elapsed", elapsed),
			zap.Binary("new_label", m.currentLabel),
			zap.Int("num_messages", len(sortedLabels)),
		)
	}

	// Mark the protocol as completed
	m.mu.Lock()
	m.isRunning = false
	m.mu.Unlock()

	return m.state, nil
}

// handleMessage processes incoming messages from the network.
//
// The function validates that the message is from a known neighbor, has a matching session ID,
// and is received while the protocol is running.
//
// Parameters:
//   - from: The peer ID of the sender
//   - payload: Raw message bytes received from the network
//
// Returns an error if validation fails, nil otherwise.
func (m *MDAG) handleMessage(from peer.ID, payload []byte) error {
	m.mu.Lock()
	running := m.isRunning
	m.mu.Unlock()

	if !running {
		return errors.New("protocol not running")
	}

	var pbMsg mdagpb.MDAGMessage
	if err := proto.Unmarshal(payload, &pbMsg); err != nil {
		m.logger.Error("Failed to unmarshal message", zap.Error(err))
		return err
	}

	round := int(pbMsg.Round)
	if round < 0 || round > m.rounds {
		m.logger.Warn(
			"Received message with invalid round number", zap.Int("round", round), zap.Int("max_rounds", m.rounds),
		)
		return nil
	}

	// Record total message received
	m.stats.RecordReceived(m.protocolType, m.step, round, time.Now())

	if !m.network.IsNeighbor(from) {
		err := errors.New("message from unknown neighbor")
		m.logger.Warn(
			"Received message from unknown neighbor", zap.String("from", from.String()),
		)
		return err
	}

	if m.sessionID != "" && pbMsg.SessionId != m.sessionID {
		err := errors.New("session id mismatch")
		m.logger.Warn(
			"Received message with mismatched session id", zap.String("expected", m.sessionID), zap.String("received", pbMsg.SessionId),
		)
		return err
	}

	m.mu.Lock()
	if _, exists := m.messages[round]; !exists {
		m.messages[round] = make([][]byte, 0, 16)
	}
	m.messages[round] = append(m.messages[round], pbMsg.Label)
	m.mu.Unlock()

	// Record valid message - passed all validation checks
	m.stats.RecordValid(m.protocolType, m.step, round)

	if m.logger.Core().Enabled(zap.DebugLevel) {
		m.logger.Debug(
			"Received message", zap.String("from", from.String()), zap.Int("round", round), zap.Binary("label", pbMsg.Label),
		)
	}
	return nil
}

// broadcast sends a message to all network peers.
func (m *MDAG) broadcast(round int, label []byte) {
	pbMsg := &mdagpb.MDAGMessage{
		SessionId: m.sessionID,
		Round:     uint32(round), //nolint:gosec
		Label:     label,
		Id:        m.network.GetNodeID(),
	}

	data, err := proto.Marshal(pbMsg)
	if err != nil {
		m.logger.Error("Failed to marshal message", zap.Error(err))
		return
	}

	fullProtocolID := fmt.Sprintf("%s/%s/%s", protocolID, m.protocolType, m.sessionID)
	m.network.SendProtocolMessage(fullProtocolID, data)

	if m.logger.Core().Enabled(zap.DebugLevel) {
		m.logger.Debug(
			"Broadcast message", zap.Int("round", round), zap.Binary("label", label),
		)
	}
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

	if roundIndex < 0 || roundIndex > m.rounds {
		m.logger.Warn(
			"Invalid round index for GetComputedLabel", zap.Int("requested_index", roundIndex), zap.Int("max_valid_index", m.rounds),
		)
		return nil
	}

	if roundIndex < len(m.computedLabels) {
		return m.computedLabels[roundIndex]
	}

	return nil
}

// oracleBufPool reuses concat buffers for Oracle, which is called per merkle layer
// per gossiped message. The previous per-call bytes.Buffer allocation was pure GC
// churn. m.oracle copies its input into a fresh digest slice, so the buffer can be
// returned to the pool immediately after.
var oracleBufPool = sync.Pool{
	New: func() any { return new(bytes.Buffer) },
}

func (m *MDAG) Oracle(h ...[]byte) []byte {
	buf := oracleBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	for _, v := range h {
		buf.Write(v)
	}

	out := m.oracle(buf.Bytes())
	oracleBufPool.Put(buf)

	return out
}
