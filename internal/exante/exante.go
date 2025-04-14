package exante

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/network"
	pb "github.com/mamorski/committee-sampling/pkg/proto"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const exanteProtocolID = "/exante/1.0.0"

// MDAG defines the interface for the MDAG required by ExAnte
type MDAG interface {
	Generate(sid string, vk []byte, vi ...[]byte) ([][][]byte, error)
	GetStateForRound(round int) [][]byte
	GetComputedLabel(roundIndex int) []byte
	Verify(merkleRoot []byte, path [][]byte) bool
}

// ExAnte implements the Ex-Ante Timestamp protocol
type ExAnte struct {
	network       network.Network
	logger        *zap.Logger
	mdag          MDAG
	roundTimeout  time.Duration
	startTime     time.Time
	diameter      int
	gradeFunction common.GradeFunc
	isRunning     bool
	sid           string
	challenge     []byte

	mu               sync.Mutex
	verifiedValues   map[string]map[string]*verifiedTuple
	processedSenders map[string]map[uint32]bool
	messages         map[int]map[string]receivedMessage
	neighbors        map[string]bool // set of allowed neighbor node IDs
}

type verifiedTuple struct {
	verificationKey []byte
	value           []byte
	auxKey          *common.AuxKey
	grade           int
}

type receivedMessage struct {
	sid        string
	vk         []byte
	ch         []byte
	auxKey     *common.AuxKey
	merklePath [][][]byte
}

// New creates a new ExAnte instance with the specified parameters
func New(
	net network.Network,
	mdag MDAG,
	sid string,
	startTime time.Time,
	roundTimeout time.Duration,
	diameter int,
	gradeFunction common.GradeFunc,
	logger *zap.Logger) *ExAnte {

	e := &ExAnte{
		network:          net,
		logger:           logger,
		mdag:             mdag,
		roundTimeout:     roundTimeout,
		startTime:        startTime,
		diameter:         diameter,
		gradeFunction:    gradeFunction,
		verifiedValues:   make(map[string]map[string]*verifiedTuple),
		processedSenders: make(map[string]map[uint32]bool),
		messages:         make(map[int]map[string]receivedMessage),
		neighbors:        make(map[string]bool),
		sid:              sid,
		isRunning:        true,
	}

	neighborsList := net.GetNeighbors()
	for _, neighbor := range neighborsList {
		e.neighbors[neighbor] = true
	}

	// Register message handler
	protocolID := fmt.Sprintf("%s/%s", exanteProtocolID, sid)
	net.RegisterHandler(protocolID, e.handleMessage)

	e.logger.Info("ExAnte instance created",
		zap.String("session_id", sid),
		zap.Int("diameter", diameter))

	return e
}

// Generate implements the ExAnte Generate method using MDAG
func (e *ExAnte) Generate(session string, vk []byte, challenge []byte, rpProof []byte) ([][][]byte, error) {

	if e.sid != "" && e.sid != session {
		return nil, fmt.Errorf("session ID mismatch: expected %s, got %s", e.sid, session)
	}

	e.logger.Info("Starting ExAnte Generation phase",
		zap.String("session_id", session),
		zap.String("node_id", e.network.GetNodeID()))

	state, err := e.mdag.Generate(session, vk, challenge, rpProof)
	if err != nil {
		e.isRunning = false
		return nil, fmt.Errorf("MDAG generation failed: %w", err)
	}

	e.challenge = challenge
	e.logger.Info("ExAnte Generation phase completed")
	return state, nil
}

// Verify implements the ExAnte Verify method
func (e *ExAnte) Verify(
	session string,
	vk []byte,
	sigma [][][]byte,
	auxTag *common.AuxTag,
	auxLocal float64,
	filterFn common.FilterTagF) (map[common.Key]common.O, error) {

	if e.sid != session {
		return nil, fmt.Errorf("session ID mismatch: expected %s, got %s", e.sid, session)
	}

	if len(sigma) == 0 {
		e.isRunning = false
		e.logger.Error("Empty sigma received")
		return nil, fmt.Errorf("empty sigma")
	}

	e.logger.Info("Starting ExAnte Verification phase",
		zap.String("session_id", session),
		zap.String("node_id", e.network.GetNodeID()))

	// Check if P_i is also acts like a prover
	if e.gradeFunction(session, vk, auxTag.AuxKey.PhiVRF, auxTag.AuxKey, auxLocal) >= e.diameter+1 &&
		filterFn(session, vk, auxTag.AuxKey.PhiVRF, auxTag) {

		e.logger.Info("Node is a prover, sending initial message")

		msg := &pb.ExAnteMessage{
			SessionId:       session,
			VerificationKey: vk,
			Value:           e.challenge,
			AuxKey: &pb.AuxKeyMessage{
				PhiVrf: auxTag.AuxKey.PhiVRF,
				PiVrf:  auxTag.AuxKey.PiVRF,
				PhiVdf: auxTag.AuxKey.PhiVDF,
				PiVdf:  auxTag.AuxKey.PiVDF,
			},
			MerklePath: nil,
			Round:      0,
			From:       e.network.GetNodeID(),
		}

		msg.MerklePath = make([]*pb.State, 1)
		msg.MerklePath[0] = &pb.State{
			Row: sigma[0],
		}

		msgBytes, err := proto.Marshal(msg)
		if err != nil {
			e.isRunning = false
			e.logger.Error("Failed to marshal initial message", zap.Error(err))
			return nil, fmt.Errorf("failed to marshal initial message: %w", err)
		}

		protocolID := fmt.Sprintf("%s/%s", exanteProtocolID, session)
		e.network.SendProtocolMessage(protocolID, msgBytes)
	}

	for r := 1; r <= e.diameter; r++ {
		time.Sleep(time.Until(e.startTime.Add(time.Duration(r) * e.roundTimeout)))
		e.logger.Info("ExAnte verification round", zap.Int("round", r))
		time.Sleep(e.roundTimeout)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	sessionValues := e.verifiedValues[session]
	results := make(map[common.Key]common.O)

	maxGrades := make(map[string]int)
	for vkString, tuple := range sessionValues {
		if maxGrade, exists := maxGrades[vkString]; !exists || tuple.grade > maxGrade {
			maxGrades[vkString] = tuple.grade
		}
	}

	for vkString, tuple := range sessionValues {
		if tuple.grade == maxGrades[vkString] {
			key := common.Key{
				VK: string(tuple.verificationKey),
				Ch: string(tuple.value),
			}
			results[key] = common.O{
				VK:        tuple.verificationKey,
				Challenge: tuple.value,
				Aux:       tuple.auxKey,
				Grade:     tuple.grade,
			}
		}
	}

	e.isRunning = false
	e.logger.Info("ExAnte verification phase completed")

	return results, nil
}

// handleMessage processes incoming messages from the network.
func (e *ExAnte) handleMessage(from string, payload []byte) error {
	if !e.isRunning {
		return errors.New("protocol not running")
	}
	e.logger.Debug(fmt.Sprintf("Received message from %s", from))

	var msg pb.ExAnteMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		e.logger.Error("Failed to unmarshal ExAnte message", zap.Error(err))
		return err
	}

	if msg.SessionId != e.sid {
		err := errors.New("session id mismatch")
		e.logger.Warn("Received message with mismatched session id",
			zap.String("expected", e.sid),
			zap.String("received", msg.SessionId))
		return err
	}

	if msg.From != from {
		err := errors.New("sender id mismatch")
		e.logger.Error("Received message with mismatched sender id",
			zap.String("expected", from),
			zap.String("received", msg.From))
		return err
	}

	if !e.neighbors[from] {
		err := errors.New("sender not in neighbors list")
		e.logger.Error("Received message from non-neighbor sender",
			zap.String("sender", from))

		return err
	}

	round := int(msg.Round)

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.messages[round]; !exists {
		e.messages[round] = make(map[string]receivedMessage)
	}

	if _, exists := e.messages[round][from]; exists {
		e.logger.Debug("Ignoring duplicate message from sender for this round",
			zap.String("from", from),
			zap.Int("round", round))
		return nil
	}

	e.messages[round][from] = receivedMessage{
		sid: msg.SessionId,
		vk:  msg.VerificationKey,
		ch:  msg.Value,
		auxKey: &common.AuxKey{
			PhiVRF: msg.AuxKey.PhiVrf,
			PiVRF:  msg.AuxKey.PiVrf,
			PhiVDF: msg.AuxKey.PhiVdf,
			PiVDF:  msg.AuxKey.PiVdf,
		},
		merklePath: convertExAnteMessageToBytes(&msg),
	}

	return nil
}

// processMessage handles a single message according to the protocol logic
func (e *ExAnte) processMessage(from string, msg *pb.ExAnteMessage, auxKey *common.AuxKey) {
	e.logger.Debug("Processing message",
		zap.String("from", from),
		zap.String("session", msg.SessionId),
		zap.Uint32("round", msg.Round))

	if e.filterFunction(e.sid, msg.VerificationKey, msg.Value, auxKey) {
		grade := e.gradeFunction(e.sid, msg.VerificationKey, msg.Value, auxKey, 0)
		if grade <= 0 {
			return
		}

		vkValHash := append([]byte(e.sid), msg.VerificationKey...)
		vkValHash = append(vkValHash, msg.Value...)

		if !e.validateMerklePath(msg.MerklePath, vkValHash, int(msg.Round)) {
			return
		}

		computedGrade := int(math.Min(
			float64(e.diameter-int(msg.Round))/float64(e.diameter),
			float64(grade),
		))

		vkString := string(msg.VerificationKey)

		existing, exists := e.verifiedValues[e.sid][vkString]
		shouldUpdate := !exists || existing.grade < computedGrade

		if shouldUpdate {
			e.verifiedValues[e.sid][vkString] = &verifiedTuple{
				verificationKey: msg.VerificationKey,
				value:           msg.Value,
				auxKey:          auxKey,
				grade:           computedGrade,
			}
		}

		if msg.Round < uint32(e.diameter) {
			e.propagateMessage(msg, auxKey)
		}
	}
}

// propagateMessage forwards a message to neighbors with updated round and path
func (e *ExAnte) propagateMessage(msg *pb.ExAnteMessage, auxKey *common.AuxKey) {
	nextRound := msg.Round + 1
	nextPath := append(msg.MerklePath, e.mdag.GetComputedLabel(int(nextRound)))

	forwardMsg := &pb.ExAnteMessage{
		SessionId:       e.sid,
		VerificationKey: msg.VerificationKey,
		Value:           msg.Value,
		AuxKey:          msg.AuxKey,
		MerklePath:      nextPath,
		Round:           nextRound,
		From:            e.network.GetNodeID(),
	}

	msgBytes, err := proto.Marshal(forwardMsg)
	if err != nil {
		e.logger.Error("Failed to marshal ExAnte message", zap.Error(err))
		return
	}

	protocolID := fmt.Sprintf("%s/%s", exanteProtocolID, e.sid)
	e.network.SendProtocolMessage(protocolID, msgBytes)
}

func convertExAnteMessageToBytes(msg *pb.ExAnteMessage) [][][]byte {

	result := make([][][]byte, len(msg.MerklePath))

	for i, state := range msg.MerklePath {
		if state == nil {
			result[i] = nil
			continue
		}

		if state.Row == nil {
			result[i] = [][]byte{}
			continue
		}

		result[i] = make([][]byte, len(state.Row))
		for j, rowBytes := range state.Row {
			result[i][j] = rowBytes
		}
	}

	return result
}
