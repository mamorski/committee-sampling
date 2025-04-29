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
	Oracle(h ...[]byte) []byte
}

// ExAnte implements the Ex-Ante Timestamp protocol
type ExAnte struct {
	network       network.Network
	logger        *zap.Logger
	mdag          MDAG
	roundTimeout  time.Duration
	startTime     time.Time
	diameter      int
	D             int
	gradeFunction common.GradeFunc
	isRunning     bool
	sid           string
	challenge     []byte

	mu             sync.Mutex
	verifiedValues map[string]verifiedTuple
	messages       map[int]map[string]receivedMessage
	neighbors      map[string]bool // set of allowed neighbor node IDs
	state          [][][]byte
}

type verifiedTuple struct {
	vk    []byte
	v     []byte
	aux   *common.AuxTag
	grade int
}

type receivedMessage struct {
	sid        string
	vk         []byte
	ch         []byte
	aux        *common.AuxTag
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
	D int,
	gradeFunction common.GradeFunc,
	logger *zap.Logger) *ExAnte {

	e := &ExAnte{
		network:        net,
		logger:         logger,
		mdag:           mdag,
		roundTimeout:   roundTimeout,
		startTime:      startTime,
		diameter:       diameter,
		D:              D,
		gradeFunction:  gradeFunction,
		verifiedValues: make(map[string]verifiedTuple),
		messages:       make(map[int]map[string]receivedMessage),
		neighbors:      make(map[string]bool),
		sid:            sid,
		isRunning:      true,
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
func (e *ExAnte) Generate(session string, vk []byte, challenge []byte, piRP []byte) ([][][]byte, error) {

	if e.sid != "" && e.sid != session {
		return nil, fmt.Errorf("session ID mismatch: expected %s, got %s", e.sid, session)
	}

	e.logger.Info("Starting ExAnte Generation phase",
		zap.String("session_id", session),
		zap.String("node_id", e.network.GetNodeID()))

	state, err := e.mdag.Generate(session, vk, challenge, piRP)
	e.state = make([][][]byte, len(state))
	e.state = state
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
		filterFn(session, vk, e.challenge, auxTag) {

		e.logger.Info("Node is a prover, sending initial message")

		msg := &pb.ExAnteMessage{
			SessionId:       session,
			VerificationKey: vk,
			Value:           e.challenge,
			Aux: &pb.Aux{
				PiRP: auxTag.PiRP,
				AuxKey: &pb.AuxKeyMessage{
					PhiVrf: auxTag.AuxKey.PhiVRF,
					PiVrf:  auxTag.AuxKey.PiVRF,
					PhiVdf: auxTag.AuxKey.PhiVDF,
					PiVdf:  auxTag.AuxKey.PiVDF,
				},
			},
			MerklePath: make([]*pb.State, 1),
			Round:      0,
			From:       e.network.GetNodeID(),
		}

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
	results := make(map[common.Key]common.O)

	for r := 1; r <= e.diameter; r++ {
		time.Sleep(time.Until(e.startTime.Add(time.Duration(r) * e.roundTimeout)))
		e.logger.Info("ExAnte verification round", zap.Int("round", r))

		for _, msg := range e.messages[r] {
			if e.isMessageValid(&msg, auxLocal, filterFn, r) {
				g := min(e.gradeFunction(msg.sid, msg.vk, msg.ch, msg.aux.AuxKey, auxLocal),
					e.diameter-int(math.Floor(float64(r)/float64(e.D))))
				key := e.mdag.Oracle(msg.aux.PiRP, msg.aux.AuxKey.PhiVRF, msg.aux.AuxKey.PiVRF, msg.aux.AuxKey.PhiVDF, msg.aux.AuxKey.PiVDF)
				if v, exists := e.verifiedValues[string(key)]; !exists {
					e.verifiedValues[string(key)] = verifiedTuple{
						vk:    msg.vk,
						v:     msg.ch,
						aux:   msg.aux,
						grade: g,
					}
				} else if g > v.grade {
					e.verifiedValues[string(key)] = verifiedTuple{
						vk:    msg.vk,
						v:     msg.ch,
						aux:   msg.aux,
						grade: g,
					}
				} else {
					continue
				}

				if _, exists := results[common.Key{VK: string(msg.vk), Ch: string(msg.ch)}]; !exists ||
					g > results[common.Key{VK: string(msg.vk), Ch: string(msg.ch)}].Grade {
					results[common.Key{VK: string(msg.vk), Ch: string(msg.ch)}] = common.O{
						VK:        msg.vk,
						Challenge: msg.ch,
						Aux:       msg.aux,
						Grade:     g,
					}
				}

				pMsg := &pb.ExAnteMessage{
					SessionId:       session,
					VerificationKey: msg.vk,
					Value:           msg.ch,
					Aux: &pb.Aux{
						PiRP: msg.aux.PiRP,
						AuxKey: &pb.AuxKeyMessage{
							PhiVrf: msg.aux.AuxKey.PhiVRF,
							PiVrf:  msg.aux.AuxKey.PiVRF,
							PhiVdf: msg.aux.AuxKey.PhiVDF,
							PiVdf:  msg.aux.AuxKey.PiVDF,
						},
					},
					MerklePath: make([]*pb.State, r+1),
					Round:      uint32(r),
					From:       e.network.GetNodeID(),
				}

				for i := 0; i < r; i++ {
					pMsg.MerklePath[i] = &pb.State{
						Row: msg.merklePath[i],
					}
				}
				pMsg.MerklePath[r] = &pb.State{
					Row: sigma[r],
				}
				pMsgBytes, err := proto.Marshal(pMsg)
				if err != nil {
					e.logger.Error("Failed to marshal message", zap.Error(err))
					continue
				}
				protocolID := fmt.Sprintf("%s/%s", exanteProtocolID, session)
				e.network.SendProtocolMessage(protocolID, pMsgBytes)
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
		aux: &common.AuxTag{
			PiRP: msg.Aux.PiRP,
			AuxKey: &common.AuxKey{
				PhiVRF: msg.Aux.AuxKey.PhiVrf,
				PiVRF:  msg.Aux.AuxKey.PiVrf,
				PhiVDF: msg.Aux.AuxKey.PhiVdf,
				PiVDF:  msg.Aux.AuxKey.PiVdf,
			},
		},
		merklePath: convertExAnteMessageToBytes(&msg),
	}

	return nil
}

func (e *ExAnte) validateMerklePath(merklePath [][][]byte, round int) bool {

	if len(merklePath) < round {
		e.logger.Error("Invalid Merkle path length",
			zap.Int("expected", round),
			zap.Int("actual", len(merklePath)))
		return false
	}
	if len(e.state) < round+1 {
		e.logger.Error("Invalid state length",
			zap.Int("expected", round),
			zap.Int("actual", len(e.state)))
		return false
	}

	for i := 0; i < round; i++ {
		h := e.mdag.Oracle(merklePath[i]...)
		if i == round-1 {
			return isValueInState(h, e.state[round])
		} else if !isValueInState(h, merklePath[i+1]) {
			return false
		}
	}

	return true
}

func (e *ExAnte) isMessageValid(msg *receivedMessage, auxLocal float64, filterFn common.FilterTagF, r int) bool {
	if msg == nil {
		return false
	}

	if !filterFn(msg.sid, msg.vk, msg.ch, msg.aux) {
		return false
	}

	if !(e.gradeFunction(msg.sid, msg.vk, msg.ch, msg.aux.AuxKey, auxLocal) > 0) {
		return false
	}

	if !isValueInState(e.mdag.Oracle([]byte(msg.sid), msg.vk, msg.ch, msg.aux.PiRP), msg.merklePath[r]) {
		return false
	}

	if !e.validateMerklePath(msg.merklePath, r) {
		return false
	}

	return true
}

func isValueInState(value []byte, state [][]byte) bool {

	for _, state := range state {
		if string(state) == string(value) {
			return true
		}
	}

	return false
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
