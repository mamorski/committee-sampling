package exante

import (
	"errors"
	"fmt"
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
	Oracle(h ...[]byte) []byte
}

// ExAnte implements the Ex-Ante Timestamp protocol
type ExAnte struct {
	network       network.Network
	logger        *zap.Logger
	mdag          MDAG
	roundTimeout  time.Duration
	startTime     time.Time
	d             int
	D             int
	gradeFunction common.GradeFunc
	isRunning     bool
	sid           string
	challenge     []byte

	mu        sync.Mutex
	messages  map[int]map[string]receivedMessage
	neighbors map[string]bool // set of allowed neighbor node IDs
	state     [][][]byte
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
	d int,
	D int,
	gradeFunction common.GradeFunc,
	logger *zap.Logger) *ExAnte {

	e := &ExAnte{
		network:       net,
		logger:        logger.Named("exante"),
		mdag:          mdag,
		roundTimeout:  roundTimeout,
		startTime:     startTime,
		d:             d,
		D:             D,
		gradeFunction: gradeFunction,
		messages:      make(map[int]map[string]receivedMessage),
		neighbors:     make(map[string]bool),
		sid:           sid,
		isRunning:     true,
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
		zap.Int("d", d))

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

	R := e.d * e.D
	if len(sigma) <= R {
		e.isRunning = false
		e.logger.Error("Sigma length is less than required rounds",
			zap.Int("expected_rounds", R),
			zap.Int("actual_length", len(sigma)))

		return nil, fmt.Errorf("sigma length is less than required rounds: %d < %d", len(sigma), R)
	}

	e.logger.Info("Starting ExAnte Verification phase",
		zap.String("session_id", session),
		zap.String("node_id", e.network.GetNodeID()))
	protocolID := fmt.Sprintf("%s/%s", exanteProtocolID, session)

	// Check if P_i is also acts like a prover
	if e.gradeFunction(session, vk, auxTag.AuxKey.PhiVRF, auxTag.AuxKey, auxLocal) >= e.d+1 &&
		filterFn(session, vk, e.challenge, auxTag) {

		e.logger.Info("Node is a prover, sending initial message")

		msg := &pb.TimestampMessage{
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
			MerklePath: []*pb.State{{Row: sigma[0][0:]}},
			Round:      0,
			From:       e.network.GetNodeID(),
		}

		msgBytes, err := proto.Marshal(msg)
		if err != nil {
			e.isRunning = false
			e.logger.Error("Failed to marshal initial message", zap.Error(err))
			return nil, fmt.Errorf("failed to marshal initial message: %w", err)
		}

		e.network.SendProtocolMessage(protocolID, msgBytes)
	}
	results := make(map[common.Key]common.O)

	for r := 1; r < R; r++ {
		time.Sleep(time.Until(e.startTime.Add(time.Duration(r) * e.roundTimeout)))
		e.logger.Info("ExAnte verification round", zap.Int("round", r))

		e.mu.Lock()
		msgs := e.messages[r-1]
		e.mu.Unlock()

		for _, msg := range msgs {
			if e.isMessageValid(&msg, auxLocal, filterFn, r) {

				g := min(e.gradeFunction(msg.sid, msg.vk, msg.ch, msg.aux.AuxKey, auxLocal), e.d-r/e.D)

				key := common.Key{VK: string(msg.vk), Ch: string(msg.ch)}
				if v, exists := results[key]; !exists || g > v.Grade {
					results[key] = common.O{
						VK:        msg.vk,
						Challenge: msg.ch,
						Aux:       msg.aux,
						Grade:     g,
					}
				} else {
					e.logger.Debug("Ignoring message with lower grade",
						zap.String("sid", msg.sid),
						zap.String("vk", string(msg.vk)),
						zap.String("challenge", string(msg.ch)),
						zap.Int("grade", g),
						zap.Int("existing_grade", v.Grade))
					// Ignore this message as it has a lower grade than the existing one
					continue
				}

				pMsg := &pb.TimestampMessage{
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
	e.mu.Lock()
	running := e.isRunning
	e.mu.Unlock()

	if !running {
		return errors.New("protocol not running")
	}
	e.logger.Debug(fmt.Sprintf("Received message from %s", from))

	var msg pb.TimestampMessage
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
		merklePath: convertTimestampToBytes(&msg),
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

	if !isValueInState(e.mdag.Oracle([]byte(msg.sid), msg.vk, msg.ch, msg.aux.PiRP), msg.merklePath[0]) {
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

func convertTimestampToBytes(msg *pb.TimestampMessage) [][][]byte {

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
