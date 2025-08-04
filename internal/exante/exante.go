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
	synchronizer  common.Synchronizer
	d             int
	D             int
	gradeFunction common.GradeFunc
	isRunning     bool
	sid           string
	challenge     []byte

	mu       sync.Mutex
	messages map[int][]receivedMessage
	state    [][][]byte
}

type receivedMessage struct {
	id         string
	sid        string
	vk         []byte
	v          []byte
	aux        *common.AuxTag
	merklePath [][][]byte
}

// New creates a new ExAnte instance with the specified parameters
func New(
	net network.Network,
	mdag MDAG,
	sid string,
	synchronizer common.Synchronizer,
	d int,
	D int,
	gradeFunction common.GradeFunc,
	logger *zap.Logger) *ExAnte {

	e := &ExAnte{
		network:       net,
		logger:        logger.Named("exante"),
		mdag:          mdag,
		synchronizer:  synchronizer,
		d:             d,
		D:             D,
		gradeFunction: gradeFunction,
		messages:      make(map[int][]receivedMessage),
		sid:           sid,
		isRunning:     true,
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
	e.logger.Info("Generate started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		e.logger.Info("Generate completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

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
	filterFn common.FilterTagF) (*common.Committee, error) {

	e.logger.Info("Verify started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		e.logger.Info("Verify completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	if e.sid != session {
		return nil, fmt.Errorf("session ID mismatch: expected %s, got %s", e.sid, session)
	}

	R := e.d * e.D
	if len(sigma) < R {
		e.isRunning = false
		e.logger.Error("Sigma length is less than required rounds",
			zap.Int("expected_rounds", R),
			zap.Int("actual_length", len(sigma)))

		return nil, fmt.Errorf("sigma length is less than required rounds: %d < %d", len(sigma), R)
	}

	e.logger.Info("Starting ExAnte Verification phase",
		zap.String("session_id", session),
	)
	protocolID := fmt.Sprintf("%s/%s", exanteProtocolID, session)

	// Check if P_i is also acts like a prover
	grade := e.gradeFunction(session, vk, auxTag.AuxKey.PhiVRF, auxTag.AuxKey, auxLocal)
	if grade >= e.d+1 && filterFn(session, e.network.GetNodeID(), vk, e.challenge, auxTag) {

		e.logger.Info("Node is a prover, sending initial message",
			zap.Int("grade", grade),
			zap.Int("d", e.d),
		)

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
			Id:         e.network.GetNodeID(),
		}

		msgBytes, err := proto.Marshal(msg)
		if err != nil {
			e.isRunning = false
			e.logger.Error("Failed to marshal initial message", zap.Error(err))
			return nil, fmt.Errorf("failed to marshal initial message: %w", err)
		}

		e.network.SendProtocolMessage(protocolID, msgBytes)
	}
	results := &common.Committee{}

	for r := 1; r < R; r++ {
		// Wait for round r synchronization
		waitChan, err := e.synchronizer.WaitForRound(common.ExAnteVerify, r)
		if err != nil {
			e.isRunning = false
			return nil, fmt.Errorf("failed to wait for round %d: %w", r, err)
		}
		<-waitChan
		e.logger.Info("ExAnte verification round", zap.Int("round", r))

		e.mu.Lock()
		msgs := e.messages[r-1]
		e.mu.Unlock()

		if len(msgs) == 0 {
			e.logger.Warn("No messages received for round", zap.Int("round", r))
		}

		loopStart := time.Now()
		for _, msg := range msgs {
			if e.isMessageValid(&msg, auxLocal, filterFn, r) {

				g := min(e.gradeFunction(msg.sid, msg.vk, msg.v, msg.aux.AuxKey, auxLocal), e.d-r/e.D)
				e.logger.Debug("Processing valid message",
					zap.String("sender_id", msg.id),
					zap.Int("round", r),
					zap.Int("grade", g),
				)
				if results.Add(msg.vk, msg.v, msg.id, g) {
					e.logger.Info("Added to results",
						zap.String("sender_id", msg.id),
						zap.Binary("vk", msg.vk),
						zap.Binary("value", msg.v),
						zap.Int("grade", g))
				} else {
					e.logger.Info("Skipping message with lower grade",
						zap.String("sender_id", msg.id),
						zap.Binary("vk", msg.vk),
						zap.Binary("value", msg.v),
						zap.Int("grade", g))
					continue
				}

				pMsg := &pb.TimestampMessage{
					SessionId:       session,
					VerificationKey: msg.vk,
					Value:           msg.v,
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
					Round:      uint32(r), //nolint:gosec
					Id:         msg.id,
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
			} else {
				e.logger.Debug("Ignoring invalid message",
					zap.String("sender_id", msg.id),
					zap.Int("round", r))
			}
		}
		e.logger.Info("Finished processing messages for round",
			zap.Int("round", r),
			zap.Duration("elapsed", time.Since(loopStart)),
			zap.Int("messages_processed", len(msgs)),
		)
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

	if !e.network.IsNeighbor(from) {
		err := errors.New("sender not in neighbors list")
		e.logger.Warn("Received message from non-neighbor sender",
			zap.String("sender", from))

		return err
	}

	round := int(msg.Round)

	e.mu.Lock()
	defer e.mu.Unlock()

	e.logger.Debug("ExAnte: Received message",
		zap.String("from", from),
		zap.String("sender_id", msg.Id),
		zap.Int("round", round),
		zap.Binary("verification_key", msg.VerificationKey),
		zap.Binary("value", msg.Value),
		zap.Binary("proof", msg.Aux.PiRP),
	)

	e.messages[round] = append(e.messages[round], receivedMessage{
		id:  msg.Id,
		sid: msg.SessionId,
		vk:  msg.VerificationKey,
		v:   msg.Value,
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
	})

	return nil
}

func (e *ExAnte) validateMerklePath(merklePath [][][]byte, round int) bool {

	if len(merklePath) < round {
		e.logger.Warn("Invalid Merkle path length",
			zap.Int("expected", round),
			zap.Int("actual", len(merklePath)))
		return false
	}
	if len(e.state) < round+1 {
		e.logger.Warn("Invalid state length",
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

	if !filterFn(msg.sid, msg.id, msg.vk, msg.v, msg.aux) {
		e.logger.Warn("Message filtered out by filter function",
			zap.String("session_id", msg.sid),
		)
		return false
	}

	if !(e.gradeFunction(msg.sid, msg.vk, msg.v, msg.aux.AuxKey, auxLocal) > 0) {
		e.logger.Warn("Message filtered out by grade function",
			zap.String("sender_id", msg.id),
		)
		return false
	}

	if !isValueInState(e.mdag.Oracle([]byte(msg.sid), msg.vk, msg.v, msg.aux.PiRP), msg.merklePath[0]) {
		e.logger.Warn("Message filtered out by merkle path",
			zap.String("sender_id", msg.id),
		)
		return false
	}

	if !e.validateMerklePath(msg.merklePath, r) {
		e.logger.Warn("Message filtered out by merkle path",
			zap.String("sender_id", msg.id),
		)
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
		copy(result[i], state.Row)
	}

	return result
}
