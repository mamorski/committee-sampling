package expost

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"slices"
	"sync"
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/network"
	pb "github.com/mamorski/committee-sampling/pkg/proto"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	expostProtocolID = "/expost/1.0.0"
	charset          = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

// MDAG defines the interface for the MDAG required by ExPost
type MDAG interface {
	Generate(sid string, vk []byte, vi ...[]byte) ([][][]byte, error)
	GetComputedLabel(roundIndex int) []byte
	Oracle(h ...[]byte) []byte
}

// ExPost implements the Ex-Post Timestamp protocol
type ExPost struct {
	network      network.Network
	logger       *zap.Logger
	mdag         MDAG
	synchronizer common.Synchronizer
	d            int
	D            int
	lambda       int
	gradeFunc    common.GradeFunc
	isRunning    bool
	sid          string
	vk           []byte

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

// New creates a new ExPost instance
func New(
	net network.Network,
	mdag MDAG,
	sid string,
	vk []byte,
	synchronizer common.Synchronizer,
	gradeLevels int,
	diameterBound int,
	lambda int,
	gradeFunc common.GradeFunc,
	logger *zap.Logger) *ExPost {

	e := &ExPost{
		network:      net,
		logger:       logger.Named("expost"),
		mdag:         mdag,
		synchronizer: synchronizer,
		d:            gradeLevels,
		D:            diameterBound,
		lambda:       lambda,
		gradeFunc:    gradeFunc,
		messages:     make(map[int][]receivedMessage),
		sid:          sid,
		vk:           vk,
		isRunning:    true,
	}

	protocolID := fmt.Sprintf("%s/%s", expostProtocolID, sid)
	net.RegisterHandler(protocolID, e.handleMessage)

	logger.Info("ExPost instance created", zap.String("session_id", sid), zap.Int("d", gradeLevels))

	return e
}

// Generate implements the ExPost Generation phase
func (e *ExPost) Generate(session string, vk []byte) ([][][]byte, []byte, error) {
	e.logger.Info("Generate started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		e.logger.Info("Generate completed",
			zap.Duration("elapsed", elapsed),
		)
	}()

	// Step 1: Choose a random string ri
	ri, err := secureRandomBytes(e.lambda, charset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate randomness: %w", err)
	}

	// Step 2: Run MDAG.Generate(sid, vk, ri) for R = d·D rounds
	R := e.d * e.D
	state, err := e.mdag.Generate(session, vk, ri)
	if err != nil {
		return nil, nil, fmt.Errorf("MDAG generation failed: %w", err)
	}
	e.state = state

	// Step 3: Output (σi, ℓi, R)
	labelR := e.mdag.GetComputedLabel(R)
	if labelR == nil {
		return nil, nil, fmt.Errorf("computed label for round R is nil")
	}

	e.logger.Info("ExPost Generation phase completed")
	return state, labelR, nil
}

// Verify implements the ExPost Verification phase
func (e *ExPost) Verify(
	session string,
	vk []byte,
	fSigmaExp *common.FSigmaExp,
	auxTag *common.AuxTag,
	auxLocal float64,
	filterFn common.FilterTagF) (*common.Committee, error) { //nolint:funlen

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
	sigma := fSigmaExp.Sigma
	challenge := fSigmaExp.Challenge
	if len(sigma) < R {
		e.isRunning = false
		e.logger.Warn("Sigma length is less than required rounds",
			zap.Int("expected_rounds", R),
			zap.Int("actual_length", len(sigma)))

		return nil, fmt.Errorf("sigma length is less than required rounds: %d < %d", len(sigma), R)
	}

	e.logger.Info("Starting ExPost Verification phase",
		zap.String("session_id", session),
		zap.String("node_id", e.network.GetNodeID()))

	results := &common.Committee{}
	protocolID := fmt.Sprintf("%s/%s", expostProtocolID, session)

	// Step 2: Prover logic for round 0
	grade := e.gradeFunc(session, vk, challenge, auxTag.AuxKey, auxLocal)
	if grade >= (e.d+1) &&
		filterFn(session, e.network.GetNodeID(), vk, challenge, auxTag) {

		e.logger.Info("Node is a prover, sending initial message",
			zap.Int("grade", grade),
			zap.Int("d", e.d),
			zap.Binary("vk", vk),
			zap.Binary("PiRP", auxTag.PiRP),
			zap.Binary("challenge", challenge),
		)
		msg := &pb.TimestampMessage{
			SessionId:       session,
			VerificationKey: vk,
			Value:           challenge,
			Aux: &pb.Aux{
				PiRP: auxTag.PiRP,
				AuxKey: &pb.AuxKeyMessage{
					PhiVrf: auxTag.AuxKey.PhiVRF,
					PiVrf:  auxTag.AuxKey.PiVRF,
					PhiVdf: auxTag.AuxKey.PhiVDF,
					PiVdf:  auxTag.AuxKey.PiVDF,
				},
			},
			MerklePath: []*pb.State{{Row: sigma[len(sigma)-1][0:]}},
			Round:      0,
			Id:         e.network.GetNodeID(),
		}
		msgBytes, err := proto.Marshal(msg)
		if err != nil {
			e.isRunning = false
			e.logger.Warn("Failed to marshal initial message", zap.Error(err))
			return nil, fmt.Errorf("failed to marshal initial message: %w", err)
		}

		e.network.SendProtocolMessage(protocolID, msgBytes)
	}

	for r := 1; r < R; r++ {
		// Wait for round r synchronization
		waitChan, err := e.synchronizer.WaitForRound(common.ExPostVerify, r)
		if err != nil {
			e.isRunning = false
			return nil, fmt.Errorf("failed to wait for round %d: %w", r, err)
		}
		<-waitChan
		e.logger.Info("ExPost verification round",
			zap.Int("round", r),
			zap.String("node_id", e.network.GetNodeID()),
		)
		e.mu.Lock()
		msgs := e.messages[r-1]
		e.mu.Unlock()

		if len(msgs) == 0 {
			e.logger.Info("No messages received for round", zap.Int("round", r))
		}

		loopStart := time.Now()
		for _, msg := range msgs {
			if e.isMessageValid(&msg, auxLocal, filterFn, r) {

				g := min(e.d-r/e.D, e.gradeFunc(msg.sid, msg.vk, msg.v, msg.aux.AuxKey, auxLocal))
				// Debug logging
				e.logger.Debug("Processing valid message", zap.Int("round", r),
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

				// Propagate message to incoming neighbors
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
					pMsg.MerklePath[i+1] = &pb.State{Row: msg.merklePath[i]}
				}

				pMsg.MerklePath[0] = &pb.State{Row: sigma[len(sigma)-1-r]}
				pMsgBytes, err := proto.Marshal(pMsg)
				if err != nil {
					e.logger.Warn("Failed to marshal message", zap.Error(err))
					continue
				}

				e.network.SendProtocolMessage(protocolID, pMsgBytes)
			} else {
				e.logger.Debug("Ignoring invalid message",
					zap.Int("round", r),
					zap.String("sender_id", msg.id),
				)
			}
		}
		e.logger.Info("Finished processing messages for round",
			zap.Int("round", r),
			zap.Duration("elapsed", time.Since(loopStart)),
			zap.Int("messages_processed", len(msgs)),
		)
	}

	e.isRunning = false
	e.logger.Info("ExPost verification phase completed")
	return results, nil
}

func (e *ExPost) isMessageValid(msg *receivedMessage, auxLocal float64, filterFn common.FilterTagF, r int) bool {
	if msg == nil {
		e.logger.Debug("Received nil message")
		return false
	}

	// Check if we have enough layers in the merkle path
	// We need layers from R-round+1 to R (inclusive)
	if len(msg.merklePath) < r {
		e.logger.Warn("Invalid Merkle path length",
			zap.Int("expected", r),
			zap.Int("actual", len(msg.merklePath)),
			zap.String("sender_id", msg.id),
		)
		return false
	}

	if !filterFn(msg.sid, msg.id, msg.vk, msg.v, msg.aux) {
		e.logger.Warn("Message does not pass filter function",
			zap.String("sender_id", msg.id),
		)
		return false
	}

	if !(e.gradeFunc(msg.sid, msg.vk, msg.v, msg.aux.AuxKey, auxLocal) > 0) {
		e.logger.Warn("Message does not pass grade function",
			zap.String("sender_id", msg.id),
		)
		return false
	}

	// vj = H(sort(LR))
	if msg.v == nil || string(msg.v) != string(e.mdag.Oracle(msg.merklePath[len(msg.merklePath)-1]...)) {
		e.logger.Warn("Message does not pass value check",
			zap.String("sender_id", msg.id),
		)
		return false
	}

	if !e.validateMerklePath(msg.merklePath, r) {

		e.logger.Warn("Merkle path validation failed",
			zap.String("sender_id", msg.id),
		)
		return false
	}

	return true
}

func (e *ExPost) validateMerklePath(merklePath [][][]byte, round int) bool {

	R := e.d * e.D
	// Validate that ℓi,R−r+1 ∈ LR−r+1
	// The local label at round R-round should be in the first layer of merkle path
	localLabel := e.mdag.GetComputedLabel(R - round)
	if !isValueInState(localLabel, merklePath[0]) {
		e.logger.Warn("Local label not found in merkle path", zap.Int("round", round), zap.Int("label_round", R-round))
		return false
	}

	// Validate the Merkle path structure
	// Each layer should contain the hash of the previous layer
	for i := 1; i < round; i++ {
		// Hash the previous layer (i-1) and check if it's in current layer (i)
		prevLayerHash := e.mdag.Oracle(merklePath[i-1]...)
		if !isValueInState(prevLayerHash, merklePath[i]) {
			e.logger.Warn("Invalid Merkle path at layer", zap.Int("layer", i), zap.Int("round", round))
			return false
		}
	}

	return true
}

// handleMessage processes incoming messages from the network.
func (e *ExPost) handleMessage(from string, payload []byte) error {
	if !e.isRunning {
		return fmt.Errorf("protocol not running")
	}

	e.logger.Debug(fmt.Sprintf("Received message from %s", from))

	var msg pb.TimestampMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		e.logger.Warn("Failed to unmarshal ExPost message", zap.Error(err))
		return err
	}

	if msg.SessionId != e.sid {
		err := fmt.Errorf("session id mismatch")
		e.logger.Warn("Received message with mismatched session id",
			zap.String("expected", e.sid),
			zap.String("received", msg.SessionId))

		return err
	}

	if !e.network.IsNeighbor(from) {
		err := fmt.Errorf("sender not in neighbors list")

		e.logger.Warn("Received message from non-neighbor sender", zap.String("sender", from))

		return err
	}

	round := int(msg.Round)
	e.mu.Lock()
	defer e.mu.Unlock()

	e.logger.Debug("ExPost: Received message",
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

func secureRandomBytes(n int, allowedCharset string) ([]byte, error) {
	result := make([]byte, n)
	for i := range result {
		idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(allowedCharset))))
		if err != nil {
			return nil, err
		}
		result[i] = charset[idx.Int64()]
	}

	return result, nil
}

func isValueInState(value []byte, state [][]byte) bool {
	return slices.ContainsFunc(state, func(s []byte) bool {
		return string(s) == string(value)
	})
}
