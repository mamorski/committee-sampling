package expost

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/metrics"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/mamorski/committee-sampling/internal/threadpool"
	pb "github.com/mamorski/committee-sampling/pkg/proto"

	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	expostProtocolID = "/expost/1.0.0"
	charset          = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

// Object pools for protobuf message reuse
var (
	timestampMessagePool = sync.Pool{
		New: func() interface{} {
			return &pb.TimestampMessage{}
		},
	}

	auxPool = sync.Pool{
		New: func() interface{} {
			return &pb.Aux{}
		},
	}

	auxKeyMessagePool = sync.Pool{
		New: func() interface{} {
			return &pb.AuxKeyMessage{}
		},
	}

	statePool = sync.Pool{
		New: func() interface{} {
			return &pb.State{}
		},
	}
)

// Helper functions for object pool management
func getTimestampMessage() *pb.TimestampMessage {
	msg := timestampMessagePool.Get().(*pb.TimestampMessage)
	// Reset the message to clean state
	msg.Reset()
	return msg
}

func putTimestampMessage(msg *pb.TimestampMessage) {
	if msg != nil {
		timestampMessagePool.Put(msg)
	}
}

func getAux() *pb.Aux {
	aux := auxPool.Get().(*pb.Aux)
	aux.Reset()
	return aux
}

func putAux(aux *pb.Aux) {
	if aux != nil {
		auxPool.Put(aux)
	}
}

func getAuxKeyMessage() *pb.AuxKeyMessage {
	auxKey := auxKeyMessagePool.Get().(*pb.AuxKeyMessage)
	auxKey.Reset()
	return auxKey
}

func putAuxKeyMessage(auxKey *pb.AuxKeyMessage) {
	if auxKey != nil {
		auxKeyMessagePool.Put(auxKey)
	}
}

func getState() *pb.State {
	state := statePool.Get().(*pb.State)
	state.Reset()
	return state
}

func putState(state *pb.State) {
	if state != nil {
		statePool.Put(state)
	}
}

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
	messages map[int][]*pb.TimestampMessage
	state    [][][]byte

	protocolID string
	nodeID     string
	R          int // d * D
	threadPool *threadpool.ThreadPool
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
	logger *zap.Logger,
) *ExPost {

	protocolID := fmt.Sprintf("%s/%s", expostProtocolID, sid)

	e := &ExPost{
		network:      net,
		logger:       logger.Named("expost"),
		mdag:         mdag,
		synchronizer: synchronizer,
		d:            gradeLevels,
		D:            diameterBound,
		lambda:       lambda,
		gradeFunc:    gradeFunc,
		messages:     make(map[int][]*pb.TimestampMessage, gradeLevels*diameterBound),
		sid:          sid,
		vk:           vk,
		isRunning:    true,

		protocolID: protocolID,
		nodeID:     net.GetNodeID(),
		R:          gradeLevels * diameterBound,
	}

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
		e.logger.Info(
			"Generate completed", zap.Duration("elapsed", elapsed),
		)
	}()

	// Step 1: Choose a random string ri
	ri, err := secureRandomBytes(e.lambda, charset)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate randomness: %w", err)
	}

	// Step 2: Run MDAG.Generate(sid, vk, ri) for R = d·D rounds
	state, err := e.mdag.Generate(session, vk, ri)
	if err != nil {
		return nil, nil, fmt.Errorf("MDAG generation failed: %w", err)
	}
	e.state = state

	// Step 3: Output (σi, ℓi, R)
	labelR := e.mdag.GetComputedLabel(e.R)
	if labelR == nil {
		return nil, nil, fmt.Errorf("computed label for round R is nil")
	}

	e.logger.Info("ExPost Generation phase completed")
	return state, labelR, nil
}

// Verify implements the ExPost Verification phase
func (e *ExPost) Verify(
	session string, vk []byte, fSigmaExp *common.FSigmaExp, auxTag *common.AuxTag, auxLocal float64, filterFn common.FilterTagF,
) (*common.Committee, error) { //nolint:funlen

	e.logger.Info("Verify started")
	start := time.Now()

	// Initialize threadpool for parallel message processing
	e.threadPool = threadpool.New(min(runtime.NumCPU()*2, 4))

	defer func() {
		elapsed := time.Since(start)
		e.logger.Info(
			"Verify completed", zap.Duration("elapsed", elapsed),
		)
		e.threadPool.Close()
	}()

	if e.sid != session {
		return nil, fmt.Errorf("session ID mismatch: expected %s, got %s", e.sid, session)
	}

	sigma := fSigmaExp.Sigma
	challenge := fSigmaExp.Challenge
	if len(sigma) < e.R {
		e.isRunning = false
		e.logger.Warn(
			"Sigma length is less than required rounds", zap.Int("expected_rounds", e.R), zap.Int("actual_length", len(sigma)),
		)

		return nil, fmt.Errorf("sigma length is less than required rounds: %d < %d", len(sigma), e.R)
	}

	e.logger.Info(
		"Starting ExPost Verification phase", zap.String("session_id", session), zap.String("node_id", e.nodeID),
	)

	results := &common.Committee{}

	// Step 2: Prover logic for round 0
	auxPb := &pb.Aux{
		PiRP: auxTag.PiRP,
		AuxKey: &pb.AuxKeyMessage{
			PhiVrf: auxTag.AuxKey.PhiVRF,
			PiVrf:  auxTag.AuxKey.PiVRF,
			PhiVdf: auxTag.AuxKey.PhiVDF,
			PiVdf:  auxTag.AuxKey.PiVDF,
		},
	}
	grade := e.gradeFunc(session, vk, challenge, auxPb.AuxKey, auxLocal)
	if grade >= (e.d+1) &&
		filterFn(session, e.nodeID, vk, challenge, auxPb) {

		e.logger.Info(
			"Node is a prover, sending initial message",
			zap.Int("grade", grade),
			zap.Int("d", e.d),
			zap.Binary("vk", vk),
			zap.Binary("PiRP", auxTag.PiRP),
			zap.Binary("challenge", challenge),
		)
		// Use pooled objects
		msg := getTimestampMessage()
		defer putTimestampMessage(msg)

		auxKey := getAuxKeyMessage()
		defer putAuxKeyMessage(auxKey)

		aux := getAux()
		defer putAux(aux)

		state := getState()
		defer putState(state)

		// Configure the pooled objects
		msg.SessionId = session
		msg.VerificationKey = vk
		msg.Value = challenge
		msg.Round = 0
		msg.Id = e.nodeID

		auxKey.PhiVrf = auxTag.AuxKey.PhiVRF
		auxKey.PiVrf = auxTag.AuxKey.PiVRF
		auxKey.PhiVdf = auxTag.AuxKey.PhiVDF
		auxKey.PiVdf = auxTag.AuxKey.PiVDF

		aux.PiRP = auxTag.PiRP
		aux.AuxKey = auxKey
		msg.Aux = aux

		state.Row = sigma[len(sigma)-1][0:]
		msg.MerklePath = []*pb.State{state}
		msgBytes, err := proto.Marshal(msg)
		if err != nil {
			e.isRunning = false
			e.logger.Warn("Failed to marshal initial message", zap.Error(err))
			return nil, fmt.Errorf("failed to marshal initial message: %w", err)
		}

		results.Add(vk, challenge, e.nodeID, grade)
		e.network.SendProtocolMessage(e.protocolID, msgBytes)
	}

	for r := 1; r < e.R; r++ {
		// Wait for round r synchronization
		waitChan, err := e.synchronizer.WaitForRound(common.ExPostVerify, r)
		if err != nil {
			e.isRunning = false
			return nil, fmt.Errorf("failed to wait for round %d: %w", r, err)
		}
		<-waitChan
		e.logger.Info(
			"ExPost verification round", zap.Int("round", r), zap.String("node_id", e.nodeID),
		)
		e.mu.Lock()
		msgs := e.messages[r-1]
		e.mu.Unlock()

		if len(msgs) == 0 {
			e.logger.Debug("No messages received for round", zap.Int("round", r))
		}

		loopStart := time.Now()
		// Process messages in parallel using a threadpool
		for _, msg := range msgs {
			e.threadPool.Submit(
				func() {
					e.processMessage(msg, session, sigma, auxLocal, filterFn, r, results)
				},
			)
		}

		// Wait for all tasks in this round to complete
		e.threadPool.Wait()
		e.logger.Info(
			"Finished processing messages for round",
			zap.Int("round", r),
			zap.Duration("elapsed", time.Since(loopStart)),
			zap.Int("messages_processed", len(msgs)),
		)
	}

	e.isRunning = false
	e.logger.Info("ExPost verification phase completed")
	return results, nil
}

func (e *ExPost) isMessageValid(msg *pb.TimestampMessage, auxLocal float64, filterFn common.FilterTagF, r int) bool {
	if msg == nil {
		e.logger.Debug("Received nil message")
		return false
	}

	// Check if we have enough layers in the merkle path
	// We need layers from R-round+1 to R (inclusive)
	if len(msg.MerklePath) < r {
		e.logger.Warn(
			"Invalid Merkle path length", zap.Int("expected", r), zap.Int("actual", len(msg.MerklePath)), zap.String("sender_id", msg.Id),
		)
		return false
	}

	if !filterFn(msg.SessionId, msg.Id, msg.VerificationKey, msg.Value, msg.Aux) {
		e.logger.Warn(
			"Message does not pass filter function", zap.String("sender_id", msg.Id),
		)
		return false
	}

	if !(e.gradeFunc(msg.SessionId, msg.VerificationKey, msg.Value, msg.Aux.AuxKey, auxLocal) > 0) {
		e.logger.Warn(
			"Message does not pass grade function", zap.String("sender_id", msg.Id),
		)
		return false
	}

	if msg.Value == nil || string(msg.Value) != string(e.mdag.Oracle(msg.MerklePath[len(msg.MerklePath)-1].Row...)) {
		e.logger.Warn(
			"Message does not pass value check", zap.String("sender_id", msg.Id),
		)
		return false
	}

	if !e.validateMerklePath(msg.MerklePath, r) {
		e.logger.Warn(
			"Merkle path validation failed", zap.String("sender_id", msg.Id),
		)
		return false
	}

	return true
}

func (e *ExPost) validateMerklePath(merklePath []*pb.State, round int) bool {
	// Validate that ℓi, R−r+1 ∈ LR−r+1
	// The local label at round R-round should be in the first layer of a merkle path
	labelRound := e.R - round
	localLabel := e.mdag.GetComputedLabel(labelRound)
	if !isValueInState(localLabel, merklePath[0].Row) {
		e.logger.Warn("Local label not found in merkle path", zap.Int("round", round), zap.Int("label_round", labelRound))
		return false
	}

	// Validate the Merkle path structure
	// Each layer should contain the hash of the previous layer
	for i := 1; i < round; i++ {
		// Hash the previous layer (i-1) and check if it's in current layer (i)
		prevLayerHash := e.mdag.Oracle(merklePath[i-1].Row...)
		if !isValueInState(prevLayerHash, merklePath[i].Row) {
			e.logger.Warn("Invalid Merkle path at layer", zap.Int("layer", i), zap.Int("round", round))
			return false
		}
	}

	return true
}

// processMessage processes a single message in parallel
func (e *ExPost) processMessage(
	msg *pb.TimestampMessage,
	session string,
	sigma [][][]byte,
	auxLocal float64,
	filterFn common.FilterTagF,
	r int,
	results *common.Committee,
) {

	if e.isMessageValid(msg, auxLocal, filterFn, r) {

		g := min(e.d-r/e.D, e.gradeFunc(msg.SessionId, msg.VerificationKey, msg.Value, msg.Aux.AuxKey, auxLocal))
		if e.logger.Core().Enabled(zap.DebugLevel) {
			e.logger.Debug(
				"Processing valid message", zap.Int("round", r), zap.String("sender_id", msg.Id), zap.Int("round", r), zap.Int("grade", g),
			)
		}
		if results.Add(msg.VerificationKey, msg.Value, msg.Id, g) {
			e.logger.Info(
				"Added to results",
				zap.String("sender_id", msg.Id),
				zap.Binary("vk", msg.VerificationKey),
				zap.Binary("value", msg.Value),
				zap.Int("grade", g),
			)
		} else {
			if e.logger.Core().Enabled(zap.DebugLevel) {
				e.logger.Debug(
					"Skipping message with lower grade",
					zap.String("sender_id", msg.Id),
					zap.Binary("vk", msg.VerificationKey),
					zap.Binary("value", msg.Value),
					zap.Int("grade", g),
				)
			}
			return
		}

		// Propagate message to incoming neighbors using pooled objects
		pMsg := getTimestampMessage()
		defer putTimestampMessage(pMsg)

		// Copy the received message fields
		pMsg.SessionId = session
		pMsg.VerificationKey = msg.VerificationKey
		pMsg.Value = msg.Value
		pMsg.Round = uint32(r) //nolint:gosec
		pMsg.Id = msg.Id
		pMsg.Aux = msg.Aux // Reuse the existing Aux

		// Create MerklePath using pooled State objects
		pMsg.MerklePath = make([]*pb.State, r+1)
		for i := 0; i < r; i++ {
			state := getState()
			state.Row = msg.MerklePath[i].Row
			pMsg.MerklePath[i+1] = state
		}

		firstState := getState()
		firstState.Row = sigma[len(sigma)-1-r]
		pMsg.MerklePath[0] = firstState
		pMsgBytes, err := proto.Marshal(pMsg)
		if err != nil {
			e.logger.Warn("Failed to marshal message", zap.Error(err))
			return
		}

		e.network.SendProtocolMessage(e.protocolID, pMsgBytes)

		// Return MerklePath State objects to pools
		for _, state := range pMsg.MerklePath {
			putState(state)
		}
	} else {
		e.logger.Info(
			"Ignoring invalid message", zap.Int("round", r), zap.String("sender_id", msg.Id),
		)
	}
}

// handleMessage processes incoming messages from the network.
func (e *ExPost) handleMessage(from peer.ID, payload []byte) error {
	if !e.isRunning {
		return fmt.Errorf("protocol not running")
	}

	if e.logger.Core().Enabled(zap.DebugLevel) {
		e.logger.Debug(fmt.Sprintf("Received message from %s", from.String()))
	}

	var msg pb.TimestampMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		e.logger.Warn("Failed to unmarshal ExPost message", zap.Error(err))
		return err
	}

	round := int(msg.Round)

	// Increment total messages received metric
	metrics.TotalMessages.WithLabelValues(strconv.Itoa(round), "expost", e.nodeID, e.sid).Inc()

	if msg.SessionId != e.sid {
		err := fmt.Errorf("session id mismatch")
		e.logger.Warn(
			"Received message with mismatched session id", zap.String("expected", e.sid), zap.String("received", msg.SessionId),
		)

		return err
	}

	if msg.Aux == nil {
		err := fmt.Errorf("message missing required Aux field")
		e.logger.Warn("Received message with nil Aux", zap.String("sender", from.String()))
		return err
	}

	if msg.Aux.AuxKey == nil {
		err := fmt.Errorf("message missing required AuxKey field")
		e.logger.Warn("Received message with nil AuxKey", zap.String("sender", from.String()))
		return err
	}

	if !e.network.IsNeighbor(from) {
		err := fmt.Errorf("sender not in neighbors list")

		e.logger.Warn("Received message from non-neighbor sender", zap.String("sender", from.String()))

		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.logger.Core().Enabled(zap.DebugLevel) {
		e.logger.Debug(
			"ExPost: Received message",
			zap.String("from", from.String()),
			zap.String("sender_id", msg.Id),
			zap.Int("round", round),
			zap.Binary("verification_key", msg.VerificationKey),
			zap.Binary("value", msg.Value),
			zap.Binary("proof", msg.Aux.PiRP),
		)
	}

	// Increment valid messages metric - message passed all validation checks
	metrics.ValidMessages.WithLabelValues(strconv.Itoa(round), "expost", e.nodeID, e.sid).Inc()

	e.messages[round] = append(e.messages[round], &msg)
	return nil
}

func secureRandomBytes(n int, allowedCharset string) ([]byte, error) {
	result := make([]byte, n)
	for i := range result {
		idx, err := rand.Int(rand.Reader, big.NewInt(int64(len(allowedCharset))))
		if err != nil {
			return nil, err
		}
		result[i] = allowedCharset[idx.Int64()]
	}

	return result, nil
}

func isValueInState(value []byte, state [][]byte) bool {
	return slices.ContainsFunc(
		state, func(s []byte) bool {
			return slices.Equal(s, value)
		},
	)
}
