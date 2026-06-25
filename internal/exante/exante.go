package exante

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap/zapcore"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/mamorski/committee-sampling/internal/threadpool"
	pb "github.com/mamorski/committee-sampling/pkg/proto"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const exanteProtocolID = "/exante/1.0.0"

// Object pools for protobuf message reuse
var (
	exanteTimestampMessagePool = sync.Pool{
		New: func() interface{} {
			return &pb.TimestampMessage{}
		},
	}

	exanteAuxPool = sync.Pool{
		New: func() interface{} {
			return &pb.Aux{}
		},
	}

	exanteAuxKeyMessagePool = sync.Pool{
		New: func() interface{} {
			return &pb.AuxKeyMessage{}
		},
	}

	exanteStatePool = sync.Pool{
		New: func() interface{} {
			return &pb.State{}
		},
	}
)

// Helper functions for object pool management in ExAnte
func getExAnteTimestampMessage() *pb.TimestampMessage {
	msg := exanteTimestampMessagePool.Get().(*pb.TimestampMessage)
	msg.Reset()
	return msg
}

func putExAnteTimestampMessage(msg *pb.TimestampMessage) {
	if msg != nil {
		exanteTimestampMessagePool.Put(msg)
	}
}

func getExAnteAux() *pb.Aux {
	aux := exanteAuxPool.Get().(*pb.Aux)
	aux.Reset()
	return aux
}

func putExAnteAux(aux *pb.Aux) {
	if aux != nil {
		exanteAuxPool.Put(aux)
	}
}

func getExAnteAuxKeyMessage() *pb.AuxKeyMessage {
	auxKey := exanteAuxKeyMessagePool.Get().(*pb.AuxKeyMessage)
	auxKey.Reset()
	return auxKey
}

func putExAnteAuxKeyMessage(auxKey *pb.AuxKeyMessage) {
	if auxKey != nil {
		exanteAuxKeyMessagePool.Put(auxKey)
	}
}

func getExAnteState() *pb.State {
	state := exanteStatePool.Get().(*pb.State)
	state.Reset()
	return state
}

func putExAnteState(state *pb.State) {
	if state != nil {
		exanteStatePool.Put(state)
	}
}

// MDAG defines the interface for the MDAG required by ExAnte
type MDAG interface {
	Generate(sid string, vk []byte, vi ...[]byte) ([][][]byte, error)
	Oracle(h ...[]byte) []byte
}

// ExAnte implements the Ex-Ante Timestamp protocol
type ExAnte struct {
	network      network.Network
	logger       *zap.Logger
	mdag         MDAG
	synchronizer common.Synchronizer
	mu           sync.Mutex
	messages     map[int][]*pb.TimestampMessage
	state        [][][]byte
	threadPool   *threadpool.ThreadPool

	gradeFunction common.GradeFunc
	roundAcc      map[int]*common.RoundAcc
	tickTimes     map[int]time.Time
	currentRound  int

	protocolID string
	nodeID     string
	sid        string
	challenge  []byte

	d         int
	D         int
	R         int
	isRunning bool
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
	logger *zap.Logger,
) *ExAnte {

	// Register message handler
	protocolID := fmt.Sprintf("%s/%s", exanteProtocolID, sid)

	e := &ExAnte{
		network:       net,
		logger:        logger.Named("exante"),
		mdag:          mdag,
		synchronizer:  synchronizer,
		d:             d,
		D:             D,
		gradeFunction: gradeFunction,
		messages:      make(map[int][]*pb.TimestampMessage),
		sid:           sid,
		isRunning:     true,
		protocolID:    protocolID,
		nodeID:        net.GetNodeID(),
		R:             d * D,
		roundAcc:      make(map[int]*common.RoundAcc),
		tickTimes:     make(map[int]time.Time),
		currentRound:  -1,
	}

	net.RegisterHandler(protocolID, e.handleMessage)

	e.logger.Info(
		"ExAnte instance created", zap.String("session_id", sid), zap.Int("d", d),
	)

	return e
}

// Generate implements the ExAnte Generate method using MDAG
func (e *ExAnte) Generate(session string, vk []byte, challenge []byte, piRP []byte) ([][][]byte, error) {
	e.logger.Info("Generate started")
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		e.logger.Info(
			"Generate completed", zap.Duration("elapsed", elapsed),
		)
	}()

	if e.sid != "" && e.sid != session {
		return nil, fmt.Errorf("session ID mismatch: expected %s, got %s", e.sid, session)
	}

	e.logger.Info(
		"Starting ExAnte Generation phase", zap.String("session_id", session), zap.String("node_id", e.nodeID),
	)

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
	session string, vk []byte, sigma [][][]byte, auxTag *common.AuxTag, auxLocal float64, filterFn common.FilterTagF,
) (*common.Committee, error) {

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

	if len(sigma) < e.R {
		e.isRunning = false
		e.logger.Error(
			"Sigma length is less than required rounds", zap.Int("expected_rounds", e.R), zap.Int("actual_length", len(sigma)),
		)

		return nil, fmt.Errorf("sigma length is less than required rounds: %d < %d", len(sigma), e.R)
	}

	e.logger.Info(
		"Starting ExAnte Verification phase", zap.String("session_id", session),
	)
	results := &common.Committee{}

	// Check if P_i is also acts like a prover
	auxPb := &pb.Aux{
		PiRP: auxTag.PiRP,
		AuxKey: &pb.AuxKeyMessage{
			PhiVrf: auxTag.AuxKey.PhiVRF,
			PiVrf:  auxTag.AuxKey.PiVRF,
			PhiVdf: auxTag.AuxKey.PhiVDF,
			PiVdf:  auxTag.AuxKey.PiVDF,
		},
	}
	grade := e.gradeFunction(session, vk, auxPb.AuxKey.PhiVrf, auxPb.AuxKey, auxLocal)
	if grade >= e.d+1 && filterFn(session, e.nodeID, vk, e.challenge, auxPb) {

		e.logger.Info(
			"Node is a prover, sending initial message", zap.Int("grade", grade), zap.Int("d", e.d),
		)

		// Use pooled objects
		msg := getExAnteTimestampMessage()
		defer putExAnteTimestampMessage(msg)

		auxKey := getExAnteAuxKeyMessage()
		defer putExAnteAuxKeyMessage(auxKey)

		aux := getExAnteAux()
		defer putExAnteAux(aux)

		state := getExAnteState()
		defer putExAnteState(state)

		// Configure the pooled objects
		msg.SessionId = session
		msg.VerificationKey = vk
		msg.Value = e.challenge
		msg.Round = 0
		msg.Id = e.nodeID

		auxKey.PhiVrf = auxTag.AuxKey.PhiVRF
		auxKey.PiVrf = auxTag.AuxKey.PiVRF
		auxKey.PhiVdf = auxTag.AuxKey.PhiVDF
		auxKey.PiVdf = auxTag.AuxKey.PiVDF

		aux.PiRP = auxTag.PiRP
		aux.AuxKey = auxKey
		msg.Aux = aux

		state.Row = sigma[0][0:]
		msg.MerklePath = []*pb.State{state}

		msgBytes, err := proto.Marshal(msg)
		if err != nil {
			e.isRunning = false
			e.logger.Error("Failed to marshal initial message", zap.Error(err))
			return nil, fmt.Errorf("failed to marshal initial message: %w", err)
		}

		results.Add(vk, e.nodeID, grade)
		e.network.SendProtocolMessage(e.protocolID, msgBytes)
	}

	for r := 1; r < e.R; r++ {
		// Wait for round r synchronization
		waitChan, err := e.synchronizer.WaitForRound(common.ExAnteVerify, r)
		if err != nil {
			e.isRunning = false
			return nil, fmt.Errorf("failed to wait for round %d: %w", r, err)
		}
		<-waitChan
		tickNow := time.Now()
		e.logger.Info("ExAnte verification round", zap.Int("round", r))

		e.mu.Lock()
		e.currentRound = r
		e.tickTimes[r] = tickNow
		msgs := e.messages[r-1]
		prevAcc := e.roundAcc[r-1]
		delete(e.roundAcc, r-1)
		e.mu.Unlock()

		if prevAcc != nil {
			e.flushRoundStats(r-1, prevAcc)
		}

		if len(msgs) == 0 {
			e.logger.Debug("No messages received for round", zap.Int("round", r))
		}

		loopStart := time.Now()
		// Process messages in parallel using a threadpool
		for _, msg := range msgs {
			e.threadPool.Submit(
				func() {
					e.processMessage(msg, session, sigma, auxLocal, filterFn, r, results, e.protocolID)
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
	e.logger.Info("ExAnte verification phase completed")

	// Flush stats for any rounds not yet flushed (including the last round).
	e.mu.Lock()
	remaining := e.roundAcc
	e.roundAcc = make(map[int]*common.RoundAcc)
	e.mu.Unlock()
	for round, a := range remaining {
		e.flushRoundStats(round, a)
	}

	return results, nil
}

// handleMessage processes incoming messages from the network.
func (e *ExAnte) handleMessage(from peer.ID, payload []byte) error {
	e.mu.Lock()
	running := e.isRunning
	e.mu.Unlock()

	if !running {
		return errors.New("protocol not running")
	}
	if e.logger.Core().Enabled(zapcore.DebugLevel) {
		e.logger.Debug(fmt.Sprintf("Received message from %s", from))
	}

	var msg pb.TimestampMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		e.logger.Error("Failed to unmarshal ExAnte message", zap.Error(err))
		return err
	}

	round := int(msg.Round)
	if round < 0 || round >= e.R {
		e.logger.Warn(
			"Received message with invalid round", zap.Int("round", round), zap.Int("expected_max_round", e.R-1),
		)
		return nil
	}

	arrival := time.Now()

	// Phase 1: brief lock to snapshot state and count total.
	e.mu.Lock()
	acc := e.getAcc(round)
	acc.Total++
	curRound := e.currentRound
	tt := e.tickTimes[round]
	e.mu.Unlock()

	if curRound >= 0 && round < curRound {
		late := curRound - round
		e.mu.Lock()
		acc.LateCount++
		if late > acc.MaxLateness {
			acc.MaxLateness = late
		}
		e.mu.Unlock()
	}
	if !tt.IsZero() {
		if lag := arrival.Sub(tt).Nanoseconds(); lag > 0 {
			e.mu.Lock()
			acc.LagSumNs += lag
			acc.LagCount++
			acc.LagLastNs = lag
			if lag > acc.LagMaxNs {
				acc.LagMaxNs = lag
			}
			e.mu.Unlock()
		}
	}

	// Validation outside lock — Warn calls must not hold mu.
	if msg.SessionId != e.sid {
		e.logger.Warn(
			"Received message with mismatched session id", zap.String("expected", e.sid), zap.String("received", msg.SessionId),
		)
		return errors.New("session id mismatch")
	}

	if !e.network.IsNeighbor(from) {
		e.logger.Warn(
			"Received message from non-neighbor sender", zap.String("sender", from.String()),
		)
		return errors.New("sender not in neighbors list")
	}

	if e.logger.Core().Enabled(zapcore.DebugLevel) {
		e.logger.Debug(
			"ExAnte: Received message",
			zap.String("from", from.String()),
			zap.String("sender_id", msg.Id),
			zap.Int("round", round),
			zap.Binary("verification_key", msg.VerificationKey),
			zap.Binary("value", msg.Value),
			zap.Binary("proof", msg.Aux.PiRP),
		)
	}

	// Phase 2: brief lock to record valid message.
	e.mu.Lock()
	acc.Valid++
	e.messages[round] = append(e.messages[round], &msg)
	e.mu.Unlock()
	return nil
}

func (e *ExAnte) getAcc(round int) *common.RoundAcc {
	a, ok := e.roundAcc[round]
	if !ok {
		a = &common.RoundAcc{}
		e.roundAcc[round] = a
	}
	return a
}

func (e *ExAnte) flushRoundStats(round int, a *common.RoundAcc) {
	if a.Total == 0 {
		return
	}
	var meanNs int64
	if a.LagCount > 0 {
		meanNs = a.LagSumNs / int64(a.LagCount)
	}
	e.logger.Warn("round stats",
		zap.String("proto", "exante"), zap.Int("round", round),
		zap.Int("total", a.Total), zap.Int("valid", a.Valid),
		zap.Int("late", a.LateCount), zap.Int("max_lateness", a.MaxLateness),
		zap.Int64("lag_mean_ns", meanNs), zap.Int64("lag_max_ns", a.LagMaxNs),
		zap.Int64("lag_last_ns", a.LagLastNs), zap.Int("lag_count", a.LagCount))
}

func (e *ExAnte) validateMerklePath(merklePath []*pb.State, round int) bool {

	if len(merklePath) < round {
		e.logger.Warn(
			"Invalid Merkle path length", zap.Int("expected", round), zap.Int("actual", len(merklePath)),
		)
		return false
	}
	if len(e.state) < round+1 {
		e.logger.Warn(
			"Invalid state length", zap.Int("expected", round), zap.Int("actual", len(e.state)),
		)
		return false
	}

	for i := 0; i < round; i++ {
		h := e.mdag.Oracle(merklePath[i].Row...)
		if i == round-1 {
			return isValueInState(h, e.state[round])
		} else if !isValueInState(h, merklePath[i+1].Row) {
			return false
		}
	}

	return true
}

func (e *ExAnte) isMessageValid(msg *pb.TimestampMessage, auxLocal float64, filterFn common.FilterTagF, r int) bool {
	if msg == nil {
		return false
	}

	if !filterFn(msg.SessionId, msg.Id, msg.VerificationKey, msg.Value, msg.Aux) {
		e.logger.Warn(
			"Message filtered out by filter function", zap.String("session_id", msg.SessionId),
		)
		return false
	}
	if !(e.gradeFunction(msg.SessionId, msg.VerificationKey, msg.Value, msg.Aux.AuxKey, auxLocal) > 0) {
		e.logger.Warn(
			"Message filtered out by grade function", zap.String("sender_id", msg.Id),
		)
		return false
	}
	if !isValueInState(
		e.mdag.Oracle([]byte(msg.SessionId), msg.VerificationKey, msg.Value, msg.Aux.PiRP), msg.MerklePath[0].Row,
	) {
		e.logger.Warn(
			"Message filtered out by merkle path", zap.String("sender_id", msg.Id),
		)
		return false
	}
	if !e.validateMerklePath(msg.MerklePath, r) {
		e.logger.Warn(
			"Message filtered out by merkle path", zap.String("sender_id", msg.Id),
		)
		return false
	}
	return true
}

// processMessage processes a single message in parallel
func (e *ExAnte) processMessage(
	msg *pb.TimestampMessage,
	session string,
	sigma [][][]byte,
	auxLocal float64,
	filterFn common.FilterTagF,
	r int,
	results *common.Committee,
	protocolID string,
) {

	if e.isMessageValid(msg, auxLocal, filterFn, r) {

		g := min(e.gradeFunction(msg.SessionId, msg.VerificationKey, msg.Value, msg.Aux.AuxKey, auxLocal), e.d-r/e.D)
		if e.logger.Core().Enabled(zapcore.DebugLevel) {
			e.logger.Debug(
				"Processing valid message", zap.String("sender_id", msg.Id), zap.Int("round", r), zap.Int("grade", g),
			)
		}
		if results.Add(msg.VerificationKey, msg.Id, g) {
			e.logger.Info(
				"Added to results",
				zap.String("sender_id", msg.Id),
				zap.Binary("vk", msg.VerificationKey),
				zap.Binary("value", msg.Value),
				zap.Int("grade", g),
			)
		} else {
			if e.logger.Core().Enabled(zapcore.DebugLevel) {
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
		// Use pooled objects for message propagation
		pMsg := getExAnteTimestampMessage()
		defer putExAnteTimestampMessage(pMsg)
		auxKey := getExAnteAuxKeyMessage()
		defer putExAnteAuxKeyMessage(auxKey)
		aux := getExAnteAux()
		defer putExAnteAux(aux)
		pMsg.SessionId = session
		pMsg.VerificationKey = msg.VerificationKey
		pMsg.Value = msg.Value
		pMsg.Round = uint32(r) //nolint:gosec
		pMsg.Id = msg.Id
		auxKey.PhiVrf = msg.Aux.AuxKey.PhiVrf
		auxKey.PiVrf = msg.Aux.AuxKey.PiVrf
		auxKey.PhiVdf = msg.Aux.AuxKey.PhiVdf
		auxKey.PiVdf = msg.Aux.AuxKey.PiVdf
		aux.PiRP = msg.Aux.PiRP
		aux.AuxKey = auxKey
		pMsg.Aux = aux
		pMsg.MerklePath = make([]*pb.State, r+1)
		for i := 0; i < r; i++ {
			state := getExAnteState()
			state.Row = msg.MerklePath[i].Row
			pMsg.MerklePath[i] = state
		}
		lastState := getExAnteState()
		lastState.Row = sigma[r]
		pMsg.MerklePath[r] = lastState
		pMsgBytes, err := proto.Marshal(pMsg)
		if err != nil {
			e.logger.Error("Failed to marshal message", zap.Error(err))
			return
		}
		e.network.SendProtocolMessage(protocolID, pMsgBytes)
		for _, state := range pMsg.MerklePath {
			putExAnteState(state)
		}
	} else {
		e.logger.Info(
			"Ignoring invalid message", zap.String("sender_id", msg.Id), zap.Int("round", r),
		)
	}
}

func isValueInState(value []byte, state [][]byte) bool {
	for _, s := range state {
		if bytes.Equal(s, value) {
			return true
		}
	}
	return false
}
