package exante

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/mamorski/committee-sampling/internal/threadpool"
	pb "github.com/mamorski/committee-sampling/pkg/proto"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
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

var (
	exanteMessagesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "exante_messages_received_total",
			Help: "Total number of messages received by ExAnte handleMessage",
		},
		[]string{"node_id", "round", "protocol"},
	)

	exanteMessagesValid = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "exante_messages_valid_total",
			Help: "Total number of valid messages processed by ExAnte handleMessage",
		},
		[]string{"node_id", "round", "protocol"},
	)
)

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

	mu         sync.Mutex
	messages   map[int][]*pb.TimestampMessage
	state      [][][]byte
	threadPool *threadpool.ThreadPool
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
		messages:      make(map[int][]*pb.TimestampMessage),
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

	// Initialize threadpool for parallel message processing
	e.threadPool = threadpool.New(min(runtime.NumCPU()*2, 4))

	defer func() {
		elapsed := time.Since(start)
		e.logger.Info("Verify completed",
			zap.Duration("elapsed", elapsed),
		)
		e.threadPool.Close()
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
	if grade >= e.d+1 && filterFn(session, e.network.GetNodeID(), vk, e.challenge, auxPb) {

		e.logger.Info("Node is a prover, sending initial message",
			zap.Int("grade", grade),
			zap.Int("d", e.d),
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
		msg.Id = e.network.GetNodeID()

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
			e.logger.Debug("No messages received for round", zap.Int("round", r))
		}

		loopStart := time.Now()
		// Process messages in parallel using a threadpool
		for _, msg := range msgs {
			e.threadPool.Submit(func() {
				e.processMessage(msg, session, sigma, auxLocal, filterFn, r, results, protocolID)
			})
		}

		// Wait for all tasks in this round to complete
		e.threadPool.Wait()
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
func (e *ExAnte) handleMessage(from peer.ID, payload []byte) error {
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

	round := int(msg.Round)
	nodeID := e.network.GetNodeID()

	// Increment total messages received metric
	exanteMessagesTotal.WithLabelValues(nodeID, fmt.Sprintf("%d", round), "exante").Inc()

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
			zap.String("sender", from.String()))
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	e.logger.Debug("ExAnte: Received message",
		zap.String("from", from.String()),
		zap.String("sender_id", msg.Id),
		zap.Int("round", round),
		zap.Binary("verification_key", msg.VerificationKey),
		zap.Binary("value", msg.Value),
		zap.Binary("proof", msg.Aux.PiRP),
	)

	// Increment valid messages metric - message passed all validation checks
	exanteMessagesValid.WithLabelValues(nodeID, fmt.Sprintf("%d", round), "exante").Inc()

	e.messages[round] = append(e.messages[round], &msg)

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

func (e *ExAnte) isMessageValid(msg *pb.TimestampMessage, auxLocal float64, filterFn common.FilterTagF, r int) bool {
	if msg == nil {
		return false
	}

	if !filterFn(msg.SessionId, msg.Id, msg.VerificationKey, msg.Value, msg.Aux) {
		e.logger.Warn("Message filtered out by filter function",
			zap.String("session_id", msg.SessionId),
		)
		return false
	}
	if !(e.gradeFunction(msg.SessionId, msg.VerificationKey, msg.Value, msg.Aux.AuxKey, auxLocal) > 0) {
		e.logger.Warn("Message filtered out by grade function",
			zap.String("sender_id", msg.Id),
		)
		return false
	}
	if !isValueInState(e.mdag.Oracle([]byte(msg.SessionId), msg.VerificationKey, msg.Value, msg.Aux.PiRP), convertTimestampToBytes(msg)[0]) {
		e.logger.Warn("Message filtered out by merkle path",
			zap.String("sender_id", msg.Id),
		)
		return false
	}
	if !e.validateMerklePath(convertTimestampToBytes(msg), r) {
		e.logger.Warn("Message filtered out by merkle path",
			zap.String("sender_id", msg.Id),
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
	protocolID string) {

	if e.isMessageValid(msg, auxLocal, filterFn, r) {

		g := min(e.gradeFunction(msg.SessionId, msg.VerificationKey, msg.Value, msg.Aux.AuxKey, auxLocal), e.d-r/e.D)
		e.logger.Debug("Processing valid message",
			zap.String("sender_id", msg.Id),
			zap.Int("round", r),
			zap.Int("grade", g),
		)
		if results.Add(msg.VerificationKey, msg.Value, msg.Id, g) {
			e.logger.Info("Added to results",
				zap.String("sender_id", msg.Id),
				zap.Binary("vk", msg.VerificationKey),
				zap.Binary("value", msg.Value),
				zap.Int("grade", g))
		} else {
			e.logger.Debug("Skipping message with lower grade",
				zap.String("sender_id", msg.Id),
				zap.Binary("vk", msg.VerificationKey),
				zap.Binary("value", msg.Value),
				zap.Int("grade", g))
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
			state.Row = convertTimestampToBytes(msg)[i]
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
		e.logger.Info("Ignoring invalid message",
			zap.String("sender_id", msg.Id),
			zap.Int("round", r))
	}
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
