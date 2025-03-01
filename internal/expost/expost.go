package expost

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
	expostpb "github.com/mamorski/committee-sampling/pkg/proto"

	"google.golang.org/protobuf/proto"
)

const (
	protocolID = "/expost/1.0.0"
)

type filterTagFunc func(session string, vk, v []byte, aux *common.AuxTag) bool

type gradeFunc func(string, []byte, []byte, *common.AuxKey, float64) int

type Network interface {
	RegisterHandler(protocolID string, handler MessageHandler)
	SendProtocolMessage(protocolID string, data []byte)
	GetNeighbors() []string
	GetNodeID() string
	Close() error
}

type MessageHandler func(from string, payload []byte) error

type MDAG interface {
	Gen(session string, vk []byte, r []byte) ([][][]byte, []byte, error)
}

// ExPost implements the ex‐post timestamp protocol.
type ExPost struct {
	net           Network
	mdag          MDAG
	d             int
	D             int
	roundDuration time.Duration
	gradeFunc     gradeFunc
	hashFunc      func(data []byte) []byte

	sid string
	// lambda is the length (in bytes) for the random string.
	lambda int

	// receivedMsgs stores incoming ExPost messages, indexed by round.
	receivedMsgs map[int][]*expostpb.ExPostMessage
	mu           sync.Mutex
}

// New constructs an ExPost instance.
// Parameters:
// - sid: session identifier.
// - net: the network instance.
// - mdag: an MDAG instance.
// - d: protocol parameter d.
// - D: honest diameter bound.
// - lambda: length (in bytes) of the random string.
// - roundDuration: duration for each round.
// - gradeFunc: a function to compute the grade.
func New(sid string, net Network, mdag MDAG, d, D, lambda int, roundDuration time.Duration, gradeFunc gradeFunc) *ExPost {
	ep := &ExPost{
		sid:           sid,
		net:           net,
		mdag:          mdag,
		d:             d,
		D:             D,
		lambda:        lambda,
		roundDuration: roundDuration,
		gradeFunc:     gradeFunc,
		hashFunc: func(data []byte) []byte {
			sum := sha256.Sum256(data)
			return sum[:]
		},
		receivedMsgs: make(map[int][]*expostpb.ExPostMessage),
	}
	net.RegisterHandler(protocolID, ep.handleMessage)
	return ep
}

// handleMessage decodes an incoming protobuf ExPostMessage,
// checks that the session matches, and saves it to receivedMsgs.
func (ep *ExPost) handleMessage(from string, payload []byte) error {
	var msg expostpb.ExPostMessage
	if err := proto.Unmarshal(payload, &msg); err != nil {
		return err
	}
	// Ignore messages from other sessions or if the sender is self.
	if msg.SessionId != ep.sid || from == msg.From {
		return nil
	}

	ep.mu.Lock()
	defer ep.mu.Unlock()
	round := int(msg.Round)
	ep.receivedMsgs[round] = append(ep.receivedMsgs[round], &msg)
	return nil
}

// broadcastMessage marshals and sends an ExPostMessage via the network.
func (ep *ExPost) broadcastMessage(msg *expostpb.ExPostMessage) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return
	}
	ep.net.SendProtocolMessage(protocolID, data)
}

// encodeSigmaParts converts a slice of [][]byte into []string with hex encoding.
func encodeSigmaParts(parts [][]byte) []string {
	var encoded []string
	for _, part := range parts {
		encoded = append(encoded, hex.EncodeToString(part))
	}
	return encoded
}

// Generate implements the timestamp generation phase.
func (ep *ExPost) Generate(session string, vk []byte) ([][][]byte, []byte, error) {
	// Step 1: Generate a random string of length ep.lambda bytes.
	r := make([]byte, ep.lambda)
	if _, err := rand.Read(r); err != nil {
		return nil, nil, err
	}

	// Step 2: Execute MDAG.Gen.
	sigma, label, err := ep.mdag.Gen(session, vk, r)
	if err != nil {
		return nil, nil, err
	}

	return sigma, label, nil
}

// Verify implements the timestamp proof-verification phase.
func (ep *ExPost) Verify(session string, vk []byte, fSigmaExp *common.FSigmaExp, auxTag *common.AuxTag, auxLocal float64, filter filterTagFunc) (map[common.Key]common.O, error) {
	rounds := ep.d * ep.D
	V := make(map[common.Key]common.O)

	// --- Round 0: Local check and broadcast ---
	if ep.gradeFunc(session, vk, fSigmaExp.Challenge, auxTag.AuxKey, auxLocal) > (ep.d) && filter(session, vk, fSigmaExp.Challenge, auxTag) {
		k := common.Key{VK: hex.EncodeToString(vk), Ch: hex.EncodeToString(fSigmaExp.Challenge)}
		V[k] = common.O{
			VK:        vk,
			Challenge: fSigmaExp.Challenge,
			Aux:       auxTag.AuxKey,
			Grade:     ep.d + 1,
		}

		// Broadcast initial message using the last round's sigma part.
		sigmaRounds := len(fSigmaExp.Sigma)
		if sigmaRounds == 0 {
			return nil, errors.New("empty sigma")
		}

		msg := &expostpb.ExPostMessage{
			SessionId:  ep.sid,
			Round:      0,
			Vk:         hex.EncodeToString(vk),
			Challenge:  hex.EncodeToString(fSigmaExp.Challenge),
			Aux:        auxTag.AuxKey.ToProto(),
			SigmaParts: encodeSigmaParts(fSigmaExp.Sigma[sigmaRounds-1]),
		}
		ep.broadcastMessage(msg)
	}

	// --- Rounds 1 to rounds-1 ---
	for r := 1; r < rounds; r++ {
		// Wait for the duration of the round.
		time.Sleep(ep.roundDuration)
		ep.mu.Lock()
		msgs := ep.receivedMsgs[r-1]
		delete(ep.receivedMsgs, r-1)
		ep.mu.Unlock()

		for _, msg := range msgs {
			// Decode sender VK and challenge from hex.
			senderVK, err := hex.DecodeString(msg.Vk)
			if err != nil {
				continue
			}
			challengeBytes, err := hex.DecodeString(msg.Challenge)
			if err != nil {
				continue
			}
			// Use an empty AuxTag for verification if none is provided.
			if !filter(session, senderVK, challengeBytes, &common.AuxTag{}) {
				continue
			}
			grade := ep.gradeFunc(session, senderVK, challengeBytes, nil, auxLocal)
			if grade <= 0 {
				continue
			}
			g := ep.d - r
			if grade < g {
				g = grade
			}
			k := common.Key{VK: msg.Vk, Ch: msg.Challenge}
			if existing, ok := V[k]; ok && existing.Grade >= g {
				continue
			}
			V[k] = common.O{
				VK:        senderVK,
				Challenge: challengeBytes,
				Aux:       nil,
				Grade:     g,
			}
			// Append local sigma part for the next round if available.
			sigmaIndex := len(fSigmaExp.Sigma) - r - 1
			if sigmaIndex < 0 {
				continue
			}
			newSigmaPart := fSigmaExp.Sigma[sigmaIndex]
			// Build new payload: prepend the hex encoded new sigma parts to the received sigma parts.
			newPayload := append(encodeSigmaParts(newSigmaPart), msg.SigmaParts...)
			newMsg := &expostpb.ExPostMessage{
				SessionId:  ep.sid,
				Round:      uint32(r),
				Vk:         msg.Vk,
				Challenge:  msg.Challenge,
				Aux:        msg.Aux,
				SigmaParts: newPayload,
			}
			ep.broadcastMessage(newMsg)
		}
	}

	// --- Compute Vmax: select tuples with maximal grade ---
	Vmax := make(map[common.Key]common.O)
	for k, o := range V {
		if existing, ok := Vmax[k]; !ok || o.Grade > existing.Grade {
			Vmax[k] = o
		}
	}
	return Vmax, nil
}
