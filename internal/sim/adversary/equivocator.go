package adversary

import (
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/mamorski/committee-sampling/internal/hash"
	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

const exAnteProtocolPrefix = "/exante/"

// ExAnteEquivocator mutates outgoing Ex-Ante Timestamp messages to send
// inconsistent values to different neighbors.
type ExAnteEquivocator struct {
	logger *zap.Logger
}

// NewExAnteEquivocator constructs the behavior.
func NewExAnteEquivocator(logger *zap.Logger) *ExAnteEquivocator {
	return &ExAnteEquivocator{logger: logger}
}

// Outbound mutates TimestampMessage.Value for half of the neighbors based on a
// deterministic hash of the recipient peer ID and session.
func (b *ExAnteEquivocator) Outbound(env *Envelope) Decision {
	if env == nil || env.Message == nil || env.Message.MessageData == nil {
		return SendNow()
	}

	if !strings.HasPrefix(env.ProtocolID, exAnteProtocolPrefix) {
		return SendNow()
	}

	msg := &pb.TimestampMessage{}
	if err := proto.Unmarshal(env.Message.Payload, msg); err != nil {
		if b.logger != nil {
			b.logger.Debug("Equivocator: failed to decode payload", zap.Error(err))
		}
		return SendNow()
	}

	if msg.SessionId == "" || len(msg.Value) == 0 {
		return SendNow()
	}

	if b.split(env.To, msg.SessionId) == 0 {
		// Group 0 receives the original payload unchanged.
		return SendNow()
	}

	alt := hash.Sum(msg.Value, []byte(env.Message.MessageData.Id), []byte(env.To))
	msg.Value = alt
	mutated, err := proto.Marshal(msg)
	if err != nil {
		if b.logger != nil {
			b.logger.Warn("Equivocator: failed to re-encode payload", zap.Error(err))
		}
		return SendNow()
	}

	env.Message.Payload = mutated
	if b.logger != nil {
		b.logger.Debug(
			"Equivocator: mutated Ex-Ante value",
			zap.String("recipient", string(env.To)),
			zap.String("message_id", env.Message.MessageData.Id),
		)
	}

	return SendNow()
}

// Inbound is a no-op for the equivocator.
func (b *ExAnteEquivocator) Inbound(_ *Envelope) Decision {
	return SendNow()
}

func (b *ExAnteEquivocator) split(peerID peer.ID, session string) int {
	digest := hash.Sum([]byte(peerID), []byte(session))
	if len(digest) == 0 {
		return 0
	}
	return int(digest[0] & 1)
}
