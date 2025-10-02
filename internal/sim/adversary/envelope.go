package adversary

import (
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	pproto "github.com/mamorski/committee-sampling/pkg/proto"
)

// Envelope captures metadata about a protocol message flowing through the
// simulated network. Behaviors can inspect or mutate the envelope prior to
// delivery.
type Envelope struct {
	ProtocolID string
	From       peer.ID
	To         peer.ID
	Message    *pproto.ProtocolMessage
	Payload    proto.Message // optional decoded payload; may be nil when unused
}

// Action enumerates the possible routing choices an adversarial behavior can
// apply to a message.
type Action int

const (
	ActionSend Action = iota
	ActionDrop
	ActionDelay
)

// Decision communicates how the decorator should handle a message.
type Decision struct {
	Action Action
	Delay  time.Duration
}

// Behavior exposes hooks for outbound and inbound traffic.
type Behavior interface {
	Outbound(*Envelope) Decision
	Inbound(*Envelope) Decision
}

// SendNow is a convenience helper for behaviors that do not wish to change
// routing.
func SendNow() Decision {
	return Decision{Action: ActionSend}
}

// Drop is a helper decision instructing the decorator to omit delivery.
func Drop() Decision {
	return Decision{Action: ActionDrop}
}

// DelayBy returns a decision requesting a deterministic delay before delivery.
func DelayBy(d time.Duration) Decision {
	return Decision{Action: ActionDelay, Delay: d}
}
