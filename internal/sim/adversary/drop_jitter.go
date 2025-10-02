package adversary

import (
	"math/rand"
	"time"

	"go.uber.org/zap"
)

// NetworkUnreliabilityConfig captures adversarial knobs for message dropping,
// jitter, and optional clock skew.
type NetworkUnreliabilityConfig struct {
	DropProbability float64
	JitterMin       time.Duration
	JitterMax       time.Duration
}

// NetworkUnreliabilityBehavior applies deterministic drop and jitter decisions
// to outbound messages.
type NetworkUnreliabilityBehavior struct {
	logger *zap.Logger
	rng    *rand.Rand
	cfg    NetworkUnreliabilityConfig
}

// NewNetworkUnreliability constructs the behavior with a pre-seeded RNG.
func NewNetworkUnreliability(logger *zap.Logger, seed int64, cfg NetworkUnreliabilityConfig) *NetworkUnreliabilityBehavior {
	return &NetworkUnreliabilityBehavior{
		logger: logger,
		rng:    rand.New(rand.NewSource(seed)),
		cfg:    cfg,
	}
}

// Outbound applies drop or delay decisions based on deterministic RNG.
func (b *NetworkUnreliabilityBehavior) Outbound(_ *Envelope) Decision {
	if b.cfg.DropProbability > 0 {
		if b.rng.Float64() < b.cfg.DropProbability {
			if b.logger != nil {
				b.logger.Debug("Adversary: dropping outbound message")
			}
			return Drop()
		}
	}

	if b.cfg.JitterMax > 0 {
		jitter := b.jitterDuration()
		if jitter > 0 {
			if b.logger != nil {
				b.logger.Debug("Adversary: delaying outbound message", zap.Duration("delay", jitter))
			}
			return DelayBy(jitter)
		}
	}

	return SendNow()
}

// Inbound leaves inbound traffic untouched for drop/jitter behavior.
func (b *NetworkUnreliabilityBehavior) Inbound(_ *Envelope) Decision {
	return SendNow()
}

func (b *NetworkUnreliabilityBehavior) jitterDuration() time.Duration {
	jitterMin := b.cfg.JitterMin
	jitterMax := b.cfg.JitterMax
	if jitterMax <= jitterMin {
		return jitterMax
	}
	span := jitterMax - jitterMin
	return jitterMin + time.Duration(b.rng.Int63n(int64(span)+1))
}
