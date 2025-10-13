package adversary

import (
	"strings"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/mamorski/committee-sampling/internal/hash"
	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

const (
	expostProtocolPrefix = "/expost/"
	maxStaleRounds       = ^uint32(0)
	minStaleRounds       = uint32(1)
)

// FreshnessMode controls the cheating strategy applied to Ex-Post messages.
type FreshnessMode string

const (
	FreshnessStale     FreshnessMode = "stale"
	FreshnessTruncated FreshnessMode = "truncate"
	FreshnessBoth      FreshnessMode = "both"
)

// FreshnessCheaterBehavior tampers with Ex-Post challenge or Merkle paths.
type FreshnessCheaterBehavior struct {
	logger      *zap.Logger
	mode        FreshnessMode
	staleRounds uint32
	truncate    bool
	history     map[string]map[uint32][]byte
	mu          sync.Mutex
}

// NewFreshnessCheater builds the behavior from configuration.
func NewFreshnessCheater(logger *zap.Logger, mode FreshnessMode, staleRounds int, truncate bool) *FreshnessCheaterBehavior {
	shouldTruncate := truncate
	if mode == FreshnessTruncated {
		shouldTruncate = true
	}
	return &FreshnessCheaterBehavior{
		logger:      logger,
		mode:        mode,
		staleRounds: sanitizeStaleRounds(staleRounds),
		truncate:    shouldTruncate,
		history:     make(map[string]map[uint32][]byte),
	}
}

// Outbound applies the configured tampering to Ex-Post Timestamp messages.
func (b *FreshnessCheaterBehavior) Outbound(env *Envelope) Decision {
	if env == nil || env.Message == nil || env.Message.MessageData == nil {
		return SendNow()
	}
	if !strings.HasPrefix(env.ProtocolID, expostProtocolPrefix) {
		return SendNow()
	}

	msg := &pb.TimestampMessage{}
	if err := proto.Unmarshal(env.Message.Payload, msg); err != nil {
		if b.logger != nil {
			b.logger.Debug("Freshness cheater: decode failed", zap.Error(err))
		}
		return SendNow()
	}

	sessionKey := msg.SessionId
	original := append([]byte(nil), msg.Value...)
	modified := false

	if b.shouldUseStale() {
		if stale := b.staleValue(sessionKey, msg.Round); len(stale) > 0 {
			msg.Value = stale
			modified = true
		}
	}

	if b.truncate && msg.MerklePath != nil && len(msg.MerklePath) > 0 {
		last := msg.MerklePath[len(msg.MerklePath)-1]
		if last != nil && len(last.Row) > 0 {
			last.Row = last.Row[:len(last.Row)-1]
			modified = true
		}
	}

	if !modified {
		b.recordValue(sessionKey, msg.Round, original)
		return SendNow()
	}

	encoded, err := proto.Marshal(msg)
	if err != nil {
		if b.logger != nil {
			b.logger.Warn("Freshness cheater: re-encode failed", zap.Error(err))
		}
		b.recordValue(sessionKey, msg.Round, original)
		return SendNow()
	}

	env.Message.Payload = encoded
	if b.logger != nil {
		b.logger.Debug("Freshness cheater: tampered message", zap.String("mode", string(b.mode)))
	}

	b.recordValue(sessionKey, msg.Round, original)

	return SendNow()
}

// Inbound is a pass-through.
func (b *FreshnessCheaterBehavior) Inbound(_ *Envelope) Decision {
	return SendNow()
}

func (b *FreshnessCheaterBehavior) shouldUseStale() bool {
	return b.mode == FreshnessStale || b.mode == FreshnessBoth
}

func (b *FreshnessCheaterBehavior) staleValue(session string, round uint32) []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	history := b.ensureHistoryLocked(session)
	if round < b.staleRounds {
		if val, ok := history[0]; ok {
			return append([]byte(nil), val...)
		}
		return nil
	}
	lookback := round - b.staleRounds
	if val, ok := history[lookback]; ok {
		return append([]byte(nil), val...)
	}
	if seed, ok := history[0]; ok {
		return DeriveAltChallenge(seed, session)
	}
	return nil
}

func (b *FreshnessCheaterBehavior) recordValue(session string, round uint32, value []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	history := b.ensureHistoryLocked(session)
	if _, exists := history[round]; !exists {
		history[round] = append([]byte(nil), value...)
	}
}

func (b *FreshnessCheaterBehavior) ensureHistoryLocked(session string) map[uint32][]byte {
	h, ok := b.history[session]
	if !ok {
		h = make(map[uint32][]byte)
		b.history[session] = h
	}
	return h
}

func sanitizeStaleRounds(input int) uint32 {
	if input < int(minStaleRounds) {
		return minStaleRounds
	}

	sanitized := int64(input)
	if sanitized > int64(maxStaleRounds) {
		return maxStaleRounds
	}

	return uint32(sanitized) // #nosec G115 -- sanitized is guaranteed within [minStaleRounds, maxStaleRounds]
}

// DeriveAltChallenge exposes deterministic stale challenge generation.
func DeriveAltChallenge(seed []byte, session string) []byte {
	if len(seed) == 0 {
		return nil
	}
	return hash.Sum(seed, []byte(session))
}

// ParseFreshnessMode normalises configuration input to a supported mode.
func ParseFreshnessMode(mode string) FreshnessMode {
	switch strings.ToLower(mode) {
	case string(FreshnessTruncated):
		return FreshnessTruncated
	case string(FreshnessBoth):
		return FreshnessBoth
	case string(FreshnessStale):
		fallthrough
	default:
		return FreshnessStale
	}
}
