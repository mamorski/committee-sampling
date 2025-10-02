package adversary

import (
	"reflect"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNetworkUnreliabilityDeterministicDrop(t *testing.T) {
	cfg := NetworkUnreliabilityConfig{DropProbability: 0.4}
	behaviorA := NewNetworkUnreliability(zap.NewNop(), 42, cfg)
	behaviorB := NewNetworkUnreliability(zap.NewNop(), 42, cfg)

	var seqA, seqB []Action
	for i := 0; i < 10; i++ {
		seqA = append(seqA, behaviorA.Outbound(&Envelope{}).Action)
		seqB = append(seqB, behaviorB.Outbound(&Envelope{}).Action)
	}

	if !reflect.DeepEqual(seqA, seqB) {
		t.Fatalf("expected deterministic action sequence, got %v vs %v", seqA, seqB)
	}
}

func TestNetworkUnreliabilityJitterWithinRange(t *testing.T) {
	cfg := NetworkUnreliabilityConfig{
		DropProbability: 0,
		JitterMin:       10 * time.Millisecond,
		JitterMax:       25 * time.Millisecond,
	}
	behavior := NewNetworkUnreliability(zap.NewNop(), 7, cfg)
	decision := behavior.Outbound(&Envelope{})

	if decision.Action != ActionDelay {
		t.Fatalf("expected delay action, got %v", decision.Action)
	}
	if decision.Delay < cfg.JitterMin || decision.Delay > cfg.JitterMax {
		t.Fatalf("delay %v out of range [%v, %v]", decision.Delay, cfg.JitterMin, cfg.JitterMax)
	}
}
