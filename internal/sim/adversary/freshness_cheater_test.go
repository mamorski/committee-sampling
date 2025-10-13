package adversary

import (
	"math"
	"math/bits"
	"testing"

	"go.uber.org/zap"
	gproto "google.golang.org/protobuf/proto"

	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

func TestFreshnessCheaterStaleMode(t *testing.T) {
	behavior := NewFreshnessCheater(zap.NewNop(), FreshnessStale, 1, false)
	session := "sid"

	msg0 := &pb.TimestampMessage{SessionId: session, Round: 0, Value: []byte("v0")}
	env0 := envelopeForExPost(msg0)
	behavior.Outbound(env0)

	msg1 := &pb.TimestampMessage{SessionId: session, Round: 1, Value: []byte("v1")}
	env1 := envelopeForExPost(msg1)
	behavior.Outbound(env1)

	decoded := &pb.TimestampMessage{}
	if err := gproto.Unmarshal(env1.Message.Payload, decoded); err != nil {
		t.Fatalf("decode mutated failed: %v", err)
	}
	if string(decoded.Value) != "v0" {
		t.Fatalf("expected stale value 'v0', got %q", decoded.Value)
	}
}

func TestFreshnessCheaterTruncatesMerklePath(t *testing.T) {
	behavior := NewFreshnessCheater(zap.NewNop(), FreshnessTruncated, 1, true)
	msg := &pb.TimestampMessage{
		SessionId: "sid",
		Round:     2,
		Value:     []byte("value"),
		MerklePath: []*pb.State{
			{Row: [][]byte{[]byte("a"), []byte("b")}},
		},
	}

	env := envelopeForExPost(msg)
	behavior.Outbound(env)

	decoded := &pb.TimestampMessage{}
	if err := gproto.Unmarshal(env.Message.Payload, decoded); err != nil {
		t.Fatalf("decode mutated failed: %v", err)
	}
	if len(decoded.MerklePath[0].Row) != 1 {
		t.Fatalf("expected truncated merkle row length 1, got %d", len(decoded.MerklePath[0].Row))
	}
}

func TestFreshnessCheaterIgnoresOtherProtocols(t *testing.T) {
	behavior := NewFreshnessCheater(zap.NewNop(), FreshnessStale, 1, true)
	msg := &pb.TimestampMessage{SessionId: "sid", Round: 0, Value: []byte("value")}
	payload, _ := gproto.Marshal(msg)
	env := &Envelope{ProtocolID: "/exante/1.0.0/sid", Message: &pb.ProtocolMessage{Payload: payload}}

	behavior.Outbound(env)
	decoded := &pb.TimestampMessage{}
	if err := gproto.Unmarshal(env.Message.Payload, decoded); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if string(decoded.Value) != "value" {
		t.Fatalf("expected untouched value, got %q", decoded.Value)
	}
}

func TestSanitizeStaleRounds(t *testing.T) {
	tests := []struct {
		name  string
		input int
		want  uint32
	}{
		{name: "clamps negative", input: -5, want: minStaleRounds},
		{name: "clamps zero", input: 0, want: minStaleRounds},
		{name: "keeps positive", input: 7, want: 7},
	}

	maxIntExpectation := maxStaleRounds
	if bits.UintSize == 32 {
		maxIntExpectation = uint32(math.MaxInt32)
	}
	tests = append(tests, struct {
		name  string
		input int
		want  uint32
	}{
		name:  "caps architecture max",
		input: math.MaxInt,
		want:  maxIntExpectation,
	})

	if bits.UintSize == 64 {
		oversized := int(int64(maxStaleRounds) + 42)
		tests = append(tests, struct {
			name  string
			input int
			want  uint32
		}{
			name:  "caps beyond uint32",
			input: oversized,
			want:  maxStaleRounds,
		})
	}

	for _, tt := range tests {
		if got := sanitizeStaleRounds(tt.input); got != tt.want {
			t.Fatalf("%s: sanitizeStaleRounds(%d) = %d, want %d", tt.name, tt.input, got, tt.want)
		}
	}
}

func envelopeForExPost(msg *pb.TimestampMessage) *Envelope {
	payload, _ := gproto.Marshal(msg)
	return &Envelope{
		ProtocolID: "/expost/1.0.0/" + msg.SessionId,
		Message: &pb.ProtocolMessage{
			Payload:     payload,
			MessageData: &pb.MessageData{Id: "msg"},
		},
	}
}
