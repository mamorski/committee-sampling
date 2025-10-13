package adversary

import (
	"fmt"
	"testing"

	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	gproto "google.golang.org/protobuf/proto"

	"github.com/mamorski/committee-sampling/internal/hash"
	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

func TestExAnteEquivocatorMutatesHalf(t *testing.T) {
	behavior := NewExAnteEquivocator(zap.NewNop())

	base := &pb.TimestampMessage{
		SessionId:       "sid",
		VerificationKey: []byte("vk"),
		Value:           []byte("value"),
		Round:           1,
		Id:              "sender",
	}
	payload, err := gproto.Marshal(base)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}

	msgData := &pb.MessageData{Id: "msg"}
	group0, group1 := selectPeers(behavior, base.SessionId)

	makeEnvelope := func(to peer.ID) *Envelope {
		copyPayload := append([]byte(nil), payload...)
		return &Envelope{
			ProtocolID: "/exante/1.0.0/sid",
			From:       peer.ID("from"),
			To:         to,
			Message: &pb.ProtocolMessage{
				Payload:     copyPayload,
				MessageData: &pb.MessageData{Id: msgData.Id},
			},
		}
	}

	altExpected := hash.Sum(base.Value, []byte(msgData.Id), []byte(group1))

	mutEnv := makeEnvelope(group1)
	behavior.Outbound(mutEnv)
	mutated := &pb.TimestampMessage{}
	if err := gproto.Unmarshal(mutEnv.Message.Payload, mutated); err != nil {
		t.Fatalf("unmarshal mutated failed: %v", err)
	}
	if string(mutated.Value) != string(altExpected) {
		t.Fatalf("expected mutated value, got %q", mutated.Value)
	}

	origEnv := makeEnvelope(group0)
	behavior.Outbound(origEnv)
	unmutated := &pb.TimestampMessage{}
	if err := gproto.Unmarshal(origEnv.Message.Payload, unmutated); err != nil {
		t.Fatalf("unmarshal original failed: %v", err)
	}
	if string(unmutated.Value) != string(base.Value) {
		t.Fatalf("expected original value, got %q", unmutated.Value)
	}
}

func selectPeers(eq *ExAnteEquivocator, session string) (peer.ID, peer.ID) {
	var group0, group1 peer.ID
	for i := 0; group0 == "" || group1 == ""; i++ {
		pid := peer.ID(fmt.Sprintf("peer-%d", i))
		if eq.split(pid, session) == 0 {
			if group0 == "" {
				group0 = pid
			}
			continue
		}
		if group1 == "" {
			group1 = pid
		}
	}
	return group0, group1
}

func TestExAnteEquivocatorIgnoresOtherProtocols(t *testing.T) {
	behavior := NewExAnteEquivocator(zap.NewNop())
	msg := &pb.ProtocolMessage{Payload: []byte("payload"), MessageData: &pb.MessageData{Id: "x"}}
	env := &Envelope{ProtocolID: "/expost/1.0.0/sid", Message: msg}

	behavior.Outbound(env)
	if string(env.Message.Payload) != "payload" {
		t.Fatalf("expected payload to remain unchanged")
	}
}
