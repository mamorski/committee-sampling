package network

import (
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"

	pproto "github.com/mamorski/committee-sampling/pkg/proto"
)

// TestAppByteTracker checks the tracker accumulates payload/envelope counts
// correctly and is safe under the concurrent send fan-out / inbound handlers.
func TestAppByteTracker(t *testing.T) {
	tr := newAppByteTracker()
	const (
		goroutines = 8
		iters      = 1000
		pid        = "/expost/1.0.0/sid"
	)

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				tr.addOut(pid, 10, 30)
				tr.addIn(pid, 5, 25)
			}
		}()
	}
	wg.Wait()

	snap := tr.snapshot()
	c, ok := snap[pid]
	if !ok {
		t.Fatalf("protocol %q missing from snapshot", pid)
	}
	n := int64(goroutines * iters)
	if c.PayloadOut != n*10 || c.EnvOut != n*30 || c.PayloadIn != n*5 || c.EnvIn != n*25 {
		t.Fatalf("unexpected counts: %+v (n=%d)", c, n)
	}
}

// TestEnvelopeExceedsPayload confirms the ProtocolMessage wrapping (signature +
// MessageData) makes the envelope strictly larger than the payload, which is the
// overhead the payload/envelope split is meant to surface.
func TestEnvelopeExceedsPayload(t *testing.T) {
	payload := []byte("representative timestamp-message payload bytes")
	m := &pproto.ProtocolMessage{
		Payload: payload,
		MessageData: &pproto.MessageData{
			NodeId:     "12D3KooWBmwXbxv2cBmDxNXAyqgr1WvVZc8oWfhT1Qof9aaa",
			NodePubKey: make([]byte, 36),
			Sign:       make([]byte, 64),
		},
	}

	env := proto.Size(m)
	if env <= len(payload) {
		t.Fatalf("envelope %d not greater than payload %d", env, len(payload))
	}
	// The signature alone is 64 bytes, so the overhead must be at least that.
	if overhead := env - len(payload); overhead < 64 {
		t.Fatalf("envelope overhead %d smaller than the 64-byte signature", overhead)
	}
}
