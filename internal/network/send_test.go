package network

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestSendProtocolMessageRoundTrip verifies that SendProtocolMessage delivers
// payloads to a real peer's registered handler over libp2p, in order.
func TestSendProtocolMessageRoundTrip(t *testing.T) {
	h1, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	defer h1.Close()
	h2, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	require.NoError(t, err)
	defer h2.Close()

	ctx := context.Background()
	require.NoError(t, h1.Connect(ctx, peer.AddrInfo{ID: h2.ID(), Addrs: h2.Addrs()}))

	const protoID = "/test/framing/1.0.0"

	receiver := &P2PNode{
		host:        h2,
		ctx:         ctx,
		logger:      zap.NewNop(),
		appBytes:    newAppByteTracker(),
		sendTimeout: 5 * time.Second,
	}
	got := make(chan []byte, 2)
	receiver.RegisterHandler(protoID, func(_ peer.ID, payload []byte) error {
		got <- payload
		return nil
	})

	h1PubKey, err := crypto.MarshalPublicKey(h1.Peerstore().PubKey(h1.ID()))
	require.NoError(t, err)
	sender := &P2PNode{
		host:        h1,
		ctx:         ctx,
		logger:      zap.NewNop(),
		appBytes:    newAppByteTracker(),
		sendTimeout: 5 * time.Second,
		dialTimeout: 5 * time.Second,
		nodeID:      h1.ID().String(),
		nodePubKey:  h1PubKey,
		neighbors: map[peer.ID]peer.AddrInfo{
			h2.ID(): {ID: h2.ID(), Addrs: h2.Addrs()},
		},
		streamPool: make(map[streamKey]*pooledStream),
	}

	sender.SendProtocolMessage(protoID, []byte("first"))
	sender.SendProtocolMessage(protoID, []byte("second"))

	// Both messages must arrive, in order.
	for _, want := range []string{"first", "second"} {
		select {
		case payload := <-got:
			require.Equal(t, want, string(payload))
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %q", want)
		}
	}
}
