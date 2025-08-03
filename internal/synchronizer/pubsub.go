package synchronizer

import (
	"context"
	"crypto/ed25519"
	"fmt"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

const DiscoveryServiceTag = "pubsub-chat-example"

type PubSubService struct {
	host   host.Host
	pubSub *pubsub.PubSub
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger
}

func NewPubSubService(ctx context.Context, listenPort int, logger *zap.Logger) (*PubSubService, error) {
	c, cancel := context.WithCancel(ctx)

	// Create multiaddress for listening
	listenAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create multiaddr: %w", err)
	}

	logger.Info("PubSub listening on address", zap.String("address", listenAddr.String()))

	// Create libp2p host
	h, err := libp2p.New(
		libp2p.ListenAddrs(listenAddr),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create host: %w", err)
	}

	// Create pubSub service using GossipSub
	ps, err := pubsub.NewGossipSub(c, h)
	if err != nil {
		_ = h.Close()
		cancel()
		return nil, fmt.Errorf("failed to create pubSub: %w", err)
	}

	// 3. Setup peer discovery
	// We use mDNS for local peer discovery
	if err := mdns.NewMdnsService(h, DiscoveryServiceTag, &discoveryNotifee{h: h}).Start(); err != nil {
		panic(err)
	}

	return &PubSubService{
		host:   h,
		pubSub: ps,
		ctx:    c,
		cancel: cancel,
		logger: logger.Named("pubSub"),
	}, nil
}

func (ps *PubSubService) Subscribe(topic string) (<-chan []byte, error) {
	topicHandle, err := ps.pubSub.Join(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to join topic %s: %w", topic, err)
	}

	sub, err := topicHandle.Subscribe()
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	msgChan := make(chan []byte)

	go func() {
		defer close(msgChan)
		for {
			msg, err := sub.Next(ps.ctx)
			if err != nil {
				if ps.ctx.Err() != nil {
					return
				}
				ps.logger.Error("Failed to get next pubSub message", zap.Error(err))
				continue
			}

			select {
			case msgChan <- msg.Data:
			case <-ps.ctx.Done():
				return
			}
		}
	}()

	return msgChan, nil
}

func (ps *PubSubService) VerifySignature(pubKeyBytes, message, signature []byte) (bool, error) {
	if len(pubKeyBytes) != ed25519.PublicKeySize {
		return false, fmt.Errorf("invalid public key size: expected %d, got %d", ed25519.PublicKeySize, len(pubKeyBytes))
	}

	pubKey := ed25519.PublicKey(pubKeyBytes)
	valid := ed25519.Verify(pubKey, message, signature)
	return valid, nil
}

func (ps *PubSubService) Close() error {
	ps.cancel()
	return ps.host.Close()
}

// discoveryNotifee gets notified when we find a new peer via mDNS discovery
type discoveryNotifee struct {
	h host.Host
}

// HandlePeerFound connects to peers discovered via mDNS. Once they're connected,
// the PubSub system will automatically start interacting with them if they also
// support PubSub.
func (n *discoveryNotifee) HandlePeerFound(pi peer.AddrInfo) {
	fmt.Printf("discovered new peer %s\n", pi.ID)
	err := n.h.Connect(context.Background(), pi)
	if err != nil {
		fmt.Printf("error connecting to peer %s: %s\n", pi.ID, err)
	}
}
