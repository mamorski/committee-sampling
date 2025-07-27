package synchronizer

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

type PubSubService struct {
	host   host.Host
	pubsub *pubsub.PubSub
	ctx    context.Context
	cancel context.CancelFunc
	logger *zap.Logger
}

func NewPubSubService(ctx context.Context, listenPort int, logger *zap.Logger) (*PubSubService, error) {
	c, cancel := context.WithCancel(ctx)

	// Generate private key
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

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
		libp2p.Identity(priv),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create host: %w", err)
	}

	// Create pubsub service using GossipSub
	ps, err := pubsub.NewGossipSub(c, h)
	if err != nil {
		_ = h.Close()
		cancel()
		return nil, fmt.Errorf("failed to create pubsub: %w", err)
	}

	return &PubSubService{
		host:   h,
		pubsub: ps,
		ctx:    c,
		cancel: cancel,
		logger: logger.Named("pubsub"),
	}, nil
}

func (ps *PubSubService) Subscribe(topic string) (<-chan []byte, error) {
	topicHandle, err := ps.pubsub.Join(topic)
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
				ps.logger.Error("Failed to get next pubsub message", zap.Error(err))
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
