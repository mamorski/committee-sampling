package network

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mamorski/committee-sampling/pkg/config"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/mamorski/committee-sampling/internal/network/discovery"
	pproto "github.com/mamorski/committee-sampling/pkg/proto"
)

const (
	neighborhoodRequest  = "/neighborhood/req/1.0.0"
	neighborhoodResponse = "/neighborhood/resp/1.0.0"
	clientVersion        = "go-p2p-node/0.0.1"
)

type MessageHandler func(from string, payload []byte) error

type Network interface {
	RegisterHandler(protocolID string, handler MessageHandler)
	SendProtocolMessage(protocolID string, data []byte)
	GetNeighbors() []string
	GetNodeID() string
	Close() error
	Subscribe(topic string) (<-chan []byte, error)
	VerifySignature(pubKey, message, signature []byte) (bool, error)
}

type Host interface {
	ID() peer.ID
	Close() error
	SetStreamHandler(proto protocol.ID, handler network.StreamHandler)
	Connect(ctx context.Context, addrInfo peer.AddrInfo) error
	NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error)
	Peerstore() peerstore.Peerstore
}

type P2PNode struct {
	host               Host
	ctx                context.Context
	cancel             context.CancelFunc
	logger             *zap.Logger
	neighbors          sync.Map
	discovery          discovery.PeerDiscovery
	maxOutbound        int
	numOfNeighbors     int
	heartbeatInterval  time.Duration
	key                crypto.PrivKey
	topic              string
	pubsub             *pubsub.PubSub
	findPeersTimeout   time.Duration
	stopReceivingPeers bool
}

func New(ctx context.Context, cfg config.Network, logger *zap.Logger) (*P2PNode, error) {
	c, cancel := context.WithCancel(ctx)

	// Generate private key
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	// Create multiaddress for listening
	listenAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.ListenPort))
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create multiaddr: %w", err)
	}

	// Create libp2p h
	h, err := libp2p.New(
		libp2p.ListenAddrs(listenAddr),
		libp2p.Identity(priv),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create h: %w", err)
	}

	// Create pubsub service using GossipSub
	ps, err := pubsub.NewGossipSub(c, h)
	if err != nil {
		_ = h.Close()
		cancel()
		return nil, fmt.Errorf("failed to create pubsub: %w", err)
	}

	// Create d service
	d, err := discovery.NewDiscovery(h, cfg.DiscoveryConfig)
	if err != nil {
		_ = h.Close()
		cancel()
		return nil, fmt.Errorf("failed to create d service: %w", err)
	}

	node := &P2PNode{
		host:               h,
		ctx:                c,
		cancel:             cancel,
		maxOutbound:        cfg.MaxOutboundDegree,
		heartbeatInterval:  cfg.HeartbeatInterval,
		discovery:          d,
		logger:             logger.Named("network"),
		numOfNeighbors:     0,
		key:                priv,
		topic:              cfg.Topic,
		pubsub:             ps,
		findPeersTimeout:   cfg.FindPeersTimeout,
		stopReceivingPeers: false,
	}

	// Set stream handler
	h.SetStreamHandler(neighborhoodRequest, node.onNeighborRequest)
	h.SetStreamHandler(neighborhoodResponse, node.onNeighborResponse)

	// Start d
	if err := d.Start(c); err != nil {
		_ = node.Close()
		return nil, fmt.Errorf("failed to start d: %w", err)
	}

	go node.handleDiscoveredPeers()

	return node, nil
}

func (n *P2PNode) Close() error {
	n.cancel()
	if err := n.discovery.Stop(); err != nil {
		return fmt.Errorf("failed to stop discovery: %w", err)
	}
	return n.host.Close()
}

func (n *P2PNode) SendProtocolMessage(protocolID string, data []byte) {
	n.neighbors.Range(func(key, value interface{}) bool {
		addrInfo := value.(peer.AddrInfo)

		m := &pproto.ProtocolMessage{
			Payload:     data,
			MessageData: n.newMessageData(uuid.New().String(), false),
		}

		signature, err := n.signProtoMessage(m)
		if err != nil {
			n.logger.Error("Failed to sign message", zap.Error(err))
			return true
		}

		m.MessageData.Sign = signature
		n.send(addrInfo, protocol.ID(protocolID), m)
		return true
	})
}

func (n *P2PNode) GetNeighbors() []string {
	neighbors := make([]string, 0)
	n.neighbors.Range(func(key, value interface{}) bool {
		neighbors = append(neighbors, key.(peer.ID).String())
		return true
	})

	return neighbors
}

func (n *P2PNode) RegisterHandler(protocolID string, handler MessageHandler) {
	n.host.SetStreamHandler(protocol.ID(protocolID), func(s network.Stream) {
		data := &pproto.ProtocolMessage{}
		buf, err := io.ReadAll(s)
		if err != nil {
			n.logger.Error("Failed to read message", zap.Error(err))
			return
		}
		_ = s.Close()

		err = proto.Unmarshal(buf, data)
		if err != nil {
			n.logger.Error("Failed to unmarshal EX ANTE message", zap.Error(err))
			return
		}

		if !n.authenticateMessage(data, data.MessageData) {
			n.logger.Error("Failed to authenticate message")
			return
		}

		err = handler(s.Conn().RemotePeer().String(), data.Payload)
		if err != nil {
			n.logger.Error("Failed to handle message", zap.Error(err))
		}
	})
}

func (n *P2PNode) GetNodeID() string {
	return n.host.ID().String()
}

func (n *P2PNode) handleDiscoveredPeers() {
	ch := n.discovery.DiscoveredPeers()
	timeout := time.NewTicker(n.findPeersTimeout)
	defer timeout.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-timeout.C:
			n.stopReceivingPeers = true
			return
		case pi := <-ch:
			n.sendRequestToNeighbor(pi)
		}
	}

}

func (n *P2PNode) onNeighborRequest(s network.Stream) {
	data := &pproto.NeighborMessage{}
	buf, err := io.ReadAll(s)
	if err != nil {
		n.logger.Error("Failed to read neighbor request message", zap.Error(err))
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		n.logger.Error("Failed to unmarshal negotiation message", zap.Error(err))
		return
	}

	n.logger.Debug("Received negotiation request", zap.Any("data", data))

	if !n.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate message")
		return
	}

	resp := &pproto.NeighborMessageResponse{
		MessageData: n.newMessageData(data.MessageData.Id, false),
		Success:     true,
	}

	err = n.addNeighbor(peer.AddrInfo{
		ID:    s.Conn().RemotePeer(),
		Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
	})
	if err != nil {
		resp.Success = false
	}

	signature, err := n.signProtoMessage(resp)
	if err != nil {
		n.logger.Error("Failed to sign response", zap.Error(err))
		return
	}

	resp.MessageData.Sign = signature
	ok := n.sendProtoMessage(s.Conn().RemotePeer(), neighborhoodResponse, resp)
	if !ok {
		n.logger.Error("Failed to send response")
	}
}

func (n *P2PNode) onNeighborResponse(s network.Stream) {
	data := &pproto.NeighborMessageResponse{}
	buf, err := io.ReadAll(s)
	if err != nil {
		n.logger.Error("Failed to read negotiation message", zap.Error(err))
		return
	}
	_ = s.Close()

	err = proto.Unmarshal(buf, data)
	if err != nil {
		n.logger.Error("Failed to unmarshal negotiation message", zap.Error(err))
		return
	}

	n.logger.Debug("Received negotiation response", zap.Any("data", data))

	if !n.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate message")
		return
	}

	if !data.Success {
		n.logger.Debug("Neighbor rejected", zap.String("peer", s.Conn().RemotePeer().String()))
		return
	}

	err = n.addNeighbor(peer.AddrInfo{
		ID:    s.Conn().RemotePeer(),
		Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
	})
	if err != nil {
		n.logger.Error("Failed to add neighbor", zap.Error(err))
	}

}

func (n *P2PNode) sendRequestToNeighbor(info peer.AddrInfo) {
	if info.ID > n.host.ID() {
		n.logger.Debug("Ignoring peer with higher ID", zap.String("peer", info.ID.String()))
		return
	}

	if n.numOfNeighbors >= n.maxOutbound {
		n.logger.Debug(
			"Already connected to max number of neighbors",
			zap.Int("max", n.maxOutbound),
			zap.Int("current", n.numOfNeighbors),
		)
		return
	}

	if _, ok := n.neighbors.Load(info.ID); ok {
		n.logger.Debug("Already connected to peer", zap.String("peer", info.ID.String()))
		return
	}

	msg := &pproto.NeighborMessage{
		MessageData: n.newMessageData(uuid.New().String(), false),
	}

	signature, err := n.signProtoMessage(msg)
	if err != nil {
		n.logger.Error("Failed to sign request", zap.Error(err))
		return
	}

	msg.MessageData.Sign = signature
	if ok := n.send(info, neighborhoodRequest, msg); !ok {
		n.logger.Error("Failed to send request to neighbor", zap.String("peer", info.ID.String()))
		return
	}
}

func (n *P2PNode) addNeighbor(addrInfo peer.AddrInfo) error {
	// Check if already connected
	if _, ok := n.neighbors.Load(addrInfo.ID); ok {
		return nil
	}

	// Check if we have reached the maximum number of neighbors
	if n.numOfNeighbors >= n.maxOutbound {
		n.logger.Debug(
			"Already connected to max number of neighbors",
			zap.Int("max", n.maxOutbound),
			zap.Int("current", n.numOfNeighbors),
		)
		return fmt.Errorf("max number of neighbors reached")
	}

	// Check if timeout for receiving peers has been reached
	if n.stopReceivingPeers {
		n.logger.Debug("Stopping receiving peers, not adding new neighbor", zap.String("peer", addrInfo.ID.String()))
		return fmt.Errorf("stopping receiving peers")
	}

	err := n.host.Connect(n.ctx, addrInfo)
	if err != nil {
		n.logger.Error("Failed to connect to neighbor", zap.Error(err))
		return err
	}

	n.neighbors.Store(addrInfo.ID, addrInfo)
	n.numOfNeighbors++
	return nil
}

// Subscribe implements the PubSub interface
func (n *P2PNode) Subscribe(topic string) (<-chan []byte, error) {
	topicHandle, err := n.pubsub.Join(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to join topic %s: %w", topic, err)
	}

	sub, err := topicHandle.Subscribe()
	if err != nil {
		return nil, fmt.Errorf("failed to subscribe to topic %s: %w", topic, err)
	}

	msgChan := make(chan []byte) // unbuffered channel for blocking behavior

	go func() {
		defer close(msgChan)
		for {
			msg, err := sub.Next(n.ctx)
			if err != nil {
				if n.ctx.Err() != nil {
					// Context canceled, exit gracefully
					return
				}
				n.logger.Error("Failed to get next pubsub message", zap.Error(err))
				continue
			}

			select {
			case msgChan <- msg.Data:
			case <-n.ctx.Done():
				return
			}
		}
	}()

	return msgChan, nil
}

// VerifySignature implements the PubSub interface using Ed25519
func (n *P2PNode) VerifySignature(pubKeyBytes, message, signature []byte) (bool, error) {
	if len(pubKeyBytes) != ed25519.PublicKeySize {
		return false, fmt.Errorf("invalid public key size: expected %d, got %d", ed25519.PublicKeySize, len(pubKeyBytes))
	}

	pubKey := ed25519.PublicKey(pubKeyBytes)
	valid := ed25519.Verify(pubKey, message, signature)
	return valid, nil
}
