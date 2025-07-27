package network

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	mathrand "math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/mamorski/committee-sampling/internal/common"
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
	neighbors          map[peer.ID]peer.AddrInfo // Stores connected peers
	potentialNeighbors sync.Map                  // Stores discovered peers before network building
	discovery          discovery.PeerDiscovery
	maxOutbound        int
	heartbeatInterval  time.Duration
	key                crypto.PrivKey
	findPeersTimeout   time.Duration
	stopReceivingPeers bool
	sync               common.Synchronizer
	mu                 sync.Mutex
}

func New(ctx context.Context, cfg config.Network, logger *zap.Logger, synchronizer common.Synchronizer) (*P2PNode, error) {
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

	logger.Info("Listening on address", zap.String("address", listenAddr.String()))

	// Create libp2p h
	h, err := libp2p.New(
		libp2p.ListenAddrs(listenAddr),
		libp2p.Identity(priv),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create h: %w", err)
	}

	// Create d service
	d, err := discovery.NewDiscovery(h, cfg.DiscoveryConfig, logger)
	if err != nil {
		_ = h.Close()
		cancel()
		return nil, fmt.Errorf("failed to create d service: %w", err)
	}
	logger = logger.With(zap.String("node_id", h.ID().String()))

	node := &P2PNode{
		host:               h,
		ctx:                c,
		cancel:             cancel,
		maxOutbound:        cfg.MaxOutboundDegree,
		heartbeatInterval:  cfg.HeartbeatInterval,
		discovery:          d,
		logger:             logger.Named("network"),
		neighbors:          make(map[peer.ID]peer.AddrInfo),
		key:                priv,
		findPeersTimeout:   cfg.FindPeersTimeout,
		stopReceivingPeers: false,
		sync:               synchronizer,
	}

	// Set stream handler
	h.SetStreamHandler(neighborhoodRequest, node.onNeighborRequest)
	h.SetStreamHandler(neighborhoodResponse, node.onNeighborResponse)

	// Start d
	if err := d.Start(c); err != nil {
		_ = node.Close()
		return nil, fmt.Errorf("failed to start d: %w", err)
	}
	go node.graphBuilder()

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
	for _, addrInfo := range n.neighbors {

		m := &pproto.ProtocolMessage{
			Payload:     data,
			MessageData: n.newMessageData(uuid.New().String(), false),
		}

		signature, err := n.signProtoMessage(m)
		if err != nil {
			n.logger.Error("Failed to sign message", zap.Error(err))
			continue
		}

		m.MessageData.Sign = signature
		n.send(addrInfo, protocol.ID(protocolID), m)
	}
}

func (n *P2PNode) GetNeighbors() []string {
	neighbors := make([]string, 0, len(n.neighbors))
	n.mu.Lock()
	defer n.mu.Unlock()
	for id := range n.neighbors {
		neighbors = append(neighbors, id.String())
	}

	n.logger.Debug("Current neighbors", zap.Int("count", len(neighbors)), zap.Strings("neighbors", neighbors))

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

func (n *P2PNode) graphBuilder() {
	go n.handleDiscoveredPeers(n.ctx)

	ch, err := n.sync.WaitForRound(common.Network, 0)
	if err != nil {
		n.logger.Error("Failed to wait for Network step", zap.Error(err))
		panic(err)
	}

	<-ch
	n.cancel() // Cancel the context to stop the discovery handler
	n.buildNetwork()

	ch, err = n.sync.WaitForRound(common.Network, 1)
	if err != nil {
		n.logger.Error("Failed to wait for Network step", zap.Error(err))
		panic(err)
	}
	<-ch
	n.stopReceivingPeers = true
	n.logger.Info("Network building phase completed")
	n.logger.Info("Discovered and connected to peers", zap.Int("count", len(n.neighbors)))
	n.logger.Info("List of neighbors", zap.Strings("neighbors", n.GetNeighbors()))

}

func (n *P2PNode) handleDiscoveredPeers(ctx context.Context) {
	ch := n.discovery.DiscoveredPeers()

	n.logger.Info("Starting peer discovery handler - collecting potential neighbors",
		zap.Duration("timeout", n.findPeersTimeout),
	)

	for {
		select {
		case <-ctx.Done():
			n.logger.Info("Peer discovery stopped due to context cancellation")
			return
		case pi := <-ch:
			// Skip ourselves and already discovered peers
			if pi.ID == n.host.ID() {
				continue
			}

			if _, exists := n.potentialNeighbors.Load(pi.ID); exists {
				continue
			}

			n.logger.Debug("Adding potential neighbor",
				zap.String("peer_id", pi.ID.String()),
				zap.Strings("addresses", addrsToStrings(pi.Addrs)),
			)
			n.potentialNeighbors.Store(pi.ID, pi)
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

	n.logger.Debug("Received negotiation request",
		zap.Any("Id", data.MessageData.Id),
		zap.String("NodeId", data.MessageData.NodeId),
		zap.String("peer_id", s.Conn().RemotePeer().String()),
	)

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
		n.dropNeighbor(s.Conn().RemotePeer())
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

	n.logger.Debug("Received negotiation response",
		zap.Any("data", data),
		zap.String("NodeId", data.MessageData.NodeId),
		zap.Binary("PK", data.MessageData.NodePubKey),
	)

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
	if _, ok := n.neighbors[info.ID]; ok {
		n.logger.Debug("Already connected to peer", zap.String("peer", info.ID.String()))
		return
	}

	n.logger.Info("Attempting to connect to discovered peer",
		zap.String("peer_id", info.ID.String()),
		zap.Strings("addresses", addrsToStrings(info.Addrs)),
		zap.Int("current_neighbors", len(n.neighbors)),
	)

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

	n.logger.Debug("Successfully sent neighbor request", zap.String("peer", info.ID.String()))
}

func (n *P2PNode) addNeighbor(addrInfo peer.AddrInfo) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	// Check if already connected
	if _, ok := n.neighbors[addrInfo.ID]; ok {
		n.logger.Debug("Neighbor already exists", zap.String("peer", addrInfo.ID.String()))
		return nil
	}

	// Check if timeout for receiving peers has been reached
	if n.stopReceivingPeers {
		n.logger.Debug("Stopping receiving peers, not adding new neighbor", zap.String("peer", addrInfo.ID.String()))
		return fmt.Errorf("stopping receiving peers")
	}

	n.logger.Info("Attempting to add neighbor",
		zap.String("peer_id", addrInfo.ID.String()),
		zap.Strings("addresses", addrsToStrings(addrInfo.Addrs)),
		zap.Int("current_neighbors", len(n.neighbors)),
		zap.Int("max_neighbors", n.maxOutbound),
	)

	err := n.host.Connect(context.Background(), addrInfo)
	if err != nil {
		n.logger.Error("Failed to add to neighbor",
			zap.String("peer", addrInfo.ID.String()),
			zap.Error(err),
		)
		return err
	}

	n.neighbors[addrInfo.ID] = addrInfo // Ensure the peer is stored in the map

	n.logger.Info("Successfully added neighbor",
		zap.String("peer_id", addrInfo.ID.String()),
		zap.Int("total_neighbors", len(n.neighbors)),
	)

	return nil
}

func (n *P2PNode) dropNeighbor(peerID peer.ID) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.neighbors[peerID]; !exists {
		n.logger.Warn("Attempted to drop a non-existing neighbor", zap.String("peer_id", peerID.String()))
		return
	}

	delete(n.neighbors, peerID)
	n.logger.Info("Dropped neighbor", zap.String("peer_id", peerID.String()), zap.Int("remaining_neighbors", len(n.neighbors)))
}

// StartBuildingNetwork initiates the network building phase by randomly selecting
// neighbors from the potential neighbors list and sending connection requests
func (n *P2PNode) buildNetwork() {
	n.logger.Info("Starting network building phase")

	// Get all potential neighbors
	var potentialPeers []peer.AddrInfo
	n.potentialNeighbors.Range(func(key, value interface{}) bool {
		potentialPeers = append(potentialPeers, value.(peer.AddrInfo))
		return true
	})

	if len(potentialPeers) == 0 {
		n.logger.Warn("No potential neighbors found for network building")
		return
	}

	n.logger.Info("Found potential neighbors for network building", zap.Int("count", len(potentialPeers)))

	// Randomly shuffle and select up to maxOutbound neighbors
	for i := len(potentialPeers) - 1; i > 0; i-- {
		j := mathrand.Intn(i + 1) // replace with cryptographically secure random number generator if needed
		potentialPeers[i], potentialPeers[j] = potentialPeers[j], potentialPeers[i]
	}

	connectCount := n.maxOutbound
	if len(potentialPeers) < connectCount {
		connectCount = len(potentialPeers)
	}

	n.logger.Info("Attempting to connect to selected neighbors",
		zap.Int("selected", connectCount),
		zap.Int("available", len(potentialPeers)))

	for i := 0; i < connectCount; i++ {
		go n.sendRequestToNeighbor(potentialPeers[i])
	}
}

// Helper function to convert addresses to strings
func addrsToStrings(addrs []multiaddr.Multiaddr) []string {
	result := make([]string, len(addrs))
	for i, addr := range addrs {
		result[i] = addr.String()
	}
	return result
}
