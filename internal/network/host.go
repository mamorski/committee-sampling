package network

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/network/discovery"
	"github.com/mamorski/committee-sampling/pkg/config"
	pproto "github.com/mamorski/committee-sampling/pkg/proto"
)

const (
	neighborhoodRequest   = "/neighborhood/req/1.0.0"
	neighborhoodResponse  = "/neighborhood/resp/1.0.0"
	clientVersion         = "go-p2p-node/0.0.1"
	maxInboundMessageSize = 1 << 20 // 1 MiB safety cap on inbound payloads
)

type MessageHandler func(from peer.ID, payload []byte) error

type Network interface {
	RegisterHandler(protocolID string, handler MessageHandler)
	SendProtocolMessage(protocolID string, data []byte)
	GetNeighbors() []string
	GetNodeID() string
	Close() error
	IsNeighbor(peerID peer.ID) bool
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
	host                        Host
	ctx                         context.Context
	cancel                      context.CancelFunc
	logger                      *zap.Logger
	neighbors                   map[peer.ID]peer.AddrInfo // Stores connected peers
	potentialNeighbors          sync.Map                  // Stores discovered peers before network building
	discovery                   discovery.PeerDiscovery
	maxOutbound                 int
	key                         crypto.PrivKey
	acceptingPotentialNeighbors atomic.Bool
	sync                        common.Synchronizer
	mu                          sync.Mutex
	connectivityRetries         int
	sid                         string
	// simulation options
	dropOnSend            bool
	dropOnSendProbability float64
}

func New(ctx context.Context, cfg config.Network, logger *zap.Logger, synchronizer common.Synchronizer, sid string) (*P2PNode, error) {
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
		libp2p.ListenAddrs(listenAddr), libp2p.Identity(priv),
	)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create h: %w", err)
	}

	// Create d service
	d := discovery.NewDHTDiscovery(h, cfg.DiscoveryConfig, logger)
	logger = logger.With(zap.String("node_id", h.ID().String()))

	node := &P2PNode{
		host:                        h,
		ctx:                         c,
		cancel:                      cancel,
		maxOutbound:                 cfg.MaxOutboundDegree,
		discovery:                   d,
		logger:                      logger.Named("network"),
		neighbors:                   make(map[peer.ID]peer.AddrInfo),
		key:                         priv,
		acceptingPotentialNeighbors: atomic.Bool{},
		sync:                        synchronizer,
		sid:                         sid,
		connectivityRetries: func() int {
			if cfg.ConnectivityRetries <= 0 {
				return 3
			}
			return cfg.ConnectivityRetries
		}(),
		dropOnSend: cfg.DropOnSend,
		dropOnSendProbability: func() float64 {
			p := cfg.DropOnSendProbability
			if p < 0 {
				p = 0
			}
			if p > 1 {
				p = 1
			}
			return p
		}(),
	}

	if node.dropOnSend {
		node.logger.Info(
			"Simulation: drop-on-send enabled", zap.Float64("probability", node.dropOnSendProbability),
		)
	}

	node.acceptingPotentialNeighbors.Store(true)
	// Set stream handler
	h.SetStreamHandler(protocol.ID(neighborhoodRequest+"/"+sid), node.onNeighborRequest)
	h.SetStreamHandler(protocol.ID(neighborhoodResponse+"/"+sid), node.onNeighborResponse)

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

func cryptoFloat64() (float64, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	u := binary.BigEndian.Uint64(b[:]) >> 11 // keep the top 53 bits
	return float64(u) / (1 << 53), nil
}

func (n *P2PNode) SendProtocolMessage(protocolID string, data []byte) {
	// snapshot neighbors to avoid concurrent map writes if send() drops neighbors
	n.mu.Lock()
	snapshot := make([]peer.AddrInfo, 0, len(n.neighbors))
	for _, ai := range n.neighbors {
		snapshot = append(snapshot, ai)
	}
	n.mu.Unlock()
	for _, addrInfo := range snapshot {
		// simulation: optional probabilistic drop per recipient using crypto-secure RNG (avoid G404)
		if n.dropOnSend {
			// draw a uniform float in [0,1) using 53 random bits (float64 mantissa)
			f, err := cryptoFloat64()
			if err != nil {
				n.logger.Warn("Simulation: crypto RNG failed; skipping drop decision", zap.Error(err))
			} else if f < n.dropOnSendProbability {
				n.logger.Info(
					"Simulation: dropped outgoing message",
					zap.String("peer_id", addrInfo.ID.String()),
					zap.Float64("probability", n.dropOnSendProbability),
					zap.String("protocol", protocolID),
					zap.Float64("random_float", f),
				)
				continue
			}
		}

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

func (n *P2PNode) IsNeighbor(peerID peer.ID) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	_, exists := n.neighbors[peerID]
	return exists
}

func (n *P2PNode) neighborCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.neighbors)
}

func (n *P2PNode) getNeighbor(peerID peer.ID) (peer.AddrInfo, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	info, ok := n.neighbors[peerID]
	return info, ok
}

func (n *P2PNode) RegisterHandler(protocolID string, handler MessageHandler) {
	n.host.SetStreamHandler(
		protocol.ID(protocolID), func(s network.Stream) {
			data := &pproto.ProtocolMessage{}
			buf, err := readStreamWithLimit(s, int64(maxInboundMessageSize))
			if err != nil {
				n.logger.Error("Failed to read message", zap.Error(err))
				_ = s.Reset()
				return
			}

			if err := s.Close(); err != nil {
				n.logger.Debug("Failed to close stream after read", zap.Error(err))
			}

			if err := proto.Unmarshal(buf, data); err != nil {
				n.logger.Error("Failed to unmarshal EX ANTE message", zap.Error(err))
				return
			}

			if !n.authenticateMessage(data, data.MessageData) {
				n.logger.Error("Failed to authenticate message")
				return
			}

			if err := handler(s.Conn().RemotePeer(), data.Payload); err != nil {
				n.logger.Error("Failed to handle message", zap.Error(err))
			}
		},
	)
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
	n.acceptingPotentialNeighbors.Store(false)
	n.buildNetwork()

	ch, err = n.sync.WaitForRound(common.Network, 1)
	if err != nil {
		n.logger.Error("Failed to wait for Network step", zap.Error(err))
		panic(err)
	}
	<-ch
	n.logger.Info("Network building phase completed")
	neighbors := n.GetNeighbors()
	n.logger.Info(
		"List of neighbors", zap.Int("count", len(neighbors)), zap.Strings("neighbors", neighbors),
	)

}

func (n *P2PNode) handleDiscoveredPeers(ctx context.Context) {
	ch := n.discovery.DiscoveredPeers()

	n.logger.Info("Starting peer discovery handler - collecting potential neighbors")

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

			if !n.acceptingPotentialNeighbors.Load() {
				n.logger.Debug("Ignoring newly discovered peer; potential neighbor intake stopped", zap.String("peer_id", pi.ID.String()))
				return
			}

			if _, exists := n.potentialNeighbors.Load(pi.ID); exists {
				continue
			}

			n.logger.Debug(
				"Adding potential neighbor", zap.String("peer_id", pi.ID.String()), zap.Strings("addresses", addrsToStrings(pi.Addrs)),
			)
			n.potentialNeighbors.Store(pi.ID, pi)
		}
	}
}

func (n *P2PNode) onNeighborRequest(s network.Stream) {
	data := &pproto.NeighborMessage{}
	buf, err := readStreamWithLimit(s, int64(maxInboundMessageSize))
	if err != nil {
		n.logger.Error("Failed to read neighbor request message", zap.Error(err))
		_ = s.Reset()
		return
	}
	if err := s.Close(); err != nil {
		n.logger.Debug("Failed to close request stream", zap.Error(err))
	}

	if err := proto.Unmarshal(buf, data); err != nil {
		n.logger.Error("Failed to unmarshal negotiation message", zap.Error(err))
		return
	}

	n.logger.Debug(
		"Received negotiation request",
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

	addrInfo := peer.AddrInfo{
		ID:    s.Conn().RemotePeer(),
		Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
	}

	added := false
	if err := n.addNeighbor(addrInfo); err != nil {
		resp.Success = false
	} else {
		added = true
	}

	signature, err := n.signProtoMessage(resp)
	if err != nil {
		n.logger.Error("Failed to sign response", zap.Error(err))
		return
	}

	resp.MessageData.Sign = signature
	ok := n.send(addrInfo, protocol.ID(neighborhoodResponse+"/"+n.sid), resp)
	if !ok {
		n.logger.Error("Failed to send response")
		if added {
			n.dropNeighbor(addrInfo.ID)
		}
	}
}

func (n *P2PNode) onNeighborResponse(s network.Stream) {
	data := &pproto.NeighborMessageResponse{}
	buf, err := readStreamWithLimit(s, int64(maxInboundMessageSize))
	if err != nil {
		n.logger.Error("Failed to read negotiation message", zap.Error(err))
		_ = s.Reset()
		return
	}
	if err := s.Close(); err != nil {
		n.logger.Debug("Failed to close response stream", zap.Error(err))
	}

	if err := proto.Unmarshal(buf, data); err != nil {
		n.logger.Error("Failed to unmarshal negotiation message", zap.Error(err))
		return
	}

	n.logger.Debug(
		"Received negotiation response", zap.String("NodeId", data.MessageData.NodeId), zap.Binary("PK", data.MessageData.NodePubKey),
	)

	if !n.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate message")
		return
	}

	if !data.Success {
		n.logger.Debug("Neighbor rejected", zap.String("peer", s.Conn().RemotePeer().String()))
		return
	}

	err = n.addNeighbor(
		peer.AddrInfo{
			ID:    s.Conn().RemotePeer(),
			Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
		},
	)
	if err != nil {
		n.logger.Error("Failed to add neighbor", zap.Error(err))
	}
}

func (n *P2PNode) sendRequestToNeighbor(info peer.AddrInfo) error {
	if n.IsNeighbor(info.ID) {
		n.logger.Debug("Already connected to peer", zap.String("peer", info.ID.String()))
		return nil
	}

	currentNeighbors := n.neighborCount()

	n.logger.Info(
		"Attempting to connect to discovered peer",
		zap.String("peer_id", info.ID.String()),
		zap.Strings("addresses", addrsToStrings(info.Addrs)),
		zap.Int("current_neighbors", currentNeighbors),
	)

	msg := &pproto.NeighborMessage{
		MessageData: n.newMessageData(uuid.New().String(), false),
	}

	signature, err := n.signProtoMessage(msg)
	if err != nil {
		n.logger.Error("Failed to sign request", zap.Error(err))
		return errors.New("failed to sign request")
	}

	msg.MessageData.Sign = signature
	if ok := n.send(info, protocol.ID(neighborhoodRequest+"/"+n.sid), msg); !ok {
		n.logger.Error("Failed to send request to neighbor", zap.String("peer", info.ID.String()))
		return errors.New("failed to send request to neighbor")
	}

	n.logger.Debug("Successfully sent neighbor request", zap.String("peer", info.ID.String()))
	return nil
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
	if !n.acceptingPotentialNeighbors.Load() {
		n.logger.Debug("Stopping receiving peers, not adding new neighbor", zap.String("peer", addrInfo.ID.String()))
		return fmt.Errorf("stopping receiving peers")
	}

	if n.maxOutbound > 0 && len(n.neighbors) >= n.maxOutbound {
		n.logger.Info(
			"Neighbor capacity reached, rejecting new neighbor",
			zap.String("peer_id", addrInfo.ID.String()),
			zap.Int("current_neighbors", len(n.neighbors)),
			zap.Int("max_neighbors", n.maxOutbound),
		)
		return fmt.Errorf("neighbor capacity reached")
	}

	n.logger.Info(
		"Attempting to add neighbor",
		zap.String("peer_id", addrInfo.ID.String()),
		zap.Strings("addresses", addrsToStrings(addrInfo.Addrs)),
		zap.Int("current_neighbors", len(n.neighbors)),
		zap.Int("max_neighbors", n.maxOutbound),
	)

	if err := n.host.Connect(context.Background(), addrInfo); err != nil {
		n.logger.Error("Failed to connect to neighbor", zap.String("peer_id", addrInfo.ID.String()), zap.Error(err))
		return err
	}

	n.neighbors[addrInfo.ID] = addrInfo // Ensure the peer is stored in the map

	n.logger.Info(
		"Successfully added neighbor", zap.String("peer_id", addrInfo.ID.String()), zap.Int("total_neighbors", len(n.neighbors)),
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
	n.logger.Info(
		"Dropped neighbor", zap.String("peer_id", peerID.String()), zap.Int("remaining_neighbors", len(n.neighbors)),
	)
}

// StartBuildingNetwork initiates the network building phase by randomly selecting
// neighbors from the potential neighbors list and sending connection requests
func (n *P2PNode) buildNetwork() {
	n.logger.Info("Starting network building phase")

	closestPeers := n.discovery.ClosestPeers(n.host.ID(), n.maxOutbound)

	n.logger.Info(
		"Attempting to connect to closest neighbors", zap.Int("selected", len(closestPeers)),
	)

	for _, pi := range closestPeers {
		if n.maxOutbound > 0 && n.neighborCount() >= n.maxOutbound {
			break
		}
		peerAddr, ok := n.potentialNeighbors.Load(pi)
		if !ok {
			n.logger.Warn("Peer not found in potential neighbors", zap.String("peer_id", pi.String()))
			continue
		}
		if err := n.sendRequestToNeighbor(peerAddr.(peer.AddrInfo)); err != nil {
			n.logger.Error("Failed to send request to neighbor", zap.String("peer_id", pi.String()), zap.Error(err))
		}
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

func readStreamWithLimit(r io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("invalid read limit: %d", limit)
	}

	buf, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}

	if int64(len(buf)) > limit {
		return nil, fmt.Errorf("message exceeds max allowed size of %d bytes", limit)
	}

	return buf, nil
}
