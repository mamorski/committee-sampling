package network

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

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
	"github.com/mamorski/committee-sampling/internal/hash"
	"github.com/mamorski/committee-sampling/internal/network/discovery"
	"github.com/mamorski/committee-sampling/internal/sim/adversary"
	"github.com/mamorski/committee-sampling/pkg/config"
	pproto "github.com/mamorski/committee-sampling/pkg/proto"
)

const (
	addNotifier           = "/neighborhood/add/1.0.0"
	dropNotifier          = "/neighborhood/drop/1.0.0"
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

var ErrNeighborCapacity = errors.New("neighbor capacity reached")

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
	degreeSlack                 int
	key                         crypto.PrivKey
	acceptingPotentialNeighbors atomic.Bool
	sync                        common.Synchronizer
	mu                          sync.Mutex
	sid                         string
	// simulation options
	dropOnSend            bool
	dropOnSendProbability float64
	behaviors             []adversary.Behavior
	clockSkew             time.Duration
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
		degreeSlack:                 cfg.DegreeSlack,
		key:                         priv,
		acceptingPotentialNeighbors: atomic.Bool{},
		sync:                        synchronizer,
		sid:                         sid,
		dropOnSend:                  cfg.DropOnSend,
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

	if cfg.Adversary.Enabled {
		seedMaterial := hash.Sum([]byte(h.ID().String()))
		var derivedSeed int64
		if len(seedMaterial) >= 8 {
			derivedSeed = int64(binary.BigEndian.Uint64(seedMaterial[:8])) // #nosec G115: only used for deterministic simulations
		}
		baseSeed := cfg.Adversary.Seed
		if baseSeed == 0 {
			baseSeed = derivedSeed
		}
		dropProbability := cfg.Adversary.DropProbability
		if dropProbability < 0 {
			dropProbability = 0
		}
		if dropProbability > 1 {
			dropProbability = 1
		}
		jitterMin := cfg.Adversary.JitterMin
		jitterMax := cfg.Adversary.JitterMax
		if jitterMax < jitterMin {
			jitterMax = jitterMin
		}
		behavior := adversary.NewNetworkUnreliability(
			node.logger.Named("adversary"),
			baseSeed,
			adversary.NetworkUnreliabilityConfig{
				DropProbability: dropProbability,
				JitterMin:       jitterMin,
				JitterMax:       jitterMax,
			},
		)
		node.behaviors = append(node.behaviors, behavior)
		node.logger.Info(
			"Simulation: adversarial drop/jitter enabled",
			zap.Float64("drop_probability", dropProbability),
			zap.Duration("jitter_min", jitterMin),
			zap.Duration("jitter_max", jitterMax),
		)
	}

	if cfg.Adversary.Enabled && cfg.Adversary.ClockSkew != 0 {
		node.clockSkew = cfg.Adversary.ClockSkew
		node.logger.Info(
			"Simulation: clock skew enabled",
			zap.Duration("clock_skew", node.clockSkew),
		)
	}

	if cfg.Adversary.ExAnte.Equivocator {
		node.behaviors = append(node.behaviors, adversary.NewExAnteEquivocator(node.logger.Named("equivocator")))
		node.logger.Info("Simulation: ex-ante equivocator enabled")
	}

	if cfg.Adversary.ExPost.FreshnessCheater.Enabled {
		mode := adversary.ParseFreshnessMode(cfg.Adversary.ExPost.FreshnessCheater.Mode)
		fresh := cfg.Adversary.ExPost.FreshnessCheater
		behavior := adversary.NewFreshnessCheater(
			node.logger.Named("freshness_cheater"),
			mode,
			fresh.StaleRounds,
			fresh.TruncateLeaf,
		)
		node.behaviors = append(node.behaviors, behavior)
		node.logger.Info(
			"Simulation: ex-post freshness cheater enabled",
			zap.String("mode", string(mode)),
			zap.Int("stale_rounds", fresh.StaleRounds),
			zap.Bool("truncate_leaf", fresh.TruncateLeaf),
		)
	}

	node.acceptingPotentialNeighbors.Store(true)
	// Set stream handler
	h.SetStreamHandler(protocol.ID(addNotifier+"/"+sid), node.addNeighborNotifier)
	h.SetStreamHandler(protocol.ID(dropNotifier+"/"+sid), node.dropNeighborNotifier)

	// Start d
	if err := d.Start(c); err != nil {
		_ = node.Close()
		return nil, fmt.Errorf("failed to start d: %w", err)
	}
	go node.graphBuilder()

	return node, nil
}

func (n *P2PNode) Close() error {
	n.mu.Lock()
	neighborSnapshot := make([]peer.AddrInfo, 0, len(n.neighbors))
	for _, info := range n.neighbors {
		neighborSnapshot = append(neighborSnapshot, info)
	}
	n.mu.Unlock()

	for _, info := range neighborSnapshot {
		n.notifyNeighborDrop(info)
	}

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

		payloadCopy := append([]byte(nil), data...)
		m := &pproto.ProtocolMessage{
			Payload:     payloadCopy,
			MessageData: n.newMessageData(uuid.New().String(), false),
		}

		decision := adversary.SendNow()
		if len(n.behaviors) > 0 {
			envelope := &adversary.Envelope{
				ProtocolID: protocolID,
				From:       n.host.ID(),
				To:         addrInfo.ID,
				Message:    m,
			}
			for _, behavior := range n.behaviors {
				dec := behavior.Outbound(envelope)
				switch dec.Action {
				case adversary.ActionDrop:
					decision = dec
				case adversary.ActionDelay:
					if decision.Action != adversary.ActionDelay || dec.Delay > decision.Delay {
						decision = dec
					}
				default:
					// keep existing decision
				}
				if decision.Action == adversary.ActionDrop {
					break
				}
			}
		}

		if decision.Action == adversary.ActionDrop {
			n.logger.Warn(
				"Adversary: outbound message dropped",
				zap.String("peer_id", addrInfo.ID.String()),
				zap.String("protocol", protocolID),
			)
			continue
		}

		signature, err := n.signProtoMessage(m)
		if err != nil {
			n.logger.Error("Failed to sign message", zap.Error(err))
			continue
		}

		m.MessageData.Sign = signature

		switch decision.Action {
		case adversary.ActionDelay:
			n.delayedSend(addrInfo, protocol.ID(protocolID), m, decision.Delay)
		default:
			n.send(addrInfo, protocol.ID(protocolID), m)
		}
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

func (n *P2PNode) neighborLimit() int {
	capacity := n.maxOutbound
	switch {
	case capacity > 0 && n.degreeSlack > 0:
		return capacity + n.degreeSlack
	case capacity > 0:
		return capacity
	case n.degreeSlack > 0:
		return n.degreeSlack
	default:
		return 0
	}
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

			from := s.Conn().RemotePeer()
			dec := adversary.SendNow()
			if len(n.behaviors) > 0 {
				envelope := &adversary.Envelope{
					ProtocolID: protocolID,
					From:       from,
					To:         n.host.ID(),
					Message:    data,
				}
				for _, behavior := range n.behaviors {
					bDec := behavior.Inbound(envelope)
					switch bDec.Action {
					case adversary.ActionDrop:
						dec = bDec
					case adversary.ActionDelay:
						if dec.Action != adversary.ActionDelay || bDec.Delay > dec.Delay {
							dec = bDec
						}
					default:
						// keep existing decision
					}
					if dec.Action == adversary.ActionDrop {
						break
					}
				}
			}

			payload := append([]byte(nil), data.Payload...)
			deliver := func() {
				if err := handler(from, payload); err != nil {
					n.logger.Error("Failed to handle message", zap.Error(err))
				}
			}

			switch dec.Action {
			case adversary.ActionDrop:
				n.logger.Debug(
					"Adversary: inbound message dropped",
					zap.String("from", from.String()),
					zap.String("protocol", protocolID),
				)
				return
			case adversary.ActionDelay:
				go func(delay time.Duration) {
					timer := time.NewTimer(delay)
					defer timer.Stop()
					select {
					case <-timer.C:
						deliver()
					case <-n.ctx.Done():
						return
					}
				}(dec.Delay)
			default:
				deliver()
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

func (n *P2PNode) addNeighborNotifier(s network.Stream) {
	data := &pproto.NeighborMessage{}
	buf, err := readStreamWithLimit(s, int64(maxInboundMessageSize))
	if err != nil {
		n.logger.Error("Failed to read neighbor notification", zap.Error(err))
		_ = s.Reset()
		return
	}
	if err := s.Close(); err != nil {
		n.logger.Debug("Failed to close request stream", zap.Error(err))
	}

	if err := proto.Unmarshal(buf, data); err != nil {
		n.logger.Error("Failed to unmarshal neighbor notification", zap.Error(err))
		return
	}

	if !n.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate neighbor notification")
		return
	}

	addrInfo := peer.AddrInfo{
		ID:    s.Conn().RemotePeer(),
		Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
	}

	if err := n.addNeighbor(addrInfo); err != nil {
		n.logger.Info(
			"Unable to accept neighbor notification",
			zap.String("peer_id", addrInfo.ID.String()),
			zap.Error(err),
		)
		n.notifyNeighborDrop(addrInfo)
		return
	}

	n.logger.Info(
		"Neighbor notification accepted",
		zap.String("peer_id", addrInfo.ID.String()),
	)
}

func (n *P2PNode) dropNeighborNotifier(s network.Stream) {
	data := &pproto.NeighborMessageResponse{}
	buf, err := readStreamWithLimit(s, int64(maxInboundMessageSize))
	if err != nil {
		n.logger.Error("Failed to read neighbor update", zap.Error(err))
		_ = s.Reset()
		return
	}
	if err := s.Close(); err != nil {
		n.logger.Debug("Failed to close response stream", zap.Error(err))
	}

	if err := proto.Unmarshal(buf, data); err != nil {
		n.logger.Error("Failed to unmarshal neighbor update", zap.Error(err))
		return
	}

	if !n.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate neighbor update")
		return
	}

	peerID := s.Conn().RemotePeer()
	if data.Success {
		n.logger.Debug("Ignoring unexpected success response", zap.String("peer", peerID.String()))
		return
	}

	n.dropNeighbor(peerID)
	n.logger.Info("Neighbor dropped via notification", zap.String("peer", peerID.String()))

}

func (n *P2PNode) notifyNeighborAdd(info peer.AddrInfo) error {
	currentNeighbors := n.neighborCount()

	n.logger.Info(
		"Announcing neighbor",
		zap.String("peer_id", info.ID.String()),
		zap.Strings("addresses", addrsToStrings(info.Addrs)),
		zap.Int("current_neighbors", currentNeighbors),
	)

	msg := &pproto.NeighborMessage{
		MessageData: n.newMessageData(uuid.New().String(), false),
	}

	signature, err := n.signProtoMessage(msg)
	if err != nil {
		n.logger.Error("Failed to sign neighbor announcement", zap.Error(err))
		return errors.New("failed to sign neighbor announcement")
	}

	msg.MessageData.Sign = signature
	if ok := n.send(info, protocol.ID(addNotifier+"/"+n.sid), msg); !ok {
		n.logger.Error("Failed to send neighbor announcement", zap.String("peer", info.ID.String()))
		return errors.New("failed to send neighbor announcement")
	}

	n.logger.Debug("Neighbor announcement sent", zap.String("peer", info.ID.String()))
	return nil
}

func (n *P2PNode) notifyNeighborDrop(info peer.AddrInfo) {
	msg := &pproto.NeighborMessageResponse{
		MessageData: n.newMessageData(uuid.New().String(), false),
		Success:     false,
	}

	signature, err := n.signProtoMessage(msg)
	if err != nil {
		n.logger.Error("Failed to sign neighbor drop notification", zap.Error(err))
		return
	}

	msg.MessageData.Sign = signature
	if ok := n.send(info, protocol.ID(dropNotifier+"/"+n.sid), msg); !ok {
		n.logger.Warn("Failed to send neighbor drop notification", zap.String("peer", info.ID.String()))
	}
}

func (n *P2PNode) addNeighbor(addrInfo peer.AddrInfo) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	// Check if already connected
	if _, ok := n.neighbors[addrInfo.ID]; ok {
		n.logger.Debug("Neighbor already exists", zap.String("peer", addrInfo.ID.String()))
		return nil
	}

	if limit := n.neighborLimit(); limit > 0 && len(n.neighbors) >= limit {
		n.logger.Info(
			"Neighbor capacity reached",
			zap.String("peer_id", addrInfo.ID.String()),
			zap.Int("current_neighbors", len(n.neighbors)),
			zap.Int("limit", limit),
		)
		return ErrNeighborCapacity
	}

	n.logger.Info(
		"Attempting to add neighbor",
		zap.String("peer_id", addrInfo.ID.String()),
		zap.Strings("addresses", addrsToStrings(addrInfo.Addrs)),
		zap.Int("current_neighbors", len(n.neighbors)),
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

	candidates := n.collectPotentialNeighbors()
	if len(candidates) == 0 {
		n.logger.Warn("No potential neighbors available for network building")
		return
	}

	selected := n.selectRandomNeighbors(candidates)

	n.logger.Info(
		"Attempting to connect to random neighbors",
		zap.Int("candidates", len(candidates)),
		zap.Int("selected", len(selected)),
	)

	connected := 0
	for _, info := range selected {
		if n.maxOutbound > 0 && connected >= n.maxOutbound {
			break
		}

		if n.IsNeighbor(info.ID) {
			continue
		}

		if err := n.addNeighbor(info); err != nil {
			if errors.Is(err, ErrNeighborCapacity) {
				break
			}
			n.logger.Error("Failed to add neighbor before announcement", zap.String("peer_id", info.ID.String()), zap.Error(err))
			continue
		}

		n.logger.Info("Notifying neighbor", zap.String("peer_id", info.ID.String()))
		if err := n.notifyNeighborAdd(info); err != nil {
			n.logger.Error("Failed to announce neighbor", zap.String("peer_id", info.ID.String()), zap.Error(err))
			n.dropNeighbor(info.ID)
		} else {
			connected++
		}
		time.Sleep(200 * time.Millisecond) // brief pause to avoid overwhelming the network
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

func (n *P2PNode) collectPotentialNeighbors() []peer.AddrInfo {
	candidates := make([]peer.AddrInfo, 0)
	selfID := n.host.ID()

	n.potentialNeighbors.Range(
		func(_ any, value any) bool {
			info, ok := value.(peer.AddrInfo)
			if !ok {
				return true
			}
			if info.ID == selfID || n.IsNeighbor(info.ID) {
				return true
			}
			candidates = append(candidates, info)
			return true
		},
	)

	return candidates
}

func (n *P2PNode) selectRandomNeighbors(candidates []peer.AddrInfo) []peer.AddrInfo {
	if len(candidates) == 0 {
		return nil
	}

	shuffled := make([]peer.AddrInfo, len(candidates))
	copy(shuffled, candidates)

	for i := len(shuffled) - 1; i > 0; i-- {
		j, err := rand.Int(rand.Reader, big.NewInt(int64(i+1)))
		if err != nil {
			n.logger.Error("Failed to shuffle neighbors securely", zap.Error(err))
			return shuffled
		}
		idx := int(j.Int64())
		shuffled[i], shuffled[idx] = shuffled[idx], shuffled[i]
	}

	return shuffled
}
