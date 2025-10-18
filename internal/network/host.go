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
	graphProposal         = "/graph/proposal/1.0.0"
	graphResponse         = "/graph/response/1.0.0"
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

type connectionState struct {
	proposalsSent    map[peer.ID]int
	pendingResponses map[peer.ID]time.Time
	rejectedPeers    map[peer.ID]bool
	mu               sync.Mutex
}

type P2PNode struct {
	host                        Host
	ctx                         context.Context
	cancel                      context.CancelFunc
	logger                      *zap.Logger
	discovery                   discovery.PeerDiscovery
	sync                        common.Synchronizer
	mu                          sync.Mutex
	key                         crypto.PrivKey
	neighbors                   map[peer.ID]peer.AddrInfo
	outboundNeighbors           map[peer.ID]bool
	potentialNeighbors          sync.Map
	connState                   *connectionState
	behaviors                   []adversary.Behavior
	sid                         string
	maxOutbound                 int
	degreeSlack                 int
	buildingRounds              int
	roundTimeout                time.Duration
	dropOnSendProbability       float64
	clockSkew                   time.Duration
	acceptingPotentialNeighbors atomic.Bool
	dropOnSend                  bool
}

func New(ctx context.Context, cfg *config.Config, logger *zap.Logger, synchronizer common.Synchronizer, sid string) (*P2PNode, error) {
	c, cancel := context.WithCancel(ctx)

	// Generate private key
	priv, _, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	// Create multiaddress for listening
	listenAddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.Network.ListenPort))
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
	d := discovery.NewDHTDiscovery(h, cfg.Network.DiscoveryConfig, logger)
	logger = logger.With(zap.String("node_id", h.ID().String()))

	node := &P2PNode{
		host:                        h,
		ctx:                         c,
		cancel:                      cancel,
		maxOutbound:                 cfg.Network.MaxOutboundDegree,
		discovery:                   d,
		logger:                      logger.Named("network"),
		neighbors:                   make(map[peer.ID]peer.AddrInfo),
		outboundNeighbors:           make(map[peer.ID]bool),
		degreeSlack:                 cfg.Network.DegreeSlack,
		key:                         priv,
		acceptingPotentialNeighbors: atomic.Bool{},
		sync:                        synchronizer,
		sid:                         sid,
		buildingRounds:              cfg.Graph.BuildingRounds,
		roundTimeout:                cfg.Synchronization.GraphBuildingRoundTimeout,
		dropOnSend:                  cfg.Network.DropOnSend,
		connState: &connectionState{
			proposalsSent:    make(map[peer.ID]int),
			pendingResponses: make(map[peer.ID]time.Time),
			rejectedPeers:    make(map[peer.ID]bool),
		},
		dropOnSendProbability: func() float64 {
			p := cfg.Network.DropOnSendProbability
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

	if cfg.Network.Adversary.Enabled {
		seedMaterial := hash.Sum([]byte(h.ID().String()))
		var derivedSeed int64
		if len(seedMaterial) >= 8 {
			derivedSeed = int64(binary.BigEndian.Uint64(seedMaterial[:8])) // #nosec G115: only used for deterministic simulations
		}
		baseSeed := cfg.Network.Adversary.Seed
		if baseSeed == 0 {
			baseSeed = derivedSeed
		}
		dropProbability := cfg.Network.Adversary.DropProbability
		if dropProbability < 0 {
			dropProbability = 0
		}
		if dropProbability > 1 {
			dropProbability = 1
		}
		jitterMin := cfg.Network.Adversary.JitterMin
		jitterMax := cfg.Network.Adversary.JitterMax
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

	if cfg.Network.Adversary.Enabled && cfg.Network.Adversary.ClockSkew != 0 {
		node.clockSkew = cfg.Network.Adversary.ClockSkew
		node.logger.Info(
			"Simulation: clock skew enabled",
			zap.Duration("clock_skew", node.clockSkew),
		)
	}

	if cfg.Network.Adversary.ExAnte.Equivocator {
		node.behaviors = append(node.behaviors, adversary.NewExAnteEquivocator(node.logger.Named("equivocator")))
		node.logger.Info("Simulation: ex-ante equivocator enabled")
	}

	if cfg.Network.Adversary.ExPost.FreshnessCheater.Enabled {
		mode := adversary.ParseFreshnessMode(cfg.Network.Adversary.ExPost.FreshnessCheater.Mode)
		fresh := cfg.Network.Adversary.ExPost.FreshnessCheater
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
	h.SetStreamHandler(protocol.ID(graphProposal+"/"+sid), node.graphProposalHandler)
	h.SetStreamHandler(protocol.ID(graphResponse+"/"+sid), node.graphResponseHandler)

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

	discoveryRound, err := n.sync.WaitForRound(common.Network, 0)
	if err != nil {
		n.logger.Error("Failed to wait for Network discovery round", zap.Error(err))
		panic(err)
	}

	<-discoveryRound
	n.acceptingPotentialNeighbors.Store(false)
	n.logger.Info("Discovery phase complete, starting multi-round graph building")

	for round := 0; round < n.buildingRounds; round++ {
		roundStart, err := n.sync.WaitForRound(common.Network, round+1)
		if err != nil {
			n.logger.Error("Failed to wait for graph building round", zap.Int("round", round), zap.Error(err))
			panic(err)
		}

		<-roundStart
		n.logger.Info("Starting graph building round", zap.Int("round", round))

		n.cleanupTimedOutProposals()

		if !n.hasOutboundCapacity() {
			n.logger.Info("Outbound capacity filled, skipping proposal phase", zap.Int("round", round))
			continue
		}

		if !n.hasNeighborCapacity() {
			n.logger.Info("Total neighbor capacity filled, skipping proposal phase", zap.Int("round", round))
			continue
		}

		n.sendProposals(round)
	}

	n.logger.Info("Network building phase completed")
	neighbors := n.GetNeighbors()
	n.logger.Info(
		"Final neighbor list", zap.Int("count", len(neighbors)), zap.Strings("neighbors", neighbors),
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
	delete(n.outboundNeighbors, peerID)
	n.logger.Info(
		"Dropped neighbor", zap.String("peer_id", peerID.String()), zap.Int("remaining_neighbors", len(n.neighbors)),
	)
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

func (n *P2PNode) graphProposalHandler(s network.Stream) {
	data := &pproto.GraphProposal{}
	buf, err := readStreamWithLimit(s, int64(maxInboundMessageSize))
	if err != nil {
		n.logger.Error("Failed to read graph proposal", zap.Error(err))
		_ = s.Reset()
		return
	}
	if err := s.Close(); err != nil {
		n.logger.Debug("Failed to close stream", zap.Error(err))
	}

	if err := proto.Unmarshal(buf, data); err != nil {
		n.logger.Error("Failed to unmarshal graph proposal", zap.Error(err))
		return
	}

	if !n.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate graph proposal")
		return
	}

	proposerID := s.Conn().RemotePeer()
	accepted := false

	if n.hasInboundCapacity() && !n.IsNeighbor(proposerID) {
		addrInfo := peer.AddrInfo{
			ID:    proposerID,
			Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
		}

		if err := n.addNeighbor(addrInfo); err == nil {
			accepted = true
			n.logger.Info("Accepted graph proposal", zap.String("from", proposerID.String()), zap.Int32("round", data.Round))
		} else {
			n.logger.Info("Failed to add neighbor from proposal", zap.String("from", proposerID.String()), zap.Error(err))
		}
	}

	n.sendGraphResponse(proposerID, data.Round, accepted)
}

func (n *P2PNode) graphResponseHandler(s network.Stream) {
	data := &pproto.GraphResponse{}
	buf, err := readStreamWithLimit(s, int64(maxInboundMessageSize))
	if err != nil {
		n.logger.Error("Failed to read graph response", zap.Error(err))
		_ = s.Reset()
		return
	}
	if err := s.Close(); err != nil {
		n.logger.Debug("Failed to close stream", zap.Error(err))
	}

	if err := proto.Unmarshal(buf, data); err != nil {
		n.logger.Error("Failed to unmarshal graph response", zap.Error(err))
		return
	}

	if !n.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate graph response")
		return
	}

	responderID := s.Conn().RemotePeer()

	n.connState.mu.Lock()
	delete(n.connState.pendingResponses, responderID)
	n.connState.mu.Unlock()

	if data.Accepted {
		_, alreadyNeighbor := n.getNeighbor(responderID)
		if !alreadyNeighbor {
			addrInfo := peer.AddrInfo{
				ID:    responderID,
				Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
			}
			if err := n.addNeighbor(addrInfo); err != nil {
				if errors.Is(err, ErrNeighborCapacity) {
					n.logger.Warn("Cannot accept proposal response due to full capacity, asymmetric edge created",
						zap.String("from", responderID.String()),
						zap.Int("current_neighbors", n.neighborCount()),
						zap.Int("limit", n.neighborLimit()))
				} else {
					n.logger.Error("Failed to add neighbor from accepted response", zap.String("from", responderID.String()), zap.Error(err))
				}
				n.connState.mu.Lock()
				n.connState.rejectedPeers[responderID] = true
				n.connState.mu.Unlock()
				return
			}
		}

		n.mu.Lock()
		n.outboundNeighbors[responderID] = true
		n.mu.Unlock()

		n.logger.Info("Graph proposal accepted", zap.String("peer", responderID.String()), zap.Int32("round", data.Round))
	} else {
		n.connState.mu.Lock()
		n.connState.rejectedPeers[responderID] = true
		n.connState.mu.Unlock()
		n.logger.Debug("Graph proposal rejected", zap.String("peer", responderID.String()), zap.Int32("round", data.Round))
	}
}

func (n *P2PNode) sendProposals(round int) {
	remaining := n.remainingOutboundCapacity()
	if remaining <= 0 {
		return
	}

	candidates := n.selectProposalCandidates()
	if len(candidates) == 0 {
		n.logger.Debug("No candidates available for proposals", zap.Int("round", round))
		return
	}

	shuffled := n.selectRandomNeighbors(candidates)
	sent := 0

	for _, candidate := range shuffled {
		if sent >= remaining {
			break
		}

		msg := &pproto.GraphProposal{
			MessageData: n.newMessageData(uuid.New().String(), false),
			Round:       int32(round),
		}

		signature, err := n.signProtoMessage(msg)
		if err != nil {
			n.logger.Error("Failed to sign graph proposal", zap.Error(err))
			continue
		}

		msg.MessageData.Sign = signature

		if ok := n.send(candidate, protocol.ID(graphProposal+"/"+n.sid), msg); ok {
			n.connState.mu.Lock()
			n.connState.proposalsSent[candidate.ID] = round
			n.connState.pendingResponses[candidate.ID] = time.Now().Add(n.roundTimeout)
			n.connState.mu.Unlock()

			sent++
			n.logger.Debug("Sent graph proposal", zap.String("to", candidate.ID.String()), zap.Int("round", round))
		}
	}

	n.logger.Info("Sent graph proposals", zap.Int("count", sent), zap.Int("round", round))
}

func (n *P2PNode) selectProposalCandidates() []peer.AddrInfo {
	candidates := make([]peer.AddrInfo, 0)
	selfID := n.host.ID()

	n.connState.mu.Lock()
	rejected := make(map[peer.ID]bool)
	for id := range n.connState.rejectedPeers {
		rejected[id] = true
	}
	pending := make(map[peer.ID]bool)
	for id := range n.connState.pendingResponses {
		pending[id] = true
	}
	n.connState.mu.Unlock()

	n.potentialNeighbors.Range(func(_ any, value any) bool {
		info, ok := value.(peer.AddrInfo)
		if !ok {
			return true
		}
		if info.ID == selfID || n.IsNeighbor(info.ID) || rejected[info.ID] || pending[info.ID] {
			return true
		}
		candidates = append(candidates, info)
		return true
	})

	return candidates
}

func (n *P2PNode) sendGraphResponse(peerID peer.ID, round int32, accepted bool) {
	addrInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: n.host.Peerstore().Addrs(peerID),
	}

	msg := &pproto.GraphResponse{
		MessageData: n.newMessageData(uuid.New().String(), false),
		Round:       round,
		Accepted:    accepted,
	}

	signature, err := n.signProtoMessage(msg)
	if err != nil {
		n.logger.Error("Failed to sign graph response", zap.Error(err))
		return
	}

	msg.MessageData.Sign = signature

	if ok := n.send(addrInfo, protocol.ID(graphResponse+"/"+n.sid), msg); !ok {
		n.logger.Warn("Failed to send graph response", zap.String("to", peerID.String()))
	} else {
		n.logger.Debug("Sent graph response", zap.String("to", peerID.String()), zap.Bool("accepted", accepted))
	}
}

func (n *P2PNode) cleanupTimedOutProposals() {
	n.connState.mu.Lock()
	defer n.connState.mu.Unlock()

	now := time.Now()
	for peerID, timeout := range n.connState.pendingResponses {
		if now.After(timeout) {
			delete(n.connState.pendingResponses, peerID)
			n.connState.rejectedPeers[peerID] = true
			n.logger.Debug("Proposal timed out", zap.String("peer", peerID.String()))
		}
	}
}
