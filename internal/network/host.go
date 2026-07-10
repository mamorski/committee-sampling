package network

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/netip"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	lp2pmetrics "github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-msgio"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/mamorski/committee-sampling/internal/common"
	"github.com/mamorski/committee-sampling/internal/network/discovery"
	"github.com/mamorski/committee-sampling/pkg/config"
	pproto "github.com/mamorski/committee-sampling/pkg/proto"
)

const (
	graphProposal         = "/graph/proposal/1.0.0"
	graphDrop             = "/graph/drop/1.0.0"
	peerDrop              = "/peer/drop/1.0.0"
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

type queuedProposal struct {
	from     peer.ID
	addrInfo peer.AddrInfo
}

type queuedDrop struct {
	from peer.ID
}

type algoPhase struct {
	step    common.Step
	timeout time.Duration
}

// ByteStats holds cumulative in/out byte counts for a protocol.
type ByteStats struct {
	In  int64
	Out int64
}

// AppByteStats holds cumulative app-layer byte counts for a protocol: Payload* is
// the protocol's own message size (the bytes handed to/from the handler), Env* the
// marshaled ProtocolMessage (payload + signature + MessageData). Counted at the
// application boundary, parallel to the libp2p wire bytes from BandwidthCounter.
type AppByteStats struct {
	PayloadIn, PayloadOut int64
	EnvIn, EnvOut         int64
}

// appByteCounts is the per-protocol accumulator. Counts use atomics so the send
// fan-out and concurrent inbound stream handlers need no shared lock on the hot path.
type appByteCounts struct {
	payloadIn, payloadOut atomic.Int64
	envIn, envOut         atomic.Int64
}

// appByteTracker accumulates app-layer byte counts keyed by protocol ID. The mutex
// guards only first-seen map insertion; per-counter updates are lock-free atomics.
type appByteTracker struct {
	mu sync.RWMutex
	m  map[string]*appByteCounts
}

func newAppByteTracker() *appByteTracker {
	return &appByteTracker{m: make(map[string]*appByteCounts)}
}

func (t *appByteTracker) counts(pid string) *appByteCounts {
	t.mu.RLock()
	c := t.m[pid]
	t.mu.RUnlock()
	if c != nil {
		return c
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if c = t.m[pid]; c == nil {
		c = &appByteCounts{}
		t.m[pid] = c
	}
	return c
}

func (t *appByteTracker) addOut(pid string, payload, envelope int64) {
	c := t.counts(pid)
	c.payloadOut.Add(payload)
	c.envOut.Add(envelope)
}

func (t *appByteTracker) addIn(pid string, payload, envelope int64) {
	c := t.counts(pid)
	c.payloadIn.Add(payload)
	c.envIn.Add(envelope)
}

// snapshot returns the cumulative app-layer counts keyed by protocol ID.
func (t *appByteTracker) snapshot() map[string]AppByteStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]AppByteStats, len(t.m))
	for pid, c := range t.m {
		out[pid] = AppByteStats{
			PayloadIn:  c.payloadIn.Load(),
			PayloadOut: c.payloadOut.Load(),
			EnvIn:      c.envIn.Load(),
			EnvOut:     c.envOut.Load(),
		}
	}
	return out
}

type P2PNode struct {
	host                        Host
	ctx                         context.Context
	cancel                      context.CancelFunc
	logger                      *zap.Logger
	discovery                   discovery.PeerDiscovery
	sync                        common.Synchronizer
	mu                          sync.RWMutex
	key                         crypto.PrivKey
	bwc                         *lp2pmetrics.BandwidthCounter
	appBytes                    *appByteTracker
	bwWG                        sync.WaitGroup
	nodeID                      string
	nodePubKey                  []byte
	neighbors                   map[peer.ID]peer.AddrInfo
	potentialNeighbors          sync.Map
	sid                         string
	maxOutbound                 int
	sendTimeout                 time.Duration
	buildingRounds              int
	dropOnSendProbability       float64
	acceptingPotentialNeighbors atomic.Bool
	dropOnSend                  bool
	peerDropEnabled             bool
	peerDropPhases              []algoPhase
	peerDropRounds              int
	proposalQueue               []queuedProposal
	dropQueue                   []queuedDrop
	queueMu                     sync.Mutex
	acceptingGraphMessages      atomic.Bool
	shuffledPotentialNeighbors  []peer.AddrInfo
	selectedNeighborTarget      int
	sentProposalsTo             map[peer.ID]bool
	streamPool                  map[streamKey]*pooledStream
	poolMu                      sync.Mutex
	dialTimeout                 time.Duration
	connectFails                sync.Map // peer.ID -> *atomic.Int64: consecutive failed dials
	connectFailsMax             int64    // evict a neighbor once its streak reaches this
}

// streamKey identifies a reusable outbound stream by peer and protocol.
type streamKey struct {
	peer  peer.ID
	proto protocol.ID
}

// pooledStream is a long-lived outbound stream with length-prefixed framing.
// s is nil until first open and after lazy eviction on write error.
type pooledStream struct {
	mu sync.Mutex // serialize writes; libp2p stream not concurrent-write safe
	s  network.Stream
	w  msgio.Writer
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

	bwc := lp2pmetrics.NewBandwidthCounter()

	opts := []libp2p.Option{
		libp2p.ListenAddrs(listenAddr), libp2p.Identity(priv),
		libp2p.BandwidthReporter(bwc),
	}
	// Co-located simulations open many concurrent streams; the default resource
	// manager rejects them ("Failed to open stream"). Disable the limits when asked.
	if cfg.Network.UnlimitedResources {
		// InfiniteLimits only relaxes the resource Limiter. The connLimiter is a
		// separate cap that defaults to 8 conns per IPv4 /32 (loopback excepted),
		// so co-located nodes sharing one non-loopback IP hit "connections per ip
		// limit exceeded". Lift the per-subnet and network-prefix caps too.
		unlimitedV4 := []rcmgr.ConnLimitPerSubnet{{PrefixLength: 0, ConnCount: math.MaxInt}}
		unlimitedV6 := []rcmgr.ConnLimitPerSubnet{{PrefixLength: 0, ConnCount: math.MaxInt}}
		unlimitedPrefixV4 := []rcmgr.NetworkPrefixLimit{{Network: netip.MustParsePrefix("0.0.0.0/0"), ConnCount: math.MaxInt}}
		unlimitedPrefixV6 := []rcmgr.NetworkPrefixLimit{{Network: netip.MustParsePrefix("::/0"), ConnCount: math.MaxInt}}
		rm, err := rcmgr.NewResourceManager(
			rcmgr.NewFixedLimiter(rcmgr.InfiniteLimits),
			rcmgr.WithLimitPerSubnet(unlimitedV4, unlimitedV6),
			rcmgr.WithNetworkPrefixLimit(unlimitedPrefixV4, unlimitedPrefixV6),
		)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create resource manager: %w", err)
		}
		opts = append(opts, libp2p.ResourceManager(rm))
		logger.Info("Simulation: libp2p resource manager limits disabled")
	}

	// Create libp2p h
	h, err := libp2p.New(opts...)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create h: %w", err)
	}

	// Create d service
	d := discovery.NewDHTDiscovery(h, cfg.Network.DiscoveryConfig, logger)
	logger = logger.With(zap.String("node_id", h.ID().String()))

	// Marshal own public key once; it is constant and attached to every outbound message.
	nodePubKey, err := crypto.MarshalPublicKey(priv.GetPublic())
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}

	node := &P2PNode{
		host:                        h,
		ctx:                         c,
		cancel:                      cancel,
		maxOutbound:                 cfg.Network.MaxOutboundDegree,
		sendTimeout: func() time.Duration {
			if cfg.Network.SendTimeout > 0 {
				return cfg.Network.SendTimeout
			}
			return 5 * time.Second
		}(),
		dialTimeout: func() time.Duration {
			if cfg.Network.DialTimeout > 0 {
				return cfg.Network.DialTimeout
			}
			return 1 * time.Second
		}(),
		connectFailsMax: func() int64 {
			if cfg.Network.ConnectivityRetries > 0 {
				return int64(cfg.Network.ConnectivityRetries)
			}
			return 3
		}(),
		discovery:                   d,
		logger:                      logger.Named("network"),
		neighbors:                   make(map[peer.ID]peer.AddrInfo),
		key:                         priv,
		bwc:                         bwc,
		appBytes:                    newAppByteTracker(),
		nodeID:                      h.ID().String(),
		nodePubKey:                  nodePubKey,
		acceptingPotentialNeighbors: atomic.Bool{},
		sync:                        synchronizer,
		sid:                         sid,
		buildingRounds:              cfg.Graph.BuildingRounds,
		dropOnSend:                  cfg.Network.DropOnSend,
		peerDropEnabled:             cfg.Network.PeerDropEnabled,
		peerDropRounds:              cfg.Graph.Diameter * cfg.Graph.GradingLevels,
		peerDropPhases: []algoPhase{
			{common.ExPostMDAG, cfg.Synchronization.MDAGRoundTimeout},
			{common.ExAnteMDAG, cfg.Synchronization.MDAGRoundTimeout},
			{common.ExPostVerify, cfg.Synchronization.ExPostRoundTimeout},
			{common.ExAnteVerify, cfg.Synchronization.ExAnteRoundTimeout},
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
		proposalQueue:          make([]queuedProposal, 0),
		dropQueue:              make([]queuedDrop, 0),
		acceptingGraphMessages: atomic.Bool{},
		sentProposalsTo:        make(map[peer.ID]bool),
		streamPool:             make(map[streamKey]*pooledStream),
	}

	if node.dropOnSend {
		node.logger.Info(
			"Simulation: drop-on-send enabled", zap.Float64("probability", node.dropOnSendProbability),
		)
	}
	if node.peerDropEnabled {
		node.logger.Info(
			"Simulation: peer-drop enabled",
			zap.Int("phases", len(node.peerDropPhases)),
			zap.Int("rounds_per_phase", node.peerDropRounds+1),
		)
	}

	node.acceptingPotentialNeighbors.Store(true)
	node.acceptingGraphMessages.Store(true)
	h.SetStreamHandler(protocol.ID(graphProposal+"/"+sid), node.graphProposalHandler)
	h.SetStreamHandler(protocol.ID(graphDrop+"/"+sid), node.graphDropHandler)
	h.SetStreamHandler(protocol.ID(peerDrop+"/"+sid), node.peerDropHandler)

	// Start d
	if err := d.Start(c); err != nil {
		_ = node.Close()
		return nil, fmt.Errorf("failed to start d: %w", err)
	}

	go node.graphBuilder()
	// Synchronous: registers all bwWG.Add before New returns, so a fast Close
	// cannot Wait() on a zero counter while watcher goroutines spawn later.
	node.startByteWatcher()

	return node, nil
}

func (n *P2PNode) Close() error {
	n.logGraphSnapshot("end")
	n.cancel()
	n.bwWG.Wait()
	// Reset any pooled outbound streams before tearing down the host.
	n.poolMu.Lock()
	for key, ps := range n.streamPool {
		ps.mu.Lock()
		if ps.s != nil {
			_ = ps.s.Reset()
			ps.s = nil
			ps.w = nil
		}
		ps.mu.Unlock()
		delete(n.streamPool, key)
	}
	n.poolMu.Unlock()
	// Log the final byte breakdown after the byte watchers have stopped.
	n.recordFinalBytes()
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
	n.mu.RLock()
	snapshot := make([]peer.AddrInfo, 0, len(n.neighbors))
	for _, ai := range n.neighbors {
		snapshot = append(snapshot, ai)
	}
	n.mu.RUnlock()

	// Fan out to neighbors concurrently: each send opens/reuses its own per-peer
	// stream, so a slow peer no longer serializes the whole round.
	var wg sync.WaitGroup
	for _, addrInfo := range snapshot {
		// simulation: optional probabilistic drop per recipient using crypto-secure RNG (avoid G404)
		if n.dropOnSend {
			// draw a uniform float in [0,1) using 53 random bits (float64 mantissa)
			f, err := cryptoFloat64()
			if err != nil {
				n.logger.Warn("Simulation: crypto RNG failed; skipping drop decision", zap.Error(err))
			} else if f < n.dropOnSendProbability {
				n.logger.Warn(
					"Simulation: dropped outgoing message",
					zap.String("peer_id", addrInfo.ID.String()),
					zap.Float64("probability", n.dropOnSendProbability),
					zap.String("protocol", protocolID),
					zap.Float64("random_float", f),
				)
				continue
			}
		}

		wg.Add(1)
		go func(addrInfo peer.AddrInfo) {
			defer wg.Done()

			payloadCopy := append([]byte(nil), data...)
			m := &pproto.ProtocolMessage{
				Payload:     payloadCopy,
				MessageData: n.newMessageData(),
			}

			signature, err := n.signProtoMessage(m)
			if err != nil {
				n.logger.Error("Failed to sign message", zap.Error(err))
				return
			}

			m.MessageData.Sign = signature
			// envelope = full marshaled ProtocolMessage (payload + signature + metadata);
			// count both only on a successful send to align with the wire counter;
			// envSize is the marshaled envelope length returned by send (no re-marshal).
			if envSize, ok := n.sendFramed(addrInfo, protocol.ID(protocolID), m); ok {
				n.appBytes.addOut(protocolID, int64(len(data)), envSize)
			}
		}(addrInfo)
	}
	wg.Wait()
}

func (n *P2PNode) GetNeighbors() []string {
	neighbors := make([]string, 0, len(n.neighbors))
	n.mu.RLock()
	defer n.mu.RUnlock()
	for id := range n.neighbors {
		neighbors = append(neighbors, id.String())
	}

	n.logger.Debug("Current neighbors", zap.Int("count", len(neighbors)), zap.Strings("neighbors", neighbors))

	return neighbors
}

func (n *P2PNode) IsNeighbor(peerID peer.ID) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	_, exists := n.neighbors[peerID]
	return exists
}

func (n *P2PNode) RegisterHandler(protocolID string, handler MessageHandler) {
	n.host.SetStreamHandler(
		protocol.ID(protocolID), func(s network.Stream) {
			// The sender keeps this stream open and writes successive
			// length-prefixed frames (see sendFramed); read them in a loop
			// until the peer resets the stream or goes away.
			rd := msgio.NewVarintReaderSize(s, maxInboundMessageSize)
			defer rd.Close()
			for {
				buf, err := rd.ReadMsg()
				if err != nil {
					if err != io.EOF {
						n.logger.Debug("Stream read ended", zap.Error(err))
					}
					_ = s.Reset()
					return
				}

				data := &pproto.ProtocolMessage{}
				if err := proto.Unmarshal(buf, data); err != nil {
					n.logger.Error("Failed to unmarshal EX ANTE message", zap.Error(err))
					rd.ReleaseMsg(buf)
					continue
				}

				if !n.authenticateMessage(data, data.MessageData) {
					n.logger.Error("Failed to authenticate message")
					rd.ReleaseMsg(buf)
					continue
				}

				from := s.Conn().RemotePeer()
				// payload = the protocol's own message; envelope = full marshaled
				// ProtocolMessage read off the stream (buf).
				n.appBytes.addIn(string(s.Protocol()), int64(len(data.Payload)), int64(len(buf)))
				payload := append([]byte(nil), data.Payload...)
				rd.ReleaseMsg(buf)
				if err := handler(from, payload); err != nil {
					n.logger.Error("Failed to handle message", zap.Error(err))
				}
			}
		},
	)
}

func (n *P2PNode) GetNodeID() string {
	return n.host.ID().String()
}

// bytesByProtocol returns cumulative in/out bytes keyed by protocol-ID string.
func (n *P2PNode) bytesByProtocol() map[string]ByteStats {
	raw := n.bwc.GetBandwidthByProtocol()
	out := make(map[string]ByteStats, len(raw))
	for pid, s := range raw {
		out[string(pid)] = ByteStats{In: s.TotalIn, Out: s.TotalOut}
	}
	return out
}

// bytesTotal returns cumulative in/out bytes across all protocols.
func (n *P2PNode) bytesTotal() (in, out int64) {
	s := n.bwc.GetBandwidthTotals()
	return s.TotalIn, s.TotalOut
}

// appBytesByProtocol returns cumulative app-layer payload/envelope bytes keyed by
// protocol-ID string (same key space as bytesByProtocol's wire counts).
func (n *P2PNode) appBytesByProtocol() map[string]AppByteStats {
	return n.appBytes.snapshot()
}

func generateSelectedTarget(limit int) (int, error) {
	if limit <= 0 {
		return 0, fmt.Errorf("limit must be positive")
	}
	minimum := limit * 2
	maximum := limit * 3
	rangeVal := maximum - minimum + 1

	bigRange := big.NewInt(int64(rangeVal))
	n, err := rand.Int(rand.Reader, bigRange)
	if err != nil {
		return 0, err
	}

	return minimum + int(n.Int64()), nil
}

func (n *P2PNode) processDropQueue() {
	n.queueMu.Lock()
	drops := n.dropQueue
	n.dropQueue = make([]queuedDrop, 0)
	n.queueMu.Unlock()

	for _, drop := range drops {
		n.dropNeighbor(drop.from)
		n.logger.Info("Processed drop message, removed neighbor", zap.String("from", drop.from.String()))
	}
}

func (n *P2PNode) processProposalQueue() {
	n.queueMu.Lock()
	proposals := n.proposalQueue
	n.proposalQueue = make([]queuedProposal, 0)
	n.queueMu.Unlock()

	for _, proposal := range proposals {
		if n.IsNeighbor(proposal.from) {
			n.logger.Debug("Proposal from existing neighbor, skipping", zap.String("from", proposal.from.String()))
			continue
		}

		n.mu.RLock()
		currentCount := len(n.neighbors)
		n.mu.RUnlock()

		if currentCount < n.selectedNeighborTarget {
			if err := n.addNeighbor(proposal.addrInfo); err == nil {
				n.logger.Info("Accepted proposal from queue", zap.String("from", proposal.from.String()))
			} else {
				n.sendDropMessage(proposal.from)
				n.logger.Debug("Failed to add neighbor from proposal", zap.String("from", proposal.from.String()), zap.Error(err))
			}
		} else {
			n.sendDropMessage(proposal.from)
			n.logger.Debug("Sent drop to proposal, target reached", zap.String("to", proposal.from.String()))
		}
	}
}

func (n *P2PNode) sendDropMessage(peerID peer.ID) {
	addrInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: n.host.Peerstore().Addrs(peerID),
	}

	msg := &pproto.GraphDrop{
		MessageData: n.newMessageData(),
	}

	signature, err := n.signProtoMessage(msg)
	if err != nil {
		n.logger.Error("Failed to sign graph drop", zap.Error(err))
		return
	}

	msg.MessageData.Sign = signature

	if _, ok := n.send(addrInfo, protocol.ID(graphDrop+"/"+n.sid), msg); !ok {
		n.logger.Warn("Failed to send graph drop", zap.String("to", peerID.String()))
	} else {
		n.logger.Debug("Sent graph drop", zap.String("to", peerID.String()))
	}
}

func (n *P2PNode) sendProposalsToUnsent(round int) {
	remaining := n.remainingOutboundCapacity()
	if remaining <= 0 {
		n.logger.Debug("No remaining outbound capacity", zap.Int("round", round))
		return
	}

	sent := 0
	for _, candidate := range n.shuffledPotentialNeighbors {
		if sent >= remaining {
			break
		}

		if n.IsNeighbor(candidate.ID) {
			continue
		}

		if n.sentProposalsTo[candidate.ID] {
			continue
		}

		if err := n.addNeighbor(candidate); err != nil {
			n.logger.Debug(
				"Failed to add candidate as neighbor before proposal",
				zap.String("candidate", candidate.ID.String()),
				zap.Error(err),
			)
			continue
		}

		msg := &pproto.GraphProposal{
			MessageData: n.newMessageData(),
			// round cannot be more than 2^31-1, disabling gosec for the linter to be happy
			// nolint:gosec
			Round: int32(round),
		}

		signature, err := n.signProtoMessage(msg)
		if err != nil {
			n.logger.Error("Failed to sign graph proposal", zap.Error(err))
			continue
		}

		msg.MessageData.Sign = signature

		if _, ok := n.send(candidate, protocol.ID(graphProposal+"/"+n.sid), msg); ok {
			n.sentProposalsTo[candidate.ID] = true
			sent++
			n.logger.Debug("Sent graph proposal", zap.String("to", candidate.ID.String()), zap.Int("round", round))
		} else {
			n.dropNeighbor(candidate.ID)
		}
	}

	n.logger.Info("Sent graph proposals in odd round", zap.Int("count", sent), zap.Int("round", round))
}

func (n *P2PNode) graphBuilder() {
	go n.handleDiscoveredPeers(n.ctx)

	discoveryRound, err := n.sync.WaitForRound(common.GraphDiscovery, 0)
	if err != nil {
		n.logger.Error("Failed to wait for graph discovery completion", zap.Error(err))
		panic(err)
	}

	<-discoveryRound
	n.logger.Info("Discovery phase complete, starting multi-round graph building")

	if n.buildingRounds%2 != 0 {
		n.logger.Error("Building rounds must be even", zap.Int("building_rounds", n.buildingRounds))
		panic(fmt.Sprintf("building rounds must be even, got %d (this should have been fixed during config loading)", n.buildingRounds))
	}

	n.acceptingPotentialNeighbors.Store(false)
	n.shuffledPotentialNeighbors = n.shuffleCandidates(n.collectPotentialNeighbors())
	n.logger.Info("Shuffled potential neighbors", zap.Int("count", len(n.shuffledPotentialNeighbors)))

	limit := n.maxOutbound
	if limit <= 0 {
		limit = 10
	}
	selectedTarget, err := generateSelectedTarget(limit)
	if err != nil {
		n.logger.Error("Failed to generate selected target", zap.Error(err))
		panic(err)
	}
	n.selectedNeighborTarget = selectedTarget
	n.logger.Info("Selected neighbor target", zap.Int("target", n.selectedNeighborTarget), zap.Int("limit", limit))

	for round := 0; round < n.buildingRounds; round++ {
		roundStart, waitErr := n.sync.WaitForRound(common.Network, round)
		if waitErr != nil {
			n.logger.Error("Failed to wait for graph building round", zap.Int("round", round), zap.Error(waitErr))
			panic(waitErr)
		}

		<-roundStart
		n.logger.Info("Starting graph building round", zap.Int("round", round))

		if round%2 == 1 {
			n.processDropQueue()

			n.mu.RLock()
			currentNeighbors := len(n.neighbors)
			n.mu.RUnlock()

			if currentNeighbors < limit {
				n.sendProposalsToUnsent(round)
			} else {
				n.logger.Info(
					"Neighbor count at or above limit, skipping proposals",
					zap.Int("current", currentNeighbors),
					zap.Int("limit", limit),
				)
			}
		} else {
			n.processProposalQueue()
		}
	}

	finalWait, err := n.sync.WaitForRound(common.Network, n.buildingRounds)
	if err != nil {
		n.logger.Error("Failed to wait for final graph building window", zap.Error(err))
		panic(err)
	}
	<-finalWait

	n.acceptingGraphMessages.Store(false)
	n.processDropQueue()

	n.queueMu.Lock()
	n.proposalQueue = make([]queuedProposal, 0)
	n.dropQueue = make([]queuedDrop, 0)
	n.queueMu.Unlock()

	n.logger.Info("Network building phase completed")
	neighbors := n.GetNeighbors()
	n.logger.Warn(
		"Final neighbor list", zap.Int("count", len(neighbors)), zap.Strings("neighbors", neighbors),
	)
	n.logGraphSnapshot("start")

	if n.peerDropEnabled {
		go n.simulatePeerDrop()
	}
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

			if n.logger.Core().Enabled(zap.DebugLevel) {
				n.logger.Debug(
					"Adding potential neighbor", zap.String("peer_id", pi.ID.String()), zap.Strings("addresses", addrsToStrings(pi.Addrs)),
				)
			}
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

	n.neighbors[addrInfo.ID] = addrInfo

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

// recordConnectSuccess clears a peer's failure streak after a successful dial.
func (n *P2PNode) recordConnectSuccess(id peer.ID) {
	if v, ok := n.connectFails.Load(id); ok {
		v.(*atomic.Int64).Store(0)
	}
}

// recordConnectFailure increments a peer's consecutive-dial-failure streak and, once it reaches
// connectFailsMax, evicts the peer from the neighbor set so dead peers stop stalling the send
// fan-out on every round. dropNeighbor logs the resulting degree.
func (n *P2PNode) recordConnectFailure(id peer.ID) {
	v, _ := n.connectFails.LoadOrStore(id, new(atomic.Int64))
	if v.(*atomic.Int64).Add(1) >= n.connectFailsMax {
		n.connectFails.Delete(id)
		n.logger.Warn(
			"Dropping unreachable neighbor after repeated connect failures",
			zap.String("peer_id", id.String()),
			zap.Int64("failures", n.connectFailsMax),
		)
		n.dropNeighbor(id)
	}
}

// logGraphSnapshot records the current neighbor set tagged with a phase ("start" post-build, "end"
// at shutdown) so start-vs-end can be diffed to see how many edges eviction/peer-drop removed.
func (n *P2PNode) logGraphSnapshot(phase string) {
	n.mu.RLock()
	neighbors := make([]string, 0, len(n.neighbors))
	for id := range n.neighbors {
		neighbors = append(neighbors, id.String())
	}
	n.mu.RUnlock()
	n.logger.Warn(
		"Graph snapshot",
		zap.String("phase", phase),
		zap.Int("count", len(neighbors)),
		zap.Strings("neighbors", neighbors),
	)
}

func (n *P2PNode) simulatePeerDrop() {
	// pick a uniformly random scheduled round across all post-build protocol
	// phases so the drop lands during the actual algorithm, not in the
	// pre-algorithm gap between graph building and ExPostMDAG.
	total := len(n.peerDropPhases) * (n.peerDropRounds + 1)
	f, err := cryptoFloat64()
	if err != nil {
		n.logger.Warn("Simulation: peer-drop: crypto RNG failed for slot, using slot 0", zap.Error(err))
		f = 0
	}
	slot := int(f * float64(total))
	if slot >= total {
		slot = total - 1
	}
	phase := n.peerDropPhases[slot/(n.peerDropRounds+1)]
	round := slot % (n.peerDropRounds + 1)

	waitChan, err := n.sync.WaitForRound(phase.step, round)
	if err != nil {
		n.logger.Warn("Simulation: peer-drop: failed to get round channel", zap.Error(err))
		return
	}
	select {
	case <-waitChan:
	case <-n.ctx.Done():
		return
	}

	// intra-round jitter so nodes that picked the same round still spread out
	fj, err := cryptoFloat64()
	if err != nil {
		fj = 0
	}
	select {
	case <-time.After(time.Duration(float64(phase.timeout) * fj)):
	case <-n.ctx.Done():
		return
	}

	n.mu.RLock()
	candidates := make([]peer.ID, 0, len(n.neighbors))
	for id := range n.neighbors {
		candidates = append(candidates, id)
	}
	n.mu.RUnlock()

	if len(candidates) == 0 {
		n.logger.Warn("Simulation: peer-drop: no neighbors available to drop")
		return
	}

	f2, err := cryptoFloat64()
	if err != nil {
		n.logger.Warn("Simulation: peer-drop: crypto RNG failed for selection, using first neighbor", zap.Error(err))
		f2 = 0
	}
	idx := int(f2 * float64(len(candidates)))
	if idx >= len(candidates) {
		idx = len(candidates) - 1
	}
	victim := candidates[idx]

	// Drop locally first, then best-effort notify the victim. If the
	// notification fails to send (peer unreachable, message dropped), the
	// neighbor relation becomes asymmetric: we no longer list the victim, but
	// the victim may still list us until it observes the loss independently.
	// This asymmetry is intentional for the simulation — it models real churn
	// where a departing peer cannot guarantee delivery of its goodbye.
	n.dropNeighbor(victim)
	n.logger.Warn(
		"Simulation: peer-drop",
		zap.String("peer_id", victim.String()),
		zap.String("phase", string(phase.step)),
		zap.Int("round", round),
	)
	n.sendPeerDropMessage(victim)
}

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

func (n *P2PNode) shuffleCandidates(candidates []peer.AddrInfo) []peer.AddrInfo {
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

	if !n.acceptingGraphMessages.Load() {
		n.logger.Debug("Ignoring graph proposal, not accepting messages")
		return
	}

	proposerID := s.Conn().RemotePeer()
	addrInfo := peer.AddrInfo{
		ID:    proposerID,
		Addrs: []multiaddr.Multiaddr{s.Conn().RemoteMultiaddr()},
	}

	n.queueMu.Lock()
	n.proposalQueue = append(
		n.proposalQueue, queuedProposal{
			from:     proposerID,
			addrInfo: addrInfo,
		},
	)
	n.queueMu.Unlock()

	n.logger.Debug("Queued graph proposal", zap.String("from", proposerID.String()), zap.Int32("round", data.Round))
}

func (n *P2PNode) graphDropHandler(s network.Stream) {
	data := &pproto.GraphDrop{}
	buf, err := readStreamWithLimit(s, int64(maxInboundMessageSize))
	if err != nil {
		n.logger.Error("Failed to read graph drop", zap.Error(err))
		_ = s.Reset()
		return
	}
	if err := s.Close(); err != nil {
		n.logger.Debug("Failed to close stream", zap.Error(err))
	}

	if err := proto.Unmarshal(buf, data); err != nil {
		n.logger.Error("Failed to unmarshal graph drop", zap.Error(err))
		return
	}

	if !n.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate graph drop")
		return
	}

	if !n.acceptingGraphMessages.Load() {
		n.logger.Debug("Ignoring graph drop, not accepting messages")
		return
	}

	dropperID := s.Conn().RemotePeer()

	n.queueMu.Lock()
	n.dropQueue = append(
		n.dropQueue, queuedDrop{
			from: dropperID,
		},
	)
	n.queueMu.Unlock()

	n.logger.Debug("Queued graph drop", zap.String("from", dropperID.String()))
}

func (n *P2PNode) sendPeerDropMessage(peerID peer.ID) {
	addrInfo := peer.AddrInfo{
		ID:    peerID,
		Addrs: n.host.Peerstore().Addrs(peerID),
	}

	msg := &pproto.GraphDrop{
		MessageData: n.newMessageData(),
	}

	signature, err := n.signProtoMessage(msg)
	if err != nil {
		n.logger.Error("Failed to sign peer drop notification", zap.Error(err))
		return
	}

	msg.MessageData.Sign = signature

	if _, ok := n.send(addrInfo, protocol.ID(peerDrop+"/"+n.sid), msg); !ok {
		n.logger.Warn("Failed to send peer drop notification", zap.String("to", peerID.String()))
	} else {
		n.logger.Debug("Sent peer drop notification", zap.String("to", peerID.String()))
	}
}

func (n *P2PNode) peerDropHandler(s network.Stream) {
	data := &pproto.GraphDrop{}
	buf, err := readStreamWithLimit(s, int64(maxInboundMessageSize))
	if err != nil {
		n.logger.Error("Failed to read peer drop notification", zap.Error(err))
		_ = s.Reset()
		return
	}
	if err := s.Close(); err != nil {
		n.logger.Debug("Failed to close stream", zap.Error(err))
	}

	if err := proto.Unmarshal(buf, data); err != nil {
		n.logger.Error("Failed to unmarshal peer drop notification", zap.Error(err))
		return
	}

	if !n.authenticateMessage(data, data.MessageData) {
		n.logger.Error("Failed to authenticate peer drop notification")
		return
	}

	dropperID := s.Conn().RemotePeer()
	n.dropNeighbor(dropperID)
}
