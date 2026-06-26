package network

import (
	"bytes"
	"context"
	"crypto/rand"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pproto "github.com/mamorski/committee-sampling/pkg/proto"
)

type MockConn struct {
	mock.Mock
}

func (m *MockConn) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockConn) LocalPeer() peer.ID {
	args := m.Called()
	return args.Get(0).(peer.ID)
}

func (m *MockConn) LocalPrivateKey() crypto.PrivKey {
	args := m.Called()
	return args.Get(0).(crypto.PrivKey)
}

func (m *MockConn) RemotePeer() peer.ID {
	args := m.Called()
	return args.Get(0).(peer.ID)
}

func (m *MockConn) RemotePublicKey() crypto.PubKey {
	args := m.Called()
	return args.Get(0).(crypto.PubKey)
}

func (m *MockConn) LocalMultiaddr() multiaddr.Multiaddr {
	args := m.Called()
	return args.Get(0).(multiaddr.Multiaddr)
}

func (m *MockConn) RemoteMultiaddr() multiaddr.Multiaddr {
	args := m.Called()
	return args.Get(0).(multiaddr.Multiaddr)
}

func (m *MockConn) ID() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockConn) NewStream(ctx context.Context) (network.Stream, error) {
	args := m.Called(ctx)
	return args.Get(0).(network.Stream), args.Error(1)
}

func (m *MockConn) GetStreams() []network.Stream {
	args := m.Called()
	return args.Get(0).([]network.Stream)
}

func (m *MockConn) Stat() network.ConnStats {
	args := m.Called()
	return args.Get(0).(network.ConnStats)
}

func (m *MockConn) Scope() network.ConnScope {
	args := m.Called()
	return args.Get(0).(network.ConnScope)
}

func (m *MockConn) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockConn) ConnState() network.ConnectionState {
	args := m.Called()
	return args.Get(0).(network.ConnectionState)
}

type HostTestSuite struct {
	suite.Suite
	mockHost      *MockHost
	mockPeerstore *MockPeerstore
	mockDiscovery *MockDiscovery
	node          *P2PNode
	ctx           context.Context
	cancel        context.CancelFunc
	logger        *zap.Logger
	testPeerID    peer.ID
	testPrivKey   crypto.PrivKey
	testPubKey    crypto.PubKey
}

func (suite *HostTestSuite) SetupTest() {
	suite.ctx, suite.cancel = context.WithCancel(context.Background())
	suite.logger = zap.NewNop()

	// Generate test keys
	priv, pub, err := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	suite.Require().NoError(err)
	suite.testPrivKey = priv
	suite.testPubKey = pub

	// Generate test peer ID
	suite.testPeerID, err = peer.IDFromPublicKey(pub)
	suite.Require().NoError(err)

	pubKeyBytes, err := crypto.MarshalPublicKey(pub)
	suite.Require().NoError(err)

	suite.mockHost = &MockHost{}
	suite.mockPeerstore = &MockPeerstore{}
	suite.mockDiscovery = &MockDiscovery{
		ch: make(chan peer.AddrInfo, 1),
	}

	suite.node = &P2PNode{
		host:        suite.mockHost,
		ctx:         suite.ctx,
		cancel:      suite.cancel,
		logger:      suite.logger,
		discovery:   suite.mockDiscovery,
		maxOutbound: 5,
		key:         suite.testPrivKey,
		nodeID:      suite.testPeerID.String(),
		nodePubKey:  pubKeyBytes,
		neighbors:   make(map[peer.ID]peer.AddrInfo),
		appBytes:    newAppByteTracker(),
		streamPool:  make(map[streamKey]*pooledStream),
	}
	suite.node.acceptingPotentialNeighbors.Store(true)
}

func (suite *HostTestSuite) TearDownTest() {
	suite.cancel()
}

func (suite *HostTestSuite) TestClose() {
	suite.mockDiscovery.On("Stop").Return(nil)
	suite.mockHost.On("Close").Return(nil)

	err := suite.node.Close()
	suite.NoError(err)

	suite.mockDiscovery.AssertExpectations(suite.T())
	suite.mockHost.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestCloseDiscoveryError() {
	suite.mockDiscovery.On("Stop").Return(assert.AnError)

	err := suite.node.Close()
	suite.Error(err)

	suite.mockDiscovery.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestGetNodeID() {
	suite.mockHost.On("ID").Return(suite.testPeerID)

	nodeID := suite.node.GetNodeID()
	suite.Equal(suite.testPeerID.String(), nodeID)

	suite.mockHost.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestGetNeighbors() {
	// Add some test neighbors
	_, pub2, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	testPeerID2, _ := peer.IDFromPublicKey(pub2)

	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	suite.node.neighbors[suite.testPeerID] = peer.AddrInfo{
		ID:    suite.testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}
	suite.node.neighbors[testPeerID2] = peer.AddrInfo{
		ID:    testPeerID2,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	neighbors := suite.node.GetNeighbors()
	suite.Len(neighbors, 2)
	suite.Contains(neighbors, suite.testPeerID.String())
	suite.Contains(neighbors, testPeerID2.String())
}

func (suite *HostTestSuite) TestGetNeighborsEmpty() {
	neighbors := suite.node.GetNeighbors()
	suite.Len(neighbors, 0)
}

func (suite *HostTestSuite) TestSendProtocolMessageNoNeighbors() {
	// Test sending message when no neighbors exist
	suite.node.SendProtocolMessage("test-protocol", []byte("test-data"))
	// Should not panic and should handle gracefully
}

func (suite *HostTestSuite) TestSendProtocolMessageWithNeighbors() {
	// Setup mock expectations
	suite.mockHost.On("ID").Return(suite.testPeerID).Maybe()
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore).Maybe()
	suite.mockPeerstore.On("PubKey", suite.testPeerID).Return(suite.testPubKey).Maybe()
	suite.mockPeerstore.On("PrivKey", suite.testPeerID).Return(suite.testPrivKey).Maybe()

	// Pooled framing: open one stream, then write length-prefixed frames over it.
	// msgio's WriteMsg issues two Writes (varint length prefix + body) and the
	// stream stays open in the pool (no per-message Close).
	mockStream := &MockStream{}
	suite.mockHost.On("NewStream", mock.Anything, mock.Anything, mock.Anything).Return(mockStream, nil).Once()
	mockStream.On("Write", mock.Anything).Return(100, nil)

	// Add a neighbor
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	suite.node.neighbors = make(map[peer.ID]peer.AddrInfo) // Initialize the map
	suite.node.neighbors[suite.testPeerID] = peer.AddrInfo{
		ID:    suite.testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	suite.node.SendProtocolMessage("test-protocol", []byte("test-data"))

	suite.mockHost.AssertExpectations(suite.T())
	suite.mockPeerstore.AssertExpectations(suite.T())
	mockStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestSendFramedReusesStream() {
	// Three sends to the same (peer, protocol) must open the stream once and
	// reuse it: NewStream is called exactly once, with two framed Writes per
	// message (varint prefix + body), and no Close between messages.
	mockStream := &MockStream{}
	suite.mockHost.On("NewStream", mock.Anything, mock.Anything, mock.Anything).Return(mockStream, nil).Once()
	mockStream.On("Write", mock.Anything).Return(100, nil)

	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	addrInfo := peer.AddrInfo{ID: suite.testPeerID, Addrs: []multiaddr.Multiaddr{addr}}
	msg := &pproto.ProtocolMessage{Payload: []byte("payload")}

	for i := 0; i < 3; i++ {
		size, ok := suite.node.sendFramed(addrInfo, protocol.ID("test-protocol"), msg)
		suite.True(ok)
		suite.Greater(size, int64(0))
	}

	suite.mockHost.AssertExpectations(suite.T())
	mockStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestRegisterHandler() {
	handlerCalled := false
	handler := func(from peer.ID, payload []byte) error {
		handlerCalled = true
		suite.Equal("test-data", string(payload))
		return nil
	}

	suite.mockHost.On("SetStreamHandler", protocol.ID("test-protocol"), mock.Anything)

	suite.node.RegisterHandler("test-protocol", handler)

	// Verify handler was registered
	suite.False(handlerCalled) // Handler isn't called yet, just registered
	suite.mockHost.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestAddNeighbor() {
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	addrInfo := peer.AddrInfo{
		ID:    suite.testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	suite.mockHost.On("Connect", context.Background(), addrInfo).Return(nil)

	err := suite.node.addNeighbor(addrInfo)
	suite.NoError(err)
	suite.Equal(1, len(suite.node.neighbors))

	// Verify neighbor was stored
	stored, ok := suite.node.neighbors[suite.testPeerID]
	suite.True(ok)
	suite.Equal(addrInfo, stored)

	suite.mockHost.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestAddNeighborConnectionError() {
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	addrInfo := peer.AddrInfo{
		ID:    suite.testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	suite.mockHost.On("Connect", context.Background(), addrInfo).Return(assert.AnError)

	err := suite.node.addNeighbor(addrInfo)
	suite.Error(err)
	suite.Equal(0, len(suite.node.neighbors))

	// Verify neighbor was not stored
	_, ok := suite.node.neighbors[suite.testPeerID]
	suite.False(ok)

	suite.mockHost.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestAddNeighborAlreadyExists() {
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	addrInfo := peer.AddrInfo{
		ID:    suite.testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	// Add neighbor first time
	suite.node.neighbors[suite.testPeerID] = addrInfo

	// Try to add again - should not call Connect
	err := suite.node.addNeighbor(addrInfo)
	suite.NoError(err)
}

func (suite *HostTestSuite) TestHandleDiscoveredPeers() {
	// Create a test peer
	_, pub, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	testPeerID, _ := peer.IDFromPublicKey(pub)
	myPeerID := peer.ID("test-peer-id")
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")

	addrInfo := peer.AddrInfo{
		ID:    testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	// Setup discovery channel
	ch := make(chan peer.AddrInfo, 1)
	suite.mockDiscovery.On("DiscoveredPeers").Return((<-chan peer.AddrInfo)(ch))
	suite.mockHost.On("ID").Return(myPeerID).Once()
	// Start the handler in a goroutine
	go suite.node.handleDiscoveredPeers(suite.ctx)

	// Send a peer to the channel
	ch <- addrInfo

	// Give some time for processing
	time.Sleep(100 * time.Millisecond)

	// Cancel context to stop the handler
	suite.cancel()

	suite.NotEmpty(suite.node.potentialNeighbors.Load(testPeerID))
	suite.mockDiscovery.AssertExpectations(suite.T())
	suite.mockHost.AssertExpectations(suite.T())
	suite.mockPeerstore.AssertExpectations(suite.T())

}

func (suite *HostTestSuite) TestRemainingCapacity() {
	suite.node.maxOutbound = 3

	addr1, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8081")
	addr2, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8082")

	_, pub1, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	_, pub2, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)

	peer1, _ := peer.IDFromPublicKey(pub1)
	peer2, _ := peer.IDFromPublicKey(pub2)

	suite.mockHost.On("Connect", mock.Anything, mock.Anything).Return(nil).Times(2)

	err := suite.node.addNeighbor(peer.AddrInfo{ID: peer1, Addrs: []multiaddr.Multiaddr{addr1}})
	suite.NoError(err)
	err = suite.node.addNeighbor(peer.AddrInfo{ID: peer2, Addrs: []multiaddr.Multiaddr{addr2}})
	suite.NoError(err)

	suite.Equal(2, len(suite.node.neighbors), "Should have 2 total neighbors")
	suite.Equal(1, suite.node.remainingOutboundCapacity(), "Should have 1 remaining capacity (3 - 2)")
	suite.True(suite.node.remainingOutboundCapacity() > 0, "Should still be able to send proposals")
}

func (suite *HostTestSuite) TestAddNeighborTracking() {
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8081")
	_, pub, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	peerID, _ := peer.IDFromPublicKey(pub)

	suite.mockHost.On("Connect", mock.Anything, mock.Anything).Return(nil).Once()

	err := suite.node.addNeighbor(peer.AddrInfo{ID: peerID, Addrs: []multiaddr.Multiaddr{addr}})
	suite.NoError(err)
	suite.Equal(1, len(suite.node.neighbors), "Should track neighbor")
}

func (suite *HostTestSuite) TestDropNeighborCleansUp() {
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8081")
	_, pub, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	peerID, _ := peer.IDFromPublicKey(pub)

	suite.mockHost.On("Connect", mock.Anything, mock.Anything).Return(nil).Once()

	err := suite.node.addNeighbor(peer.AddrInfo{ID: peerID, Addrs: []multiaddr.Multiaddr{addr}})
	suite.NoError(err)
	suite.Equal(1, len(suite.node.neighbors))

	suite.node.dropNeighbor(peerID)

	suite.Equal(0, len(suite.node.neighbors), "neighbors should be cleaned up")
}

func (suite *HostTestSuite) TestGraphDropHandler() {
	suite.mockHost.On("ID").Return(suite.testPeerID).Maybe()
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore).Maybe()
	suite.mockPeerstore.On("PubKey", suite.testPeerID).Return(suite.testPubKey).Maybe()
	suite.mockPeerstore.On("PrivKey", suite.testPeerID).Return(suite.testPrivKey).Maybe()

	_, pub, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	remotePeerID, _ := peer.IDFromPublicKey(pub)
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8081")

	mockConn := &MockConn{}
	mockConn.On("RemotePeer").Return(remotePeerID)
	mockConn.On("RemoteMultiaddr").Return(addr)

	mockStream := &MockStream{}
	mockStream.On("Conn").Return(mockConn)

	dropMsg := &pproto.GraphDrop{
		MessageData: suite.node.newMessageData(),
	}

	signature, err := suite.node.signProtoMessage(dropMsg)
	suite.Require().NoError(err)
	dropMsg.MessageData.Sign = signature

	msgBytes, err := proto.Marshal(dropMsg)
	suite.Require().NoError(err)

	mockStream.On("Close").Return(nil)

	suite.node.acceptingGraphMessages.Store(true)

	reader := bytes.NewReader(msgBytes)
	buf, err := readStreamWithLimit(reader, int64(maxInboundMessageSize))
	suite.Require().NoError(err)

	data := &pproto.GraphDrop{}
	err = proto.Unmarshal(buf, data)
	suite.Require().NoError(err)

	suite.True(suite.node.authenticateMessage(data, data.MessageData))

	suite.node.queueMu.Lock()
	initialQueueLen := len(suite.node.dropQueue)
	suite.node.queueMu.Unlock()

	suite.node.queueMu.Lock()
	suite.node.dropQueue = append(suite.node.dropQueue, queuedDrop{
		from: remotePeerID,
	})
	suite.node.queueMu.Unlock()

	suite.node.queueMu.Lock()
	finalQueueLen := len(suite.node.dropQueue)
	suite.node.queueMu.Unlock()

	suite.Equal(initialQueueLen+1, finalQueueLen)
}

func (suite *HostTestSuite) TestProcessDropQueue() {
	_, pub, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	peerID, _ := peer.IDFromPublicKey(pub)
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8081")

	suite.mockHost.On("Connect", mock.Anything, mock.Anything).Return(nil).Once()

	err := suite.node.addNeighbor(peer.AddrInfo{ID: peerID, Addrs: []multiaddr.Multiaddr{addr}})
	suite.NoError(err)
	suite.Equal(1, len(suite.node.neighbors))

	suite.node.queueMu.Lock()
	suite.node.dropQueue = append(suite.node.dropQueue, queuedDrop{
		from: peerID,
	})
	suite.node.queueMu.Unlock()

	suite.node.processDropQueue()

	suite.Equal(0, len(suite.node.neighbors), "Neighbor should be removed after processing drop queue")
	suite.node.queueMu.Lock()
	suite.Equal(0, len(suite.node.dropQueue), "Drop queue should be empty after processing")
	suite.node.queueMu.Unlock()
}

func (suite *HostTestSuite) TestSendDropMessage() {
	suite.mockHost.On("ID").Return(suite.testPeerID).Maybe()
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore).Maybe()
	suite.mockPeerstore.On("PubKey", suite.testPeerID).Return(suite.testPubKey).Maybe()
	suite.mockPeerstore.On("PrivKey", suite.testPeerID).Return(suite.testPrivKey).Maybe()

	_, pub, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	peerID, _ := peer.IDFromPublicKey(pub)

	suite.mockPeerstore.On("Addrs", peerID).Return([]multiaddr.Multiaddr{})

	mockStream := &MockStream{}
	suite.mockHost.On("NewStream", mock.Anything, peerID, mock.Anything).Return(mockStream, nil).Once()
	mockStream.On("Write", mock.Anything).Return(100, nil).Once()
	mockStream.On("Close").Return(nil).Once()

	suite.node.sendDropMessage(peerID)

	suite.mockHost.AssertExpectations(suite.T())
	mockStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestGenerateSelectedTarget() {
	limit := 10
	target, err := generateSelectedTarget(limit)

	suite.NoError(err)
	suite.GreaterOrEqual(target, limit*2, "Target should be at least limit*2")
	suite.LessOrEqual(target, limit*3, "Target should be at most limit*3")
}

func TestHostSuite(t *testing.T) {
	suite.Run(t, new(HostTestSuite))
}

func TestReadStreamWithLimit(t *testing.T) {
	payload := []byte("payload")
	result, err := readStreamWithLimit(bytes.NewReader(payload), int64(len(payload)+10))
	assert.NoError(t, err)
	assert.Equal(t, payload, result)

	_, err = readStreamWithLimit(bytes.NewReader(make([]byte, maxInboundMessageSize+1)), int64(maxInboundMessageSize))
	assert.Error(t, err)
}
