package network

import (
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/google/uuid"
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

	"github.com/mamorski/committee-sampling/pkg/config"
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

	suite.mockHost = &MockHost{}
	suite.mockPeerstore = &MockPeerstore{}
	suite.mockDiscovery = &MockDiscovery{
		ch: make(chan peer.AddrInfo, 1),
	}

	suite.node = &P2PNode{
		host:              suite.mockHost,
		ctx:               suite.ctx,
		cancel:            suite.cancel,
		logger:            suite.logger,
		discovery:         suite.mockDiscovery,
		maxOutbound:       5,
		heartbeatInterval: time.Second,
		key:               suite.testPrivKey,
	}
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
	suite.node.neighbors.Store(suite.testPeerID, peer.AddrInfo{
		ID:    suite.testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	})
	suite.node.neighbors.Store(testPeerID2, peer.AddrInfo{
		ID:    testPeerID2,
		Addrs: []multiaddr.Multiaddr{addr},
	})

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
	suite.mockHost.On("ID").Return(suite.testPeerID)
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore)
	suite.mockPeerstore.On("PubKey", suite.testPeerID).Return(suite.testPubKey)
	suite.mockPeerstore.On("PrivKey", suite.testPeerID).Return(suite.testPrivKey)
	suite.mockHost.On("Connect", mock.Anything, mock.Anything).Return(nil)

	mockStream := &MockStream{}
	suite.mockHost.On("NewStream", mock.Anything, mock.Anything, mock.Anything).Return(mockStream, nil)
	mockStream.On("Write", mock.Anything).Return(100, nil)
	mockStream.On("Close").Return(nil)

	// Add a neighbor
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	suite.node.neighbors.Store(suite.testPeerID, peer.AddrInfo{
		ID:    suite.testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	})

	suite.node.SendProtocolMessage("test-protocol", []byte("test-data"))

	suite.mockHost.AssertExpectations(suite.T())
	suite.mockPeerstore.AssertExpectations(suite.T())
	mockStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestRegisterHandler() {
	handlerCalled := false
	handler := func(from string, payload []byte) error {
		handlerCalled = true
		suite.Equal("test-data", string(payload))
		return nil
	}

	suite.mockHost.On("SetStreamHandler", protocol.ID("test-protocol"), mock.Anything)

	suite.node.RegisterHandler("test-protocol", handler)

	// Verify handler was registered
	suite.False(handlerCalled) // Handler not called yet, just registered
	suite.mockHost.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestAddNeighbor() {
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	addrInfo := peer.AddrInfo{
		ID:    suite.testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	suite.mockHost.On("Connect", suite.ctx, addrInfo).Return(nil)

	err := suite.node.addNeighbor(addrInfo)
	suite.NoError(err)
	suite.Equal(1, suite.node.numOfNeighbors)

	// Verify neighbor was stored
	stored, ok := suite.node.neighbors.Load(suite.testPeerID)
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

	suite.mockHost.On("Connect", suite.ctx, addrInfo).Return(assert.AnError)

	err := suite.node.addNeighbor(addrInfo)
	suite.Error(err)
	suite.Equal(0, suite.node.numOfNeighbors)

	// Verify neighbor was not stored
	_, ok := suite.node.neighbors.Load(suite.testPeerID)
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
	suite.node.neighbors.Store(suite.testPeerID, addrInfo)

	// Try to add again - should not call Connect
	err := suite.node.addNeighbor(addrInfo)
	suite.NoError(err)
}

func (suite *HostTestSuite) TestSendRequestToNeighborSuccess() {
	// Create a peer with lower ID to ensure request is sent
	lowerPriv, _, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	lowerPeerID, _ := peer.IDFromPublicKey(lowerPriv.GetPublic())

	// Ensure our node has higher ID
	suite.mockHost.On("ID").Return(peer.ID("z" + lowerPeerID.String()))
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore)
	suite.mockPeerstore.On("PubKey", mock.Anything).Return(suite.testPubKey)
	suite.mockPeerstore.On("PrivKey", mock.Anything).Return(suite.testPrivKey)
	suite.mockHost.On("Connect", mock.Anything, mock.Anything).Return(nil)

	mockStream := &MockStream{}
	suite.mockHost.On("NewStream", mock.Anything, mock.Anything, mock.Anything).Return(mockStream, nil)
	mockStream.On("Write", mock.Anything).Return(100, nil)
	mockStream.On("Close").Return(nil)

	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	addrInfo := peer.AddrInfo{
		ID:    lowerPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	suite.node.sendRequestToNeighbor(addrInfo)

	suite.mockHost.AssertExpectations(suite.T())
	suite.mockPeerstore.AssertExpectations(suite.T())
	mockStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestSendRequestToNeighborHigherID() {
	// Create a peer with higher ID
	higherPeerID := peer.ID("z" + suite.testPeerID.String())

	suite.mockHost.On("ID").Return(suite.testPeerID)

	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	addrInfo := peer.AddrInfo{
		ID:    higherPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	// Should ignore peer with higher ID
	suite.node.sendRequestToNeighbor(addrInfo)

	suite.mockHost.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestSendRequestToNeighborMaxOutbound() {
	suite.node.numOfNeighbors = suite.node.maxOutbound

	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	addrInfo := peer.AddrInfo{
		ID:    suite.testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	// Create a peer ID that's definitely lower
	suite.mockHost.On("ID").Return(peer.ID("z" + suite.testPeerID.String()))

	// Should not send request when at max capacity
	suite.node.sendRequestToNeighbor(addrInfo)

	suite.mockHost.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestSendRequestToNeighborAlreadyConnected() {
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	addrInfo := peer.AddrInfo{
		ID:    suite.testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	// Add neighbor first
	suite.node.neighbors.Store(suite.testPeerID, addrInfo)

	suite.mockHost.On("ID").Return(peer.ID("z" + suite.testPeerID.String()))

	// Should not send request to already connected peer
	suite.node.sendRequestToNeighbor(addrInfo)

	suite.mockHost.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestOnNeighborRequestSuccess() {
	// Setup mock expectations for newMessageData
	suite.mockHost.On("ID").Return(suite.testPeerID).Times(5)
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore).Times(3)
	suite.mockPeerstore.On("PubKey", suite.testPeerID).Return(suite.testPubKey).Twice()

	// Create test message data using the actual method
	messageData := suite.node.newMessageData(uuid.New().String(), false)

	testMessage := &pproto.NeighborMessage{
		MessageData: messageData,
	}

	// Sign the message
	data, err := proto.Marshal(testMessage)
	suite.Require().NoError(err)
	signature, err := suite.testPrivKey.Sign(data)
	suite.Require().NoError(err)
	messageData.Sign = signature

	// Marshal the complete message
	messageBytes, err := proto.Marshal(testMessage)
	suite.Require().NoError(err)

	// Create mock stream and connection
	mockStream := &MockStream{}
	mockConn := &MockConn{}
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")

	// Setup stream read expectations
	mockStream.On("Read", mock.Anything).Return(len(messageBytes), nil).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, messageBytes)
	}).Once()
	mockStream.On("Read", mock.Anything).Return(0, io.EOF).Once()
	mockStream.On("Close").Return(nil)
	mockStream.On("Conn").Return(mockConn).Times(3)
	mockConn.On("RemotePeer").Return(suite.testPeerID).Twice()
	mockConn.On("RemoteMultiaddr").Return(addr).Once()

	// Setup host expectations for adding neighbor
	suite.mockHost.On("Connect", mock.Anything, mock.AnythingOfType("peer.AddrInfo")).Return(nil).Twice()

	// Setup expectations for response sending (reuse existing mocks)
	suite.mockPeerstore.On("PrivKey", suite.testPeerID).Return(suite.testPrivKey).Once()

	responseStream := &MockStream{}
	suite.mockHost.On("NewStream", mock.Anything, mock.Anything, mock.Anything).Return(responseStream, nil).Once()
	responseStream.On("Write", mock.Anything).Return(100, nil)
	responseStream.On("Close").Return(nil)

	// Call the method
	suite.node.onNeighborRequest(mockStream)

	// Verify neighbor was added
	suite.Equal(1, suite.node.numOfNeighbors)
	_, exists := suite.node.neighbors.Load(suite.testPeerID)
	suite.True(exists)

	// Verify all expectations
	mockStream.AssertExpectations(suite.T())
	mockConn.AssertExpectations(suite.T())
	suite.mockHost.AssertExpectations(suite.T())
	suite.mockPeerstore.AssertExpectations(suite.T())
	responseStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestOnNeighborRequestInvalidMessage() {
	// Create invalid message data (missing signature)
	invalidMessage := []byte("invalid protobuf data")

	mockStream := &MockStream{}

	// Setup stream read expectations
	mockStream.On("Read", mock.Anything).Return(len(invalidMessage), nil).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, invalidMessage)
	}).Once()
	mockStream.On("Read", mock.Anything).Return(0, io.EOF).Once()
	mockStream.On("Close").Return(nil)

	// Call the method - should handle gracefully
	suite.node.onNeighborRequest(mockStream)

	// Verify no neighbor was added
	suite.Equal(0, suite.node.numOfNeighbors)

	mockStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestOnNeighborRequestAuthenticationFailure() {
	// Setup mock expectations for newMessageData
	suite.mockHost.On("ID").Return(suite.testPeerID)
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore)
	suite.mockPeerstore.On("PubKey", suite.testPeerID).Return(suite.testPubKey)

	// Create test message with invalid signature
	messageData := suite.node.newMessageData(uuid.New().String(), false)
	messageData.Sign = []byte("invalid-signature")

	testMessage := &pproto.NeighborMessage{
		MessageData: messageData,
	}

	// Marshal the message
	messageBytes, err := proto.Marshal(testMessage)
	suite.Require().NoError(err)

	mockStream := &MockStream{}

	// Setup stream read expectations
	mockStream.On("Read", mock.Anything).Return(len(messageBytes), nil).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, messageBytes)
	}).Once()
	mockStream.On("Read", mock.Anything).Return(0, io.EOF).Once()
	mockStream.On("Close").Return(nil)

	// Call the method
	suite.node.onNeighborRequest(mockStream)

	// Verify no neighbor was added due to authentication failure
	suite.Equal(0, suite.node.numOfNeighbors)

	mockStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestOnNeighborRequestAddNeighborFailure() {
	// Common mock setup with explicit invocation counts
	suite.mockHost.On("ID").Return(suite.testPeerID).Times(5)
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore).Times(3)
	suite.mockPeerstore.On("PubKey", suite.testPeerID).Return(suite.testPubKey).Twice()
	suite.mockPeerstore.On("PrivKey", suite.testPeerID).Return(suite.testPrivKey).Once()

	// Create and sign a valid NeighborMessage
	msgData := suite.node.newMessageData(uuid.New().String(), false)
	testMessage := &pproto.NeighborMessage{MessageData: msgData}
	raw, err := proto.Marshal(testMessage)
	suite.Require().NoError(err)
	sig, err := suite.testPrivKey.Sign(raw)
	suite.Require().NoError(err)
	msgData.Sign = sig
	payload, err := proto.Marshal(testMessage)
	suite.Require().NoError(err)

	// Stubbed stream reading
	mockStream := &MockStream{}
	mockConn := &MockConn{}
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")
	mockStream.On("Read", mock.Anything).Return(len(payload), nil).Run(func(args mock.Arguments) {
		copy(args.Get(0).([]byte), payload)
	}).Once()
	mockStream.On("Read", mock.Anything).Return(0, io.EOF).Once()
	mockStream.On("Close").Return(nil)
	mockStream.On("Conn").Return(mockConn)
	mockConn.On("RemotePeer").Return(suite.testPeerID)
	mockConn.On("RemoteMultiaddr").Return(addr)

	// Simulate connection failure exactly once
	suite.mockHost.On("Connect", suite.ctx, mock.Anything).Return(assert.AnError).Once()

	// Execute
	suite.node.onNeighborRequest(mockStream)

	// Expect no neighbor added
	suite.Equal(0, suite.node.numOfNeighbors)

	// Verify all expectations
	mockStream.AssertExpectations(suite.T())
	mockConn.AssertExpectations(suite.T())
	suite.mockHost.AssertExpectations(suite.T())
	suite.mockPeerstore.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestOnNeighborResponseSuccess() {
	// Setup mock expectations for newMessageData
	suite.mockHost.On("ID").Return(suite.testPeerID)
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore)
	suite.mockPeerstore.On("PubKey", suite.testPeerID).Return(suite.testPubKey)

	// Create test response message
	messageData := suite.node.newMessageData(uuid.New().String(), false)

	testResponse := &pproto.NeighborMessageResponse{
		MessageData: messageData,
		Success:     true,
	}

	// Sign the message
	data, err := proto.Marshal(testResponse)
	suite.Require().NoError(err)
	signature, err := suite.testPrivKey.Sign(data)
	suite.Require().NoError(err)
	messageData.Sign = signature

	// Marshal the complete message
	responseBytes, err := proto.Marshal(testResponse)
	suite.Require().NoError(err)

	// Create mock stream and connection
	mockStream := &MockStream{}
	mockConn := &MockConn{}
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")

	// Setup stream read expectations
	mockStream.On("Read", mock.Anything).Return(len(responseBytes), nil).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, responseBytes)
	}).Once()
	mockStream.On("Read", mock.Anything).Return(0, io.EOF).Once()
	mockStream.On("Close").Return(nil)
	mockStream.On("Conn").Return(mockConn)
	mockConn.On("RemotePeer").Return(suite.testPeerID)
	mockConn.On("RemoteMultiaddr").Return(addr)

	// Setup host expectations for adding neighbor
	suite.mockHost.On("Connect", suite.ctx, mock.Anything).Return(nil)

	// Call the method
	suite.node.onNeighborResponse(mockStream)

	// Verify neighbor was added
	suite.Equal(1, suite.node.numOfNeighbors)
	_, exists := suite.node.neighbors.Load(suite.testPeerID)
	suite.True(exists)

	// Verify all expectations
	mockStream.AssertExpectations(suite.T())
	mockConn.AssertExpectations(suite.T())
	suite.mockHost.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestOnNeighborResponseRejected() {
	// Setup mock expectations for newMessageData
	suite.mockHost.On("ID").Return(suite.testPeerID)
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore)
	suite.mockPeerstore.On("PubKey", suite.testPeerID).Return(suite.testPubKey)

	// Create test response message with success = false
	messageData := suite.node.newMessageData(uuid.New().String(), false)

	testResponse := &pproto.NeighborMessageResponse{
		MessageData: messageData,
		Success:     false, // Rejected
	}

	// Sign the message
	data, err := proto.Marshal(testResponse)
	suite.Require().NoError(err)
	signature, err := suite.testPrivKey.Sign(data)
	suite.Require().NoError(err)
	messageData.Sign = signature

	// Marshal the complete message
	responseBytes, err := proto.Marshal(testResponse)
	suite.Require().NoError(err)

	// Create mock stream and connection
	mockStream := &MockStream{}
	mockConn := &MockConn{}

	// Setup stream read expectations
	mockStream.On("Read", mock.Anything).Return(len(responseBytes), nil).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, responseBytes)
	}).Once()
	mockStream.On("Read", mock.Anything).Return(0, io.EOF).Once()
	mockStream.On("Close").Return(nil)
	mockStream.On("Conn").Return(mockConn)
	mockConn.On("RemotePeer").Return(suite.testPeerID)

	// Call the method
	suite.node.onNeighborResponse(mockStream)

	// Verify no neighbor was added due to rejection
	suite.Equal(0, suite.node.numOfNeighbors)

	// Verify all expectations
	mockStream.AssertExpectations(suite.T())
	mockConn.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestOnNeighborResponseInvalidMessage() {
	// Create invalid message data
	invalidMessage := []byte("invalid protobuf data")

	mockStream := &MockStream{}

	// Setup stream read expectations
	mockStream.On("Read", mock.Anything).Return(len(invalidMessage), nil).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, invalidMessage)
	}).Once()
	mockStream.On("Read", mock.Anything).Return(0, io.EOF).Once()
	mockStream.On("Close").Return(nil)

	// Call the method - should handle gracefully
	suite.node.onNeighborResponse(mockStream)

	// Verify no neighbor was added
	suite.Equal(0, suite.node.numOfNeighbors)

	mockStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestOnNeighborResponseAuthenticationFailure() {
	// Setup mock expectations for newMessageData
	suite.mockHost.On("ID").Return(suite.testPeerID)
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore)
	suite.mockPeerstore.On("PubKey", suite.testPeerID).Return(suite.testPubKey)

	// Create test response with invalid signature
	messageData := suite.node.newMessageData(uuid.New().String(), false)
	messageData.Sign = []byte("invalid-signature")

	testResponse := &pproto.NeighborMessageResponse{
		MessageData: messageData,
		Success:     true,
	}

	// Marshal the message
	responseBytes, err := proto.Marshal(testResponse)
	suite.Require().NoError(err)

	mockStream := &MockStream{}

	// Setup stream read expectations
	mockStream.On("Read", mock.Anything).Return(len(responseBytes), nil).Run(func(args mock.Arguments) {
		buf := args.Get(0).([]byte)
		copy(buf, responseBytes)
	}).Once()
	mockStream.On("Read", mock.Anything).Return(0, io.EOF).Once()
	mockStream.On("Close").Return(nil)

	// Call the method
	suite.node.onNeighborResponse(mockStream)

	// Verify no neighbor was added due to authentication failure
	suite.Equal(0, suite.node.numOfNeighbors)

	mockStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestHandleDiscoveredPeers() {
	// Create a test peer
	_, pub, _ := crypto.GenerateKeyPairWithReader(crypto.Ed25519, 2048, rand.Reader)
	testPeerID, _ := peer.IDFromPublicKey(pub)
	addr, _ := multiaddr.NewMultiaddr("/ip4/127.0.0.1/tcp/8080")

	addrInfo := peer.AddrInfo{
		ID:    testPeerID,
		Addrs: []multiaddr.Multiaddr{addr},
	}

	// Setup discovery channel
	ch := make(chan peer.AddrInfo, 1)
	suite.mockDiscovery.On("DiscoveredPeers").Return((<-chan peer.AddrInfo)(ch))

	// Setup expectations for sendRequestToNeighbor
	suite.mockHost.On("ID").Return(peer.ID("z" + testPeerID.String())) // Higher ID to trigger request
	suite.mockHost.On("Peerstore").Return(suite.mockPeerstore)
	suite.mockPeerstore.On("PubKey", mock.Anything).Return(suite.testPubKey)
	suite.mockPeerstore.On("PrivKey", mock.Anything).Return(suite.testPrivKey)
	suite.mockHost.On("Connect", mock.Anything, mock.Anything).Return(nil)

	mockStream := &MockStream{}
	suite.mockHost.On("NewStream", mock.Anything, mock.Anything, mock.Anything).Return(mockStream, nil)
	mockStream.On("Write", mock.Anything).Return(100, nil)
	mockStream.On("Close").Return(nil)

	// Start the handler in a goroutine
	go suite.node.handleDiscoveredPeers()

	// Send a peer to the channel
	ch <- addrInfo

	// Give some time for processing
	time.Sleep(100 * time.Millisecond)

	// Cancel context to stop the handler
	suite.cancel()

	suite.mockDiscovery.AssertExpectations(suite.T())
	suite.mockHost.AssertExpectations(suite.T())
	suite.mockPeerstore.AssertExpectations(suite.T())
	mockStream.AssertExpectations(suite.T())
}

func (suite *HostTestSuite) TestNewWithMDNS() {
	ctx := context.Background()
	logger := zap.NewNop()

	cfg := config.Network{
		ListenPort:        8080,
		MaxOutboundDegree: 10,
		HeartbeatInterval: 30 * time.Second,
		ConnectTimeout:    5 * time.Second,
		DiscoveryConfig: config.Discovery{
			DiscoveryType: "mdns",
			ProtocolID:    "committee-sampling",
			Interval:      10 * time.Second,
			ServiceTag:    "committee-sampling-mdns",
		},
	}

	node, err := New(ctx, cfg, logger)
	suite.NoError(err)
	suite.NotNil(node)

	// Verify node properties
	suite.NotNil(node.host)
	suite.NotNil(node.ctx)
	suite.NotNil(node.cancel)
	suite.NotNil(node.logger)
	suite.NotNil(node.discovery)
	suite.NotNil(node.key)
	suite.Equal(cfg.MaxOutboundDegree, node.maxOutbound)
	suite.Equal(cfg.HeartbeatInterval, node.heartbeatInterval)
	suite.Equal(0, node.numOfNeighbors)

	// Verify node ID is valid
	nodeID := node.GetNodeID()
	suite.NotEmpty(nodeID)

	// Verify discovery is working by checking the channel exists
	discoveredPeers := node.discovery.DiscoveredPeers()
	suite.NotNil(discoveredPeers)

	// Clean up
	err = node.Close()
	suite.NoError(err)
}

func TestHostSuite(t *testing.T) {
	suite.Run(t, new(HostTestSuite))
}
