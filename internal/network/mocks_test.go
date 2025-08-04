package network

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/mock"
)

type MockHost struct {
	mock.Mock
}

func (m *MockHost) ID() peer.ID {
	args := m.Called()
	return args.Get(0).(peer.ID)
}

func (m *MockHost) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockHost) SetStreamHandler(proto protocol.ID, handler network.StreamHandler) {
	m.Called(proto, handler)
}

func (m *MockHost) Connect(ctx context.Context, addrInfo peer.AddrInfo) error {
	args := m.Called(ctx, addrInfo)
	return args.Error(0)
}

func (m *MockHost) NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error) {
	args := m.Called(ctx, p, pids)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(network.Stream), args.Error(1)
}

func (m *MockHost) Peerstore() peerstore.Peerstore {
	args := m.Called()
	return args.Get(0).(peerstore.Peerstore)
}

type MockPeerstore struct {
	mock.Mock
}

func (m *MockPeerstore) PrivKey(id peer.ID) crypto.PrivKey {
	args := m.Called(id)
	return args.Get(0).(crypto.PrivKey)
}

func (m *MockPeerstore) PubKey(id peer.ID) crypto.PubKey {
	args := m.Called(id)
	return args.Get(0).(crypto.PubKey)
}

func (m *MockPeerstore) AddAddr(p peer.ID, addr multiaddr.Multiaddr, ttl time.Duration) {
	m.Called(p, addr, ttl)
}

func (m *MockPeerstore) AddAddrs(p peer.ID, addrs []multiaddr.Multiaddr, ttl time.Duration) {
	m.Called(p, addrs, ttl)
}

func (m *MockPeerstore) SetAddr(p peer.ID, addr multiaddr.Multiaddr, ttl time.Duration) {
	m.Called(p, addr, ttl)
}

func (m *MockPeerstore) SetAddrs(p peer.ID, addrs []multiaddr.Multiaddr, ttl time.Duration) {
	m.Called(p, addrs, ttl)
}

func (m *MockPeerstore) UpdateAddrs(p peer.ID, oldTTL time.Duration, newTTL time.Duration) {
	m.Called(p, oldTTL, newTTL)
}

func (m *MockPeerstore) Addrs(p peer.ID) []multiaddr.Multiaddr {
	args := m.Called(p)
	return args.Get(0).([]multiaddr.Multiaddr)
}

func (m *MockPeerstore) AddrStream(ctx context.Context, p peer.ID) <-chan multiaddr.Multiaddr {
	args := m.Called(ctx, p)
	return args.Get(0).(<-chan multiaddr.Multiaddr)
}

func (m *MockPeerstore) ClearAddrs(p peer.ID) {
	m.Called(p)
}

func (m *MockPeerstore) PeersWithAddrs() peer.IDSlice {
	args := m.Called()
	return args.Get(0).(peer.IDSlice)
}

func (m *MockPeerstore) AddPubKey(p peer.ID, pk crypto.PubKey) error {
	args := m.Called(p, pk)
	return args.Error(0)
}

func (m *MockPeerstore) AddPrivKey(p peer.ID, sk crypto.PrivKey) error {
	args := m.Called(p, sk)
	return args.Error(0)
}

func (m *MockPeerstore) SetPubKey(p peer.ID, pk crypto.PubKey) error {
	args := m.Called(p, pk)
	return args.Error(0)
}

func (m *MockPeerstore) SetPrivKey(p peer.ID, sk crypto.PrivKey) error {
	args := m.Called(p, sk)
	return args.Error(0)
}

func (m *MockPeerstore) PeersWithKeys() peer.IDSlice {
	args := m.Called()
	return args.Get(0).(peer.IDSlice)
}

func (m *MockPeerstore) Get(p peer.ID, key string) (any, error) {
	args := m.Called(p, key)
	return args.Get(0), args.Error(1)
}

func (m *MockPeerstore) Put(p peer.ID, key string, val any) error {
	args := m.Called(p, key, val)
	return args.Error(0)
}

func (m *MockPeerstore) GetProtocols(p peer.ID) ([]protocol.ID, error) {
	args := m.Called(p)
	return args.Get(0).([]protocol.ID), args.Error(1)
}

func (m *MockPeerstore) AddProtocols(p peer.ID, protos ...protocol.ID) error {
	args := m.Called(p, protos)
	return args.Error(0)
}

func (m *MockPeerstore) SetProtocols(p peer.ID, protos ...protocol.ID) error {
	args := m.Called(p, protos)
	return args.Error(0)
}

func (m *MockPeerstore) RemoveProtocols(p peer.ID, protos ...protocol.ID) error {
	args := m.Called(p, protos)
	return args.Error(0)
}

func (m *MockPeerstore) SupportsProtocols(p peer.ID, protos ...protocol.ID) ([]protocol.ID, error) {
	args := m.Called(p, protos)
	return args.Get(0).([]protocol.ID), args.Error(1)
}

func (m *MockPeerstore) FirstSupportedProtocol(p peer.ID, protos ...protocol.ID) (protocol.ID, error) {
	args := m.Called(p, protos)
	return args.Get(0).(protocol.ID), args.Error(1)
}

func (m *MockPeerstore) RemovePeer(p peer.ID) {
	m.Called(p)
}

func (m *MockPeerstore) Peers() peer.IDSlice {
	args := m.Called()
	return args.Get(0).(peer.IDSlice)
}

func (m *MockPeerstore) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockPeerstore) RecordLatency(p peer.ID, latency time.Duration) {
	m.Called(p, latency)
}

func (m *MockPeerstore) LatencyEWMA(p peer.ID) time.Duration {
	args := m.Called(p)
	return args.Get(0).(time.Duration)
}

func (m *MockPeerstore) PeerInfo(p peer.ID) peer.AddrInfo {
	args := m.Called(p)
	return args.Get(0).(peer.AddrInfo)
}

type MockStream struct {
	mock.Mock
}

func (m *MockStream) Read(p []byte) (n int, err error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

func (m *MockStream) Write(p []byte) (n int, err error) {
	args := m.Called(p)
	return args.Int(0), args.Error(1)
}

func (m *MockStream) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStream) CloseRead() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStream) CloseWrite() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStream) Reset() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockStream) SetDeadline(t time.Time) error {
	args := m.Called(t)
	return args.Error(0)
}

func (m *MockStream) SetReadDeadline(t time.Time) error {
	args := m.Called(t)
	return args.Error(0)
}

func (m *MockStream) SetWriteDeadline(t time.Time) error {
	args := m.Called(t)
	return args.Error(0)
}

func (m *MockStream) ID() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockStream) Protocol() protocol.ID {
	args := m.Called()
	return args.Get(0).(protocol.ID)
}

func (m *MockStream) SetProtocol(id protocol.ID) error {
	args := m.Called(id)
	return args.Error(0)
}

func (m *MockStream) Stat() network.Stats {
	args := m.Called()
	return args.Get(0).(network.Stats)
}

func (m *MockStream) Conn() network.Conn {
	args := m.Called()
	return args.Get(0).(network.Conn)
}

func (m *MockStream) Scope() network.StreamScope {
	args := m.Called()
	return args.Get(0).(network.StreamScope)
}

type MockDiscovery struct {
	mock.Mock
	ch chan peer.AddrInfo
}

func (m *MockDiscovery) Start(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockDiscovery) Stop() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockDiscovery) DiscoveredPeers() <-chan peer.AddrInfo {
	args := m.Called()
	return args.Get(0).(<-chan peer.AddrInfo)
}
