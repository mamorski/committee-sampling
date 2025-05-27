package discovery

import (
	"context"
	"fmt"

	"github.com/libp2p/go-libp2p/core/connmgr"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
)

type Host interface {
	ID() peer.ID
	Close() error
	Connect(ctx context.Context, addrInfo peer.AddrInfo) error
	Peerstore() peerstore.Peerstore
	Addrs() []multiaddr.Multiaddr
	Network() network.Network
	Mux() protocol.Switch
	SetStreamHandler(pid protocol.ID, handler network.StreamHandler)
	SetStreamHandlerMatch(id protocol.ID, f func(protocol.ID) bool, handler network.StreamHandler)
	RemoveStreamHandler(pid protocol.ID)
	NewStream(ctx context.Context, p peer.ID, pids ...protocol.ID) (network.Stream, error)
	ConnManager() connmgr.ConnManager
	EventBus() event.Bus
}

func NewDiscovery(host Host, config Config) (PeerDiscovery, error) {
	switch config.DiscoveryType {
	case "dht":
		return NewDHTDiscovery(host, config), nil
	case "mdns":
		return NewMDNSDiscovery(host, config), nil
	default:
		return nil, fmt.Errorf("unknown discovery type: %s", config.DiscoveryType)
	}
}
