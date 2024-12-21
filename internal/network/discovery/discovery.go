package discovery

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

type PeerDiscovery interface {
	// Start starts the discovery service
	Start(ctx context.Context) error

	// Stop stops the discovery service
	Stop() error

	// DiscoveredPeers returns a channel that receives newly discovered peers
	DiscoveredPeers() <-chan peer.AddrInfo
}

type Config struct {
	DiscoveryType string // "dht" or "mdns"
	ProtocolID    string
	Interval      time.Duration
	// DHT specific config
	BootstrapPeers []string
	// mDNS specific config
	ServiceTag string
}
