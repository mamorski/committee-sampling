package discovery

import (
	"context"

	"github.com/libp2p/go-libp2p/core/peer"
)

type PeerDiscovery interface {
	// Start starts the discovery service
	Start(ctx context.Context) error

	// Stop stops the discovery service
	Stop() error

	// DiscoveredPeers returns a channel that receives newly discovered peers
	DiscoveredPeers() <-chan peer.AddrInfo

	ClosestPeers(target peer.ID) ([]peer.ID, error)
}
