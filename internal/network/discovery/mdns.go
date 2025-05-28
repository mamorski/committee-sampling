package discovery

import (
	"context"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/mdns"
	"github.com/mamorski/committee-sampling/pkg/config"
)

type MDNSDiscovery struct {
	host            Host
	mdns            mdns.Service
	config          config.Discovery
	discoveredPeers chan peer.AddrInfo
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewMDNSDiscovery(host Host, config config.Discovery) *MDNSDiscovery {
	ctx, cancel := context.WithCancel(context.Background())
	return &MDNSDiscovery{
		host:            host,
		config:          config,
		discoveredPeers: make(chan peer.AddrInfo),
		ctx:             ctx,
		cancel:          cancel,
	}
}

func (m *MDNSDiscovery) Start(_ context.Context) error {
	m.mdns = mdns.NewMdnsService(m.host, m.config.ServiceTag, &mdnsNotifee{m})
	if err := m.mdns.Start(); err != nil {
		return err
	}
	return nil
}

func (m *MDNSDiscovery) Stop() error {
	m.cancel()
	return nil
}

func (m *MDNSDiscovery) DiscoveredPeers() <-chan peer.AddrInfo {
	return m.discoveredPeers
}

type mdnsNotifee struct {
	md *MDNSDiscovery
}

func (n *mdnsNotifee) HandlePeerFound(pi peer.AddrInfo) {
	select {
	case <-n.md.ctx.Done():
		close(n.md.discoveredPeers)
	case n.md.discoveredPeers <- pi:

	}
}
