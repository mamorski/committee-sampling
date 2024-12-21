package discovery

import (
	"context"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/multiformats/go-multiaddr"
)

type DHTDiscovery struct {
	host            host.Host
	dht             *dht.IpfsDHT
	config          Config
	discoveredPeers chan peer.AddrInfo
	ctx             context.Context
	cancel          context.CancelFunc
}

func NewDHTDiscovery(host host.Host, config Config) *DHTDiscovery {
	ctx, cancel := context.WithCancel(context.Background())
	return &DHTDiscovery{
		host:            host,
		config:          config,
		discoveredPeers: make(chan peer.AddrInfo),
		ctx:             ctx,
		cancel:          cancel,
	}
}

func (d *DHTDiscovery) Start(ctx context.Context) error {
	var err error
	d.dht, err = dht.New(ctx, d.host)
	if err != nil {
		return err
	}

	// Connect to bootstrap peers
	for _, addr := range d.config.BootstrapPeers {
		// TODO: Add error handling
		a, _ := multiaddr.NewMultiaddr(addr)
		p2pAddr, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			continue
		}
		if err := d.host.Connect(ctx, *p2pAddr); err != nil {
			continue
		}
	}

	if err := d.dht.Bootstrap(ctx); err != nil {
		return err
	}

	// Create a routing discovery instance
	routingDiscovery := routing.NewRoutingDiscovery(d.dht)
	_, _ = routingDiscovery.Advertise(ctx, d.config.ProtocolID)

	// Start discovering peers
	go d.discoverPeers(ctx, routingDiscovery)

	return nil
}

func (d *DHTDiscovery) Stop() error {
	d.cancel()
	return d.dht.Close()
}

func (d *DHTDiscovery) DiscoveredPeers() <-chan peer.AddrInfo {
	return d.discoveredPeers
}

func (d *DHTDiscovery) discoverPeers(ctx context.Context, routingDiscovery *routing.RoutingDiscovery) {
	ticker := time.NewTicker(d.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			peers, err := routingDiscovery.FindPeers(ctx, d.config.ProtocolID)
			if err != nil {
				continue
			}

			for p := range peers {
				// Skip self
				if p.ID == d.host.ID() {
					continue
				}
				select {
				case d.discoveredPeers <- p:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}
