package discovery

import (
	"context"
	"errors"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"

	"github.com/mamorski/committee-sampling/pkg/config"
)

type DHTDiscovery struct {
	host            host.Host
	dht             *dht.IpfsDHT
	config          config.Discovery
	discoveredPeers chan peer.AddrInfo
	ctx             context.Context
	cancel          context.CancelFunc
	logger          *zap.Logger
}

func NewDHTDiscovery(h host.Host, config config.Discovery, logger *zap.Logger) *DHTDiscovery {
	ctx, cancel := context.WithCancel(context.Background())
	return &DHTDiscovery{
		host:            h,
		config:          config,
		discoveredPeers: make(chan peer.AddrInfo),
		ctx:             ctx,
		cancel:          cancel,
		logger:          logger.Named("dht-discovery"),
	}
}

func (d *DHTDiscovery) Start(ctx context.Context) error {
	var err error
	d.dht, err = dht.New(ctx, d.host)
	if err != nil {
		d.logger.Error("Failed to create DHT instance", zap.Error(err))
		return err
	}

	d.logger.Info(
		"Starting DHT discovery",
		zap.String("protocol_id", d.config.ProtocolID),
		zap.Strings("bootstrap_peers", d.config.BootstrapPeers),
	)

	// Connect to bootstrap peers with proper error handling
	connectedBootstrapPeers := 0
	for i, addr := range d.config.BootstrapPeers {
		d.logger.Debug(
			"Attempting to connect to bootstrap peer",
			zap.Int("peer_index", i),
			zap.String("address", addr),
		)

		var a multiaddr.Multiaddr
		a, err = multiaddr.NewMultiaddr(addr)
		if err != nil {
			d.logger.Error(
				"Failed to parse bootstrap peer address",
				zap.String("address", addr),
				zap.Error(err),
			)
			continue
		}

		var p2pAddr *peer.AddrInfo
		p2pAddr, err = peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			d.logger.Error(
				"Failed to create peer address info",
				zap.String("address", addr),
				zap.Error(err),
			)
			continue
		}

		if err = d.host.Connect(ctx, *p2pAddr); err != nil {
			d.logger.Error(
				"Failed to connect to bootstrap peer",
				zap.String("address", addr),
				zap.String("peer_id", p2pAddr.ID.String()),
				zap.Error(err),
			)
			continue
		}

		connectedBootstrapPeers++
		d.logger.Info(
			"Successfully connected to bootstrap peer",
			zap.String("address", addr),
			zap.String("peer_id", p2pAddr.ID.String()),
		)
	}

	if connectedBootstrapPeers == 0 {
		d.logger.Error("Failed to connect to any bootstrap peers - DHT may not work properly")
	} else {
		d.logger.Info(
			"Connected to bootstrap peers",
			zap.Int("connected_count", connectedBootstrapPeers),
			zap.Int("total_count", len(d.config.BootstrapPeers)),
		)
	}

	if err = d.dht.Bootstrap(ctx); err != nil {
		d.logger.Error("Failed to bootstrap DHT", zap.Error(err))
		return err
	}

	d.logger.Info("DHT bootstrap completed successfully")

	// Create a routing discovery instance
	routingDiscovery := routing.NewRoutingDiscovery(d.dht)

	// Retry advertisement with exponential backoff
	err = d.advertise(ctx, routingDiscovery)
	if err != nil {
		d.logger.Error("Failed to advertise on DHT", zap.Error(err))
		return err
	}

	// Start discovering peers
	go d.discoverPeers(ctx, routingDiscovery)

	return nil
}

func (d *DHTDiscovery) advertise(ctx context.Context, routingDiscovery *routing.RoutingDiscovery) error {

	for i := 0; i < 3; i++ {
		_, err := routingDiscovery.Advertise(ctx, d.config.ProtocolID, discovery.TTL(24*time.Hour))
		if err != nil {
			d.logger.Warn(
				"DHT advertisement attempt failed",
				zap.Int("attempt", i),
				zap.Error(err),
			)
			time.Sleep(time.Duration(i+1) * time.Second) // Exponential backoff
			continue
		}

		d.logger.Debug(
			"Successfully advertised on DHT",
			zap.String("protocol_id", d.config.ProtocolID),
			zap.Int("attempt", i),
		)
		return nil
	}

	return errors.New("failed to advertise on DHT after multiple attempts")
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

	d.logger.Info("Starting peer discovery loop", zap.Duration("interval", d.config.Interval))

	for {
		select {
		case <-ctx.Done():
			d.logger.Info("Stopping peer discovery loop")
			return
		case <-ticker.C:
			d.logger.Debug("Searching for peers...")
			peers, err := routingDiscovery.FindPeers(ctx, d.config.ProtocolID, discovery.Limit(1000))
			if err != nil {
				d.logger.Error("Failed to find peers", zap.Error(err))
				continue
			}

			peerCount := 0
			for p := range peers {
				// Skip self
				if p.ID == d.host.ID() {
					continue
				}

				d.logger.Debug(
					"Discovered peer",
					zap.String("peer_id", p.ID.String()),
					zap.Strings("addresses", addrsToStrings(p.Addrs)),
				)

				select {
				case d.discoveredPeers <- p:
					peerCount++
				case <-ctx.Done():
					return
				}
			}

			if peerCount > 0 {
				d.logger.Debug("Found peers in discovery round", zap.Int("peer_count", peerCount))
			} else {
				d.logger.Debug("No new peers found in this discovery round")
			}
		}
	}
}

func addrsToStrings(addrs []multiaddr.Multiaddr) []string {
	result := make([]string, len(addrs))
	for i, addr := range addrs {
		result[i] = addr.String()
	}
	return result
}
