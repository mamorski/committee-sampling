package discovery

import (
	"context"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/mamorski/committee-sampling/pkg/config"
	"github.com/multiformats/go-multiaddr"
	"go.uber.org/zap"
)

type DHTDiscovery struct {
	host            Host
	dht             *dht.IpfsDHT
	config          config.Discovery
	discoveredPeers chan peer.AddrInfo
	ctx             context.Context
	cancel          context.CancelFunc
	logger          *zap.Logger
}

func NewDHTDiscovery(host Host, config config.Discovery, logger *zap.Logger) *DHTDiscovery {
	ctx, cancel := context.WithCancel(context.Background())
	return &DHTDiscovery{
		host:            host,
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
		return err
	}

	d.logger.Info("Starting DHT discovery",
		zap.String("protocol_id", d.config.ProtocolID),
		zap.Strings("bootstrap_peers", d.config.BootstrapPeers),
	)

	// Connect to bootstrap peers with proper error handling
	connectedBootstrapPeers := 0
	for i, addr := range d.config.BootstrapPeers {
		d.logger.Debug("Attempting to connect to bootstrap peer",
			zap.Int("peer_index", i),
			zap.String("address", addr),
		)

		a, err := multiaddr.NewMultiaddr(addr)
		if err != nil {
			d.logger.Error("Failed to parse bootstrap peer address",
				zap.String("address", addr),
				zap.Error(err),
			)
			continue
		}

		p2pAddr, err := peer.AddrInfoFromP2pAddr(a)
		if err != nil {
			d.logger.Error("Failed to create peer address info",
				zap.String("address", addr),
				zap.Error(err),
			)
			continue
		}

		if err := d.host.Connect(ctx, *p2pAddr); err != nil {
			d.logger.Error("Failed to connect to bootstrap peer",
				zap.String("address", addr),
				zap.String("peer_id", p2pAddr.ID.String()),
				zap.Error(err),
			)
			continue
		}

		connectedBootstrapPeers++
		d.logger.Info("Successfully connected to bootstrap peer",
			zap.String("address", addr),
			zap.String("peer_id", p2pAddr.ID.String()),
		)
	}

	if connectedBootstrapPeers == 0 {
		d.logger.Error("Failed to connect to any bootstrap peers - DHT may not work properly")
	} else {
		d.logger.Info("Connected to bootstrap peers",
			zap.Int("connected_count", connectedBootstrapPeers),
			zap.Int("total_count", len(d.config.BootstrapPeers)),
		)
	}

	if err := d.dht.Bootstrap(ctx); err != nil {
		d.logger.Error("Failed to bootstrap DHT", zap.Error(err))
		return err
	}

	d.logger.Info("DHT bootstrap completed successfully")

	// Create a routing discovery instance
	routingDiscovery := routing.NewRoutingDiscovery(d.dht)

	// Retry advertisement with exponential backoff
	go d.retryAdvertisement(ctx, routingDiscovery)

	// Start discovering peers
	go d.discoverPeers(ctx, routingDiscovery)

	return nil
}

func (d *DHTDiscovery) retryAdvertisement(ctx context.Context, routingDiscovery *routing.RoutingDiscovery) {
	maxRetries := 10
	baseDelay := 2 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Wait before each attempt (including first)
		delay := time.Duration(attempt) * baseDelay
		d.logger.Info("Attempting DHT advertisement",
			zap.Int("attempt", attempt),
			zap.Int("max_retries", maxRetries),
			zap.Duration("delay", delay))

		time.Sleep(delay)

		_, err := routingDiscovery.Advertise(ctx, d.config.ProtocolID)
		if err != nil {
			d.logger.Warn("DHT advertisement attempt failed",
				zap.Int("attempt", attempt),
				zap.Error(err))

			if attempt == maxRetries {
				d.logger.Error("All DHT advertisement attempts failed", zap.Error(err))
				return
			}
			continue
		}

		d.logger.Info("Successfully advertised on DHT",
			zap.String("protocol_id", d.config.ProtocolID),
			zap.Int("attempt", attempt))
		return
	}
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
			peers, err := routingDiscovery.FindPeers(ctx, d.config.ProtocolID)
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

				// d.logger.Debug("Discovered peer",
				// 	zap.String("peer_id", p.ID.String()),
				// 	zap.Strings("addresses", addrsToStrings(p.Addrs)),
				// )

				select {
				case d.discoveredPeers <- p:
					peerCount++
				case <-ctx.Done():
					return
				}
			}

			if peerCount > 0 {
				d.logger.Info("Found peers in discovery round", zap.Int("peer_count", peerCount))
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
