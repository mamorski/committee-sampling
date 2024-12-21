package discovery

import (
	"fmt"

	"github.com/libp2p/go-libp2p/core/host"
)

func NewDiscovery(host host.Host, config Config) (PeerDiscovery, error) {
	switch config.DiscoveryType {
	case "dht":
		return NewDHTDiscovery(host, config), nil
	case "mdns":
		return NewMDNSDiscovery(host, config), nil
	default:
		return nil, fmt.Errorf("unknown discovery type: %s", config.DiscoveryType)
	}
}
