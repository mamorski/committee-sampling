package network

import (
	"time"

	"github.com/mamorski/committee-sampling/internal/network/discovery"
)

type MessageHandler func(from string, payload []byte) error

type Config struct {
	ListenPort        int
	MaxOutboundDegree int
	HeartbeatInterval time.Duration
	ConnectTimeout    time.Duration
	DiscoveryConfig   discovery.Config
}
