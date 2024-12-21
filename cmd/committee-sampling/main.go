package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	p2pnode "github.com/mamorski/committee-sampling/internal/network"
	"github.com/mamorski/committee-sampling/internal/network/discovery"

	"go.uber.org/zap"
)

// TODO: Testing with main, replace when implemented all protocols
func main() {
	logger, _ := zap.NewDevelopment()
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logger)

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	conf := p2pnode.Config{
		ListenPort:        0,
		MaxOutboundDegree: 2,
		HeartbeatInterval: 0,
		ConnectTimeout:    0,
		DiscoveryConfig: discovery.Config{
			BootstrapPeers: []string{},
			DiscoveryType:  "mdns",
			Interval:       0,
			ProtocolID:     "committee-sampling",
			ServiceTag:     "committee-sampling",
		},
	}

	nodeCount := 5
	for i := 0; i < nodeCount; i++ {

		wg.Add(1)
		go func(nodeIndex int) {
			n := createNode(ctx, conf, logger)
			n.RegisterHandler("/committee-sampling/test", messageHandler)
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				case <-ticker.C:
					message := fmt.Sprintf("Hello from node %d", nodeIndex)
					n.SendProtocolMessage("/committee-sampling/test", []byte(message))
				}
			}
		}(i)
		fmt.Printf("Node %d started\n", i)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	cancel()
	wg.Wait()
}

func createNode(ctx context.Context, conf p2pnode.Config, logger *zap.Logger) *p2pnode.P2PNode {
	n, _ := p2pnode.NewP2PNode(ctx, conf, logger)
	return n
}

func messageHandler(from string, payload []byte) error {
	fmt.Printf("Received message from %s: %s\n", from, string(payload))
	return nil
}
