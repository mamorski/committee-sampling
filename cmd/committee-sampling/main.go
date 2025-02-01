package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mamorski/committee-sampling/internal/mdag"
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
			fmt.Println("Creating node", nodeIndex)
			n, _ := p2pnode.NewP2PNode(ctx, conf, logger)
			time.Sleep(10 * time.Second)
			m := mdag.New(10, oracle, n, 5*time.Second, logger)
			sid := "test"
			vki := "test"
			vi := []string{"test"}
			labels, err := m.Gen(sid, vki, vi...)
			if err != nil {
				fmt.Println("Error generating labels")
				fmt.Println(err)
			} else {
				for _, l := range labels {
					// Concatenate the sorted labels.
					var concatenated []byte
					for _, lab := range l {
						concatenated = append(concatenated, lab...)
					}
					fmt.Println(string(concatenated))
				}
			}
			wg.Done()
		}(i)
		fmt.Printf("Node %d started\n", i)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	cancel()
	wg.Wait()
}

//func createNode(ctx context.Context, conf p2pnode.Config, logger *zap.Logger) *p2pnode.P2PNode {
//	n, _ := p2pnode.NewP2PNode(ctx, conf, logger)
//	return n
//}
//
//func messageHandler(from string, payload []byte) error {
//	fmt.Printf("Received message from %s: %s\n", from, string(payload))
//	return nil
//}

func oracle(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}
