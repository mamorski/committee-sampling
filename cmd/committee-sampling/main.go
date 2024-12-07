package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	p2pnode "github.com/mamorski/committee-sampling/internal/network"
	"github.com/mamorski/committee-sampling/pkg/config"
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
	conf := config.Network{MaxNeighbors: 2}
	rand.Seed(time.Now().UnixNano())
	//messageTypes := []p2pnode.MessageType{p2pnode.MDAG, p2pnode.ExAnte, p2pnode.ExPost}

	nodeCount := 5
	for i := 0; i < nodeCount; i++ {

		wg.Add(1)
		go func(nodeIndex int) {
			n := createNode(ctx, conf, logger)
			ticker := time.NewTicker(5 * time.Second)
			ch := n.ReceiveMessages(p2pnode.MDAG)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					wg.Done()
					return
				case data := <-ch:
					fmt.Printf("Node %d received message: %s\n", nodeIndex, string(data))
				case <-ticker.C:
					message := p2pnode.Message{
						Type: p2pnode.MDAG,
						Data: []byte(fmt.Sprintf("Hello from node %d", nodeIndex)),
					}
					//}
					//	n.SendMessageToPeers(message, n.GetPeers())
					//	fmt.Println("Sending message", message)
					//	message := p2pnode.Message{Type: m, Data: []byte("Hello")}
					//for _, m := range messageTypes {
					n.SendMessageToPeers(message, n.GetPeers())
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

func createNode(ctx context.Context, conf config.Network, logger *zap.Logger) *p2pnode.Node {
	return p2pnode.New(ctx, conf, logger)
}
