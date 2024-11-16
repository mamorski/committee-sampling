package main

import (
	"context"
	"fmt"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"sync"
	"time"

	"github.com/mamorski/committee-sampling/internal/p2pchat"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	nodeCount := 5
	for i := 0; i < nodeCount; i++ {
		wg.Add(1)
		go func(nodeIndex int) {
			defer wg.Done()

			notifier, err := p2pchat.CreateHost(ctx)
			if err != nil {
				fmt.Printf("Node %d: failed to create host: %s\n", nodeIndex, err)
				return
			}

			err = notifier.StartDHT(ctx, dht.DefaultBootstrapPeers)
			if err != nil {
				fmt.Printf("Node %d: failed to set up DHT: %s\n", nodeIndex, err)
				return
			}

			notifier.SetStreamHandler(notifier.Host)
			p2pchat.SendPeriodicMessages(ctx, notifier.Host, &notifier.Neighbors)
			fmt.Printf("Node %d: host down\n", nodeIndex)
		}(i)
		fmt.Printf("Node %d started\n", i)
	}

	time.Sleep(120 * time.Second)
	fmt.Println("#########################")
	fmt.Println("Shutting down")
	fmt.Println("#########################")
	cancel()
	wg.Wait()
}
