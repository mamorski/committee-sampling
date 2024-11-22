package main

import (
	"context"
	"fmt"
	p2pnode "github.com/mamorski/committee-sampling/internal/network"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	nodeCount := 5
	for i := 0; i < nodeCount; i++ {
		wg.Add(1)
		go func(nodeIndex int) {
			defer wg.Done()
			p2pnode.Run(ctx)
		}(i)
		fmt.Printf("Node %d started\n", i)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	cancel()
	wg.Wait()
}
