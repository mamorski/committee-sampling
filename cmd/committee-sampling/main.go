package main

import (
	"context"
	"fmt"
	"github.com/mamorski/committee-sampling/internal/network"
	"time"
)

func main() {
	fmt.Println("Hello, Go project!")
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		go network.Run(ctx)
	}

	fmt.Printf("Started at %s\n", time.Now())
	time.Sleep(5 * time.Minute)
	ctx.Done()
}
