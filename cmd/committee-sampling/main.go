package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/mamorski/committee-sampling/pkg/config"
)

func main() {
	cfg, err := config.Load("configs/dev.json")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	printConfig(cfg)
}

func printConfig(cfg *config.Config) {
	fmt.Println("=== Committee Sampling Configuration ===")

	configJSON, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		log.Printf("Error marshaling config to JSON: %v", err)
		return
	}

	startTime := time.Unix(cfg.RunTime.StartTime, 0).UTC()

	fmt.Println("Start time:", startTime)

	fmt.Println(string(configJSON))
}
