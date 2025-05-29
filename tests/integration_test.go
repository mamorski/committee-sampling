package tests

import (
	"sync"
	"testing"
	"time"

	"github.com/mamorski/committee-sampling/internal/boot"
	"github.com/mamorski/committee-sampling/internal/gce"
	"github.com/mamorski/committee-sampling/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// TestNetworkInterface verifies that MockNetwork implements the Network interface
func TestNetworkInterface(t *testing.T) {
	cluster := NewMockNetworkCluster(0)
	err := cluster.ConnectRandom(4)
	if err != nil {
		t.Fatalf("Failed to connect random peers: %v", err)
	}

	cfg, err := config.Load("../configs/stg.json")
	if err != nil {
		t.Fatal("Failed to load configuration")
	}

	c := zap.NewProductionConfig()
	// set to DebugLevel, InfoLevel, WarnLevel, ErrorLevel, etc.
	c.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)

	logger, err := c.Build()
	if err != nil {
		panic(err)
	}
	defer func(logger *zap.Logger) {
		_ = logger.Sync()
	}(logger)

	var wg sync.WaitGroup
	logger.Info("Starting GCE Committee Election Test",
		zap.Int("nodes", len(cluster.networks)),
	)
	wg.Add(len(cluster.networks))

	cfg.RunTime.StartTime = time.Now().UTC().Add(1 * time.Minute).Unix()
	logger.Info("Configuration loaded",
		zap.Any("cfg", cfg),
	)
	for i, node := range cluster.networks {
		go func() {
			defer wg.Done()
			b, err := boot.New(cfg, node, logger)
			e := gce.New(logger)

			if err != nil {
				t.Errorf("Failed to create new boot instance: %v", err)
				return
			}

			state, err := e.Initialize(
				node.GetNodeID(),
				cfg.RunTime.SessionID,
				b.VRF,
				b.RbExp,
				b.VDF,
				20,
				cfg.RunTime.Lambda,
			)
			if err != nil {
				t.Errorf("Failed to initialize GCE: %v", err)
				return
			}

			committee, err := e.CommitteeElection(cfg.RunTime.SessionID, state, cfg.RunTime.Weight, b.VRF, b.RbExp)
			if err != nil {
				t.Errorf("Failed to perform committee election: %v", err)
				return
			}

			if len(committee) == 0 {
				t.Errorf("Node %d Committee election returned an empty committee", i*1000)
				return
			}

			t.Logf("Node %d elected committee: %v", i*1000, committee)
		}()
	}

	wg.Wait()
	for _, n := range cluster.networks {
		_ = n.Close()
	}

}
