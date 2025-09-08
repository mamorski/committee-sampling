package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"

	"github.com/mamorski/committee-sampling/internal/boot"
	"github.com/mamorski/committee-sampling/internal/metrics"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/mamorski/committee-sampling/internal/synchronizer"
	"github.com/mamorski/committee-sampling/pkg/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	configPath := flag.String("config", "", "path to config file (optional, defaults to ./configs/{env}.json)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	logger := createLogger(cfg)

	// Create a synchronizer instance
	sync, err := synchronizer.New(ctx, cfg, logger)
	if err != nil {
		panic(err)
	}

	// Start the synchronizer
	sync.Start()

	node, err := network.New(ctx, cfg.Network, logger, sync, cfg.Committee.SessionID)
	if err != nil {
		panic(err)
	}

	nodeID := node.GetNodeID()
	logger = logger.With(zap.String("node_id", nodeID))
	logger.Info("Starting node with id")

	// Initialize metrics collector
	metricsCollector, err := metrics.New(ctx, &cfg.Metrics, logger, nodeID, cfg.Committee.SessionID)
	if err != nil {
		panic(err)
	}

	// Start a metrics collection
	err = metricsCollector.Start()
	if err != nil {
		panic(err)
	}

	b, err := boot.New(ctx, cfg, node, logger, sync)
	if err != nil {
		panic(err)
	}

	// Set up a graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// Run the committee sampling protocol in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- b.Run()
	}()

	// Wait for either completion or shutdown signal
	select {
	case err := <-errChan:
		if err != nil {
			logger.Error("Committee sampling protocol failed", zap.Error(err))
		} else {
			logger.Info("Committee sampling protocol completed successfully")
		}
	case sig := <-signalChan:
		logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
	}

	// Graceful shutdown
	logger.Info("Shutting down...")
	if err := metricsCollector.Stop(); err != nil {
		logger.Error("Failed to stop metrics collector", zap.Error(err))
	}

	if err := node.Close(); err != nil {
		logger.Error("Failed to close network node", zap.Error(err))
	}
}

func createLogger(cfg *config.Config) *zap.Logger {
	c := zap.NewProductionConfig()
	level, err := zapcore.ParseLevel(cfg.Logger.Level)
	if err != nil {
		panic(err)
	}
	c.Level = zap.NewAtomicLevelAt(level)

	encoderCfg := zapcore.EncoderConfig{
		TimeKey:        "timestamp", // JSON key or console field name
		LevelKey:       "level",
		MessageKey:     "msg",
		CallerKey:      "caller",
		NameKey:        "logger",
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}

	c.EncoderConfig = encoderCfg

	return zap.Must(c.Build())
}
