package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
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
	cpuprofile := flag.String("cpuprofile", "", "write CPU profile to this file (empty = off)")
	memprofile := flag.String("memprofile", "", "write heap profile to this file at shutdown (empty = off)")
	blockprofile := flag.String("blockprofile", "", "write goroutine blocking profile to this file at shutdown (empty = off)")
	mutexprofile := flag.String("mutexprofile", "", "write mutex contention profile to this file at shutdown (empty = off)")
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	logger := createLogger(cfg)

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			logger.Fatal("could not create CPU profile", zap.Error(err))
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			logger.Fatal("could not start CPU profile", zap.Error(err))
		}
		// Order matters: defers run LIFO, so f.Close() is registered first to
		// ensure StopCPUProfile() flushes the profile before the file is closed.
		defer f.Close()
		defer pprof.StopCPUProfile()
		logger.Info("CPU profiling enabled", zap.String("file", *cpuprofile))
	}

	if *memprofile != "" {
		defer func() {
			f, err := os.Create(*memprofile)
			if err != nil {
				logger.Error("could not create heap profile", zap.Error(err))
				return
			}
			defer f.Close()
			runtime.GC() // get up-to-date statistics
			if err := pprof.WriteHeapProfile(f); err != nil {
				logger.Error("could not write heap profile", zap.Error(err))
			}
		}()
	}

	if *blockprofile != "" {
		// Record every blocking event; adds overhead, so only enable on sampled nodes.
		runtime.SetBlockProfileRate(1)
		defer writeNamedProfile(logger, "block", *blockprofile)
		logger.Info("Block profiling enabled", zap.String("file", *blockprofile))
	}

	if *mutexprofile != "" {
		// Report all mutex contention events; adds overhead, sample nodes only.
		runtime.SetMutexProfileFraction(1)
		defer writeNamedProfile(logger, "mutex", *mutexprofile)
		logger.Info("Mutex profiling enabled", zap.String("file", *mutexprofile))
	}

	gomaxprocs := cfg.Runtime.GOMAXPROCS
	if gomaxprocs == 0 {
		gomaxprocs = 2 // default cap; set a negative value in config for all host cores
	}
	if gomaxprocs > 0 {
		runtime.GOMAXPROCS(gomaxprocs)
		logger.Info("Pinned GOMAXPROCS", zap.Int("gomaxprocs", gomaxprocs))
	}

	// Create a synchronizer instance
	sync, err := synchronizer.New(ctx, cfg, logger)
	if err != nil {
		panic(err)
	}

	// Start the synchronizer
	sync.Start()

	node, err := network.New(ctx, cfg, logger, sync, cfg.Committee.SessionID)
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

// writeNamedProfile dumps a runtime/pprof named profile (e.g. "block", "mutex")
// to path. Intended to run at shutdown via defer.
func writeNamedProfile(logger *zap.Logger, name, path string) {
	f, err := os.Create(path)
	if err != nil {
		logger.Error("could not create profile", zap.String("profile", name), zap.Error(err))
		return
	}
	defer f.Close()
	if err := pprof.Lookup(name).WriteTo(f, 0); err != nil {
		logger.Error("could not write profile", zap.String("profile", name), zap.Error(err))
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
