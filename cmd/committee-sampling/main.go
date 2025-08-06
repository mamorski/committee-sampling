package main

import (
	"context"
	"flag"

	"github.com/mamorski/committee-sampling/internal/boot"
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

	// Create synchronizer instance
	sync, err := synchronizer.New(ctx, cfg, logger)
	if err != nil {
		panic(err)
	}

	// Start the synchronizer
	sync.Start()

	node, err := network.New(ctx, cfg.Network, logger, sync)
	if err != nil {
		panic(err)
	}

	logger = logger.With(zap.String("node_id", node.GetNodeID()))
	b, err := boot.New(ctx, cfg, node, logger, sync)
	if err != nil {
		panic(err)
	}

	err = b.Run()
	if err != nil {
		panic(err)
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
