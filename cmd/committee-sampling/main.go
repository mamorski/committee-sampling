package main

import (
	"context"

	"github.com/mamorski/committee-sampling/internal/boot"
	"github.com/mamorski/committee-sampling/internal/network"
	"github.com/mamorski/committee-sampling/pkg/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}

	logger := createLogger(cfg)
	node, err := network.New(context.Background(), cfg.Network, logger)
	if err != nil {
		panic(err)
	}

	b, err := boot.New(cfg, node, logger)
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
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
	}

	c.EncoderConfig = encoderCfg

	return zap.Must(c.Build())
}
