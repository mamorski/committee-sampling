package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/mamorski/committee-sampling/pkg/config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	"go.uber.org/zap"
)

// Collector handles Prometheus metrics collection and pushing
type Collector struct {
	cfg      *config.Metrics
	logger   *zap.Logger
	pusher   *push.Pusher
	registry *prometheus.Registry
	server   *http.Server
	ctx      context.Context
	cancel   context.CancelFunc
	nodeID   string
	sid      string
}

// New creates a new MetricsCollector instance
func New(ctx context.Context, cfg *config.Metrics, logger *zap.Logger, nodeID, sid string) (*Collector, error) {

	if !cfg.Enabled {
		logger.Info("Metrics collection is disabled")
		return &Collector{
			cfg:    cfg,
			logger: logger.Named("metrics"),
		}, nil
	}

	collectorCtx, cancel := context.WithCancel(ctx)
	collector := &Collector{
		cfg:      cfg,
		logger:   logger.Named("metrics"),
		registry: prometheus.NewRegistry(),
		ctx:      collectorCtx,
		cancel:   cancel,
		nodeID:   nodeID,
		sid:      sid,
	}

	// Pushgateway setup
	if cfg.PushGateway.Enabled {
		if err := collector.setupPushGateway(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to setup push gateway: %w", err)
		}
	}

	// HTTP server setup
	if cfg.HTTPServer.Enabled {
		if err := collector.setupHTTPServer(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to setup HTTP server: %w", err)
		}
	}

	collector.registerMetrics()

	return collector, nil
}

func (c *Collector) registerMetrics() {
	c.registry.MustRegister(TotalMessages)
	c.registry.MustRegister(ValidMessages)
}

func (c *Collector) setupPushGateway() error {
	if c.cfg.PushGateway.URL == "" {
		return fmt.Errorf("push gateway URL is required when push gateway is enabled")
	}

	c.pusher = push.New(c.cfg.PushGateway.URL, "committee-sampling").
		Gatherer(c.registry)

	// Add basic auth if provided
	if c.cfg.PushGateway.Username != "" && c.cfg.PushGateway.Password != "" {
		c.pusher = c.pusher.BasicAuth(c.cfg.PushGateway.Username, c.cfg.PushGateway.Password)
	}

	c.logger.Info(
		"Push gateway configured", zap.String("url", c.cfg.PushGateway.URL), zap.String("node_id", c.nodeID), zap.String("sid", c.sid),
	)

	return nil
}

func (c *Collector) setupHTTPServer() error {
	if c.cfg.HTTPServer.Port == 0 {
		return fmt.Errorf("HTTP server port is required when HTTP server is enabled")
	}

	path := c.cfg.HTTPServer.Path
	if path == "" {
		path = "/metrics"
	}

	mux := http.NewServeMux()
	mux.Handle(path, promhttp.HandlerFor(c.registry, promhttp.HandlerOpts{}))

	c.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", c.cfg.HTTPServer.Port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	c.logger.Info(
		"HTTP metrics server configured", zap.Int("port", c.cfg.HTTPServer.Port), zap.String("path", path),
	)

	return nil
}

// Start begins the metrics collection
func (c *Collector) Start() error {
	if !c.cfg.Enabled {
		return nil
	}

	// Start HTTP server if enabled
	if c.cfg.HTTPServer.Enabled && c.server != nil {
		go func() {
			c.logger.Info("Starting HTTP metrics server", zap.String("addr", c.server.Addr))
			if err := c.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				c.logger.Error("HTTP metrics server error", zap.Error(err))
			}
		}()
	}

	// Optionally, push periodically (for monitoring mid-simulation)
	if c.cfg.PushGateway.Enabled && c.pusher != nil && c.cfg.PushInterval > 0 {
		go c.pushMetricsLoop()
	}

	return nil
}

// pushMetricsLoop pushes metrics at regular intervals
func (c *Collector) pushMetricsLoop() {
	ticker := time.NewTicker(c.cfg.PushInterval)
	defer ticker.Stop()

	c.logger.Info("Starting metrics push loop", zap.Duration("interval", c.cfg.PushInterval))

	for {
		select {
		case <-ticker.C:
			if err := c.pushMetrics(); err != nil {
				c.logger.Error("Failed to push metrics", zap.Error(err))
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// pushMetrics pushes metrics to the push gateway
func (c *Collector) pushMetrics() error {
	if c.pusher == nil {
		return fmt.Errorf("pusher not configured")
	}

	c.logger.Debug("Pushing metrics to gateway")
	if err := c.pusher.Push(); err != nil {
		return fmt.Errorf("failed to push metrics: %w", err)
	}

	return nil
}

// Stop stops the metrics collection and pushes final metrics
func (c *Collector) Stop() error {
	if !c.cfg.Enabled {
		return nil
	}

	c.logger.Info("Stopping metrics collector")

	// Cancel push loop
	if c.cancel != nil {
		c.cancel()
	}

	// Push final metrics
	if c.cfg.PushGateway.Enabled && c.pusher != nil {
		if err := c.pushMetrics(); err != nil {
			c.logger.Error("Failed to push final metrics", zap.Error(err))
		}

		// Optional: delete metrics after run to avoid stale series
		if c.cfg.PushGateway.DeleteOnStop {
			if err := c.pusher.Delete(); err != nil {
				c.logger.Error("Failed to delete metrics from push gateway", zap.Error(err))
			} else {
				c.logger.Info("Deleted metrics from push gateway")
			}
		}
	}

	// Stop HTTP server
	if c.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := c.server.Shutdown(ctx); err != nil {
			c.logger.Error("Failed to shutdown HTTP server", zap.Error(err))
			return err
		}
	}

	return nil
}
