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

// Collector handles prometheus metrics collection and pushing
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

	mc := &Collector{
		cfg:      cfg,
		logger:   logger.Named("metrics"),
		registry: prometheus.DefaultRegisterer.(*prometheus.Registry),
		ctx:      collectorCtx,
		cancel:   cancel,
		nodeID:   nodeID,
		sid:      sid,
	}

	// Set up push gateway if enabled
	if cfg.PushGateway.Enabled {
		if err := mc.setupPushGateway(nodeID); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to setup push gateway: %w", err)
		}
	}

	// Set up HTTP server if enabled
	if cfg.HTTPServer.Enabled {
		if err := mc.setupHTTPServer(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to setup HTTP server: %w", err)
		}
	}

	return mc, nil
}

// setupPushGateway configures the push gateway
func (mc *Collector) setupPushGateway(nodeID string) error {
	if mc.cfg.PushGateway.URL == "" {
		return fmt.Errorf("push gateway URL is required when push gateway is enabled")
	}

	jobName := mc.cfg.JobName
	if jobName == "" {
		jobName = "committee-sampling"
	}

	instanceName := mc.cfg.InstanceName
	if instanceName == "" {
		instanceName = nodeID
	}

	mc.pusher = push.New(mc.cfg.PushGateway.URL, jobName).
		Grouping("instance", instanceName).
		Gatherer(mc.registry)

	// Add basic auth if provided
	if mc.cfg.PushGateway.Username != "" && mc.cfg.PushGateway.Password != "" {
		mc.pusher = mc.pusher.BasicAuth(mc.cfg.PushGateway.Username, mc.cfg.PushGateway.Password)
	}

	mc.logger.Info("Push gateway configured",
		zap.String("url", mc.cfg.PushGateway.URL),
		zap.String("job", jobName),
		zap.String("instance", instanceName),
	)

	return nil
}

// setupHTTPServer configures the HTTP metrics server
func (mc *Collector) setupHTTPServer() error {
	if mc.cfg.HTTPServer.Port == 0 {
		return fmt.Errorf("HTTP server port is required when HTTP server is enabled")
	}

	path := mc.cfg.HTTPServer.Path
	if path == "" {
		path = "/metrics"
	}

	mux := http.NewServeMux()
	mux.Handle(path, promhttp.HandlerFor(mc.registry, promhttp.HandlerOpts{}))

	mc.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", mc.cfg.HTTPServer.Port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	mc.logger.Info("HTTP metrics server configured",
		zap.Int("port", mc.cfg.HTTPServer.Port),
		zap.String("path", path),
	)

	return nil
}

// Start begins the metrics collection
func (mc *Collector) Start() error {
	if !mc.cfg.Enabled {
		return nil
	}

	// Start HTTP server if enabled
	if mc.cfg.HTTPServer.Enabled && mc.server != nil {
		go func() {
			mc.logger.Info("Starting HTTP metrics server",
				zap.String("addr", mc.server.Addr),
			)
			if err := mc.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				mc.logger.Error("HTTP metrics server error", zap.Error(err))
			}
		}()
	}

	// Start the push gateway goroutine if enabled
	if mc.cfg.PushGateway.Enabled && mc.pusher != nil {
		go mc.pushMetricsLoop()
	}

	return nil
}

// pushMetricsLoop pushes metrics to the push gateway at regular intervals
func (mc *Collector) pushMetricsLoop() {
	interval := mc.cfg.PushInterval
	if interval == 0 {
		interval = 30 * time.Second // Default to 30 seconds
	}

	mc.logger.Info("Starting metrics push loop",
		zap.Duration("interval", interval),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Push initial metrics
	if err := mc.pushMetrics(); err != nil {
		mc.logger.Error("Failed to push initial metrics", zap.Error(err))
	}

	for {
		select {
		case <-ticker.C:
			if err := mc.pushMetrics(); err != nil {
				mc.logger.Error("Failed to push metrics", zap.Error(err))
			}
		case <-mc.ctx.Done():
			mc.logger.Info("Stopping metrics push loop")
			// Push final metrics before shutting down
			if err := mc.pushMetrics(); err != nil {
				mc.logger.Error("Failed to push final metrics", zap.Error(err))
			}
			return
		}
	}
}

// pushMetrics pushes current metrics to the push gateway
func (mc *Collector) pushMetrics() error {
	if mc.pusher == nil {
		return fmt.Errorf("pusher not configured")
	}

	mc.logger.Debug("Pushing metrics to gateway")

	if err := mc.pusher.Push(); err != nil {
		return fmt.Errorf("failed to push metrics: %w", err)
	}

	mc.logger.Debug("Successfully pushed metrics")
	return nil
}

// Stop stops the metrics collection
func (mc *Collector) Stop() error {
	if !mc.cfg.Enabled {
		return nil
	}

	mc.logger.Info("Stopping metrics collector")

	// Cancel context to stop push loop
	if mc.cancel != nil {
		mc.cancel()
	}

	// Stop HTTP server
	if mc.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := mc.server.Shutdown(ctx); err != nil {
			mc.logger.Error("Failed to shutdown HTTP server", zap.Error(err))
			return err
		}
	}

	return nil
}

// AddCustomMetric allows adding custom metrics to the registry
func (mc *Collector) AddCustomMetric(collector prometheus.Collector) error {
	if !mc.cfg.Enabled {
		return nil
	}

	return mc.registry.Register(collector)
}

// GetRegistry returns the prometheus registry for direct access
func (mc *Collector) GetRegistry() *prometheus.Registry {
	return mc.registry
}
