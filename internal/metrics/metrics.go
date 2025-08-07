package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/mamorski/committee-sampling/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/push"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
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

	// Set up file export if enabled
	if cfg.FileExport.Enabled {
		if err := mc.setupFileExport(); err != nil {
			cancel()
			return nil, fmt.Errorf("failed to setup file export: %w", err)
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

// setupFileExport configures file export for metrics
func (mc *Collector) setupFileExport() error {
	if mc.cfg.FileExport.Directory == "" {
		return fmt.Errorf("file export directory is required when file export is enabled")
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(mc.cfg.FileExport.Directory, 0755); err != nil {
		return fmt.Errorf("failed to create metrics directory: %w", err)
	}

	format := mc.cfg.FileExport.Format
	if format == "" {
		format = "prometheus" // Default format
	}

	mc.logger.Info("File export configured",
		zap.String("directory", mc.cfg.FileExport.Directory),
		zap.String("format", format),
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

	// Start file export loop if enabled
	if mc.cfg.FileExport.Enabled {
		go mc.exportMetricsLoop()
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

// exportMetricsLoop exports metrics to files at regular intervals
func (mc *Collector) exportMetricsLoop() {
	interval := mc.cfg.PushInterval
	if interval == 0 {
		interval = 30 * time.Second // Default to 30 seconds
	}

	mc.logger.Info("Starting metrics file export loop",
		zap.Duration("interval", interval),
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Export initial metrics
	if err := mc.exportMetricsToFile(); err != nil {
		mc.logger.Error("Failed to export initial metrics", zap.Error(err))
	}

	for {
		select {
		case <-ticker.C:
			if err := mc.exportMetricsToFile(); err != nil {
				mc.logger.Error("Failed to export metrics to file", zap.Error(err))
			}
		case <-mc.ctx.Done():
			mc.logger.Info("Stopping metrics file export loop")
			// Export final metrics before shutting down
			if err := mc.exportMetricsToFile(); err != nil {
				mc.logger.Error("Failed to export final metrics", zap.Error(err))
			}
			return
		}
	}
}

// exportMetricsToFile exports current metrics to a file
func (mc *Collector) exportMetricsToFile() error {
	if mc.cfg.FileExport.Directory == "" {
		return fmt.Errorf("file export directory not configured")
	}

	// Gather metrics from registry
	metricFamilies, err := mc.registry.Gather()
	if err != nil {
		return fmt.Errorf("failed to gather metrics: %w", err)
	}

	format := mc.cfg.FileExport.Format
	if format == "" {
		format = "prometheus"
	}

	var filename string
	var extension string
	switch format {
	case "prometheus":
		extension = "prom"
	case "json":
		extension = "json"
	case "csv":
		extension = "csv"
	default:
		extension = "prom"
		format = "prometheus"
	}

	// Include instance name in filename if available
	instanceName := mc.cfg.InstanceName
	if instanceName == "" {
		instanceName = "node"
	}

	filename = fmt.Sprintf("metrics_%s_%s.%s", mc.nodeID, mc.sid, extension)
	filePath := filepath.Join(mc.cfg.FileExport.Directory, filename)

	// Create and write to file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create metrics file: %w", err)
	}
	defer file.Close()

	switch format {
	case "prometheus":
		return mc.writePrometheusFormat(file, metricFamilies)
	case "json":
		return mc.writeJSONFormat(file, metricFamilies)
	case "csv":
		return mc.writeCSVFormat(file, metricFamilies)
	default:
		return mc.writePrometheusFormat(file, metricFamilies)
	}
}

// writePrometheusFormat writes metrics in Prometheus exposition format
func (mc *Collector) writePrometheusFormat(writer io.Writer, metricFamilies []*dto.MetricFamily) error {
	encoder := expfmt.NewEncoder(writer, expfmt.FmtText)
	for _, mf := range metricFamilies {
		if err := encoder.Encode(mf); err != nil {
			return fmt.Errorf("failed to encode metric family: %w", err)
		}
	}
	mc.logger.Debug("Successfully exported metrics in Prometheus format")
	return nil
}

// writeJSONFormat writes metrics in JSON format (simplified)
func (mc *Collector) writeJSONFormat(writer io.Writer, metricFamilies []*dto.MetricFamily) error {
	// This is a simple JSON export - you might want to customize this based on your needs
	_, err := fmt.Fprintf(writer, "{\n  \"timestamp\": \"%s\",\n  \"metrics\": [\n", time.Now().Format(time.RFC3339))
	if err != nil {
		return err
	}

	for i, mf := range metricFamilies {
		if i > 0 {
			_, err = fmt.Fprintf(writer, ",\n")
			if err != nil {
				return err
			}
		}
		_, err = fmt.Fprintf(writer, "    {\n      \"name\": \"%s\",\n      \"help\": \"%s\",\n      \"type\": \"%s\"\n    }",
			mf.GetName(), mf.GetHelp(), mf.GetType())
		if err != nil {
			return err
		}
	}

	_, err = fmt.Fprintf(writer, "\n  ]\n}")
	if err != nil {
		return err
	}

	mc.logger.Debug("Successfully exported metrics in JSON format")
	return nil
}

// writeCSVFormat writes metrics in CSV format (simplified)
func (mc *Collector) writeCSVFormat(writer io.Writer, metricFamilies []*dto.MetricFamily) error {
	_, err := fmt.Fprintf(writer, "timestamp,metric_name,metric_type,help\n")
	if err != nil {
		return err
	}

	timestamp := time.Now().Format(time.RFC3339)
	for _, mf := range metricFamilies {
		_, err = fmt.Fprintf(writer, "%s,%s,%s,\"%s\"\n",
			timestamp, mf.GetName(), mf.GetType(), mf.GetHelp())
		if err != nil {
			return err
		}
	}

	mc.logger.Debug("Successfully exported metrics in CSV format")
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
