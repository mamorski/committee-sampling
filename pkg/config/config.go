package config

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/spf13/viper"
)

type SyncType int

const (
	TimeSync SyncType = iota
)

func (s SyncType) String() string {
	switch s {
	case TimeSync:
		return "TimeSync"
	default:
		return "Unknown"
	}
}

type Config struct {
	Network         Network         `mapstructure:"network"`
	Graph           Graph           `mapstructure:"graph"`
	Committee       Committee       `mapstructure:"committee"`       // Committee configuration for the protocol
	Synchronization Synchronization `mapstructure:"synchronization"` // Configuration for the synchronization protocol
	Logger          Logger          `mapstructure:"logger"`          // Configuration for the logger
	Metrics         Metrics         `mapstructure:"metrics"`         // Configuration for metrics collection
}

type Logger struct {
	Level string `mapstructure:"level"`
}

type Metrics struct {
	Enabled      bool          `mapstructure:"enabled"`       // Enable metrics collection
	PushGateway  PushGateway   `mapstructure:"push_gateway"`  // Push gateway configuration
	HTTPServer   HTTPServer    `mapstructure:"http_server"`   // HTTP server for /metrics endpoint
	PushInterval time.Duration `mapstructure:"push_interval"` // Interval for pushing metrics
	JobName      string        `mapstructure:"job_name"`      // Job name for metrics
	InstanceName string        `mapstructure:"instance_name"` // Instance name for metrics
}

type PushGateway struct {
	Enabled  bool   `mapstructure:"enabled"`  // Enable push gateway
	URL      string `mapstructure:"url"`      // Push gateway URL
	Username string `mapstructure:"username"` // Basic auth username (optional)
	Password string `mapstructure:"password"` // Basic auth password (optional)
}

type HTTPServer struct {
	Enabled bool   `mapstructure:"enabled"` // Enable HTTP metrics server
	Port    int    `mapstructure:"port"`    // Port for HTTP metrics server
	Path    string `mapstructure:"path"`    // Path for metrics endpoint (default: /metrics)
}

type Network struct {
	ListenPort        int       `mapstructure:"listen_port"`         // Port to listen for incoming connections
	MaxOutboundDegree int       `mapstructure:"max_outbound_degree"` // Maximum number of outbound connections
	DiscoveryConfig   Discovery `mapstructure:"discovery_config"`    // Configuration for peer discoveryÏ

}

type Discovery struct {
	ProtocolID     string        `mapstructure:"protocol_id"`     // Protocol ID for the discovery service
	Interval       time.Duration `mapstructure:"interval"`        // Interval for discovery messages
	BootstrapPeers []string      `mapstructure:"bootstrap_peers"` // DHT specific config
}

type Graph struct {
	Diameter      int `mapstructure:"diameter"`       // Degree bound if the graph
	GradingLevels int `mapstructure:"grading_levels"` // Grading levels for the graph
}

type Committee struct {
	SessionID     string  `mapstructure:"session_id"`     // Unique identifier for the protocol session
	Lambda        int     `mapstructure:"lambda"`         // Security parameter for VRF and VDF
	Weight        float64 `mapstructure:"weight"`         // Resource weight per party
	TotalW        float64 `mapstructure:"total_weight"`   // Total weight of the committee
	DeltaW        float64 `mapstructure:"delta_w"`        // Acceptable weight deviation
	CommitteeSize int     `mapstructure:"committee_size"` // Size of the committee to be formed
	Delay         int     `mapstructure:"delay"`          // Delay for the VDF in seconds
}

type Synchronization struct {
	Type                 SyncType      `mapstructure:"type"`                   // Type of synchronization (TimeSync only)
	ExAnteRoundTimeout   time.Duration `mapstructure:"ex_ante_round_timeout"`  // Timeout for ExAnte rounds in milliseconds
	ExPostRoundTimeout   time.Duration `mapstructure:"ex_post_round_timeout"`  // Timeout for ExPost rounds in milliseconds
	MDAGRoundTimeout     time.Duration `mapstructure:"mdag_round_timeout"`     // Timeout for MDAG rounds in milliseconds
	BuildingGraphTimeout time.Duration `mapstructure:"building_graph_timeout"` // Timeout for building the network graph
	StartTime            int64         `mapstructure:"start_time"`             // Start time as Unix timestamp UTC
	TimeServer           string        `mapstructure:"time_server"`            // NTP server for time synchronization
}

func Load(configPath string) (*Config, error) {
	viper.AutomaticEnv() // read in env vars
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetConfigType("json")

	if configPath != "" {
		viper.SetConfigFile(configPath)
	} else {
		env := viper.GetString("ENV")
		if env == "" {
			env = "dev"
		}
		viper.AddConfigPath("./configs/")
		viper.SetConfigName(env + ".json")
	}

	if err := viper.ReadInConfig(); err != nil {
		panic(fmt.Sprintf("Error reading config file: %s", err))
	}

	_, _ = fmt.Println("Using config file:", viper.ConfigFileUsed())
	cfg := &Config{}

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			stringToTimeHookFunc(),
			stringToDurationHookFunc(),
		),
		Result: cfg,
	})
	if err != nil {
		panic(fmt.Sprintf("Error creating decoder: %s", err))
	}

	if err := decoder.Decode(viper.AllSettings()); err != nil {
		panic(fmt.Sprintf("Error unmarshalling config: %s", err))
	}

	return cfg, nil
}

func stringToTimeHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf(time.Time{}) {
			return data, nil
		}

		return time.Parse(time.RFC3339, data.(string))
	}
}

func stringToDurationHookFunc() mapstructure.DecodeHookFunc {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf(time.Duration(0)) {
			return data, nil
		}

		return time.ParseDuration(data.(string))
	}
}
