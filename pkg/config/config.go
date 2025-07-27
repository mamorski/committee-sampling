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
	ChannelSync SyncType = iota
	TimeSync
)

func (s SyncType) String() string {
	switch s {
	case ChannelSync:
		return "ChannelSync"
	case TimeSync:
		return "TimeSync"
	default:
		return "Unknown"
	}
}

type Config struct {
	Network         Network         `mapstructure:"network"`
	Graph           Graph           `mapstructure:"graph"`
	RunTime         RunTimeConfig   `mapstructure:"run_time"`        // Runtime configuration for the protocol
	Synchronization Synchronization `mapstructure:"synchronization"` // Configuration for the synchronization protocol
	Logger          Logger          `mapstructure:"logger"`          // Configuration for the logger
}

type Logger struct {
	Level string `mapstructure:"level"`
}

type Network struct {
	ListenPort        int           `mapstructure:"listen_port"`         // Port to listen for incoming connections
	MaxOutboundDegree int           `mapstructure:"max_outbound_degree"` // Maximum number of outbound connections
	HeartbeatInterval time.Duration `mapstructure:"heartbeat_interval"`  // Interval for heartbeat messages
	ConnectTimeout    time.Duration `mapstructure:"connect_timeout"`     // Timeout for establishing connections
	DiscoveryConfig   Discovery     `mapstructure:"discovery_config"`    // Configuration for peer discoveryÏ
	Topic             string        `mapstructure:"topic"`               // Topic for the synchronization protocol
	FindPeersTimeout  time.Duration `mapstructure:"find_peers_timeout"`  // Timeout for finding peers

}

type Discovery struct {
	DiscoveryType  string        `mapstructure:"discovery_type"`  // "dht" or "mdns"
	ProtocolID     string        `mapstructure:"protocol_id"`     // Protocol ID for the discovery service
	Interval       time.Duration `mapstructure:"interval"`        // Interval for discovery messages
	BootstrapPeers []string      `mapstructure:"bootstrap_peers"` // DHT specific config
	ServiceTag     string        `mapstructure:"service_tag"`     // mDNS specific config
}

type Graph struct {
	Diameter      int `mapstructure:"diameter"`       // Degree bound if the graph
	GradingLevels int `mapstructure:"grading_levels"` // Grading levels for the graph
}

type RunTimeConfig struct {
	SessionID     string  `mapstructure:"session_id"`     // Unique identifier for the protocol session
	Lambda        int     `mapstructure:"lambda"`         // Security parameter for VRF and VDF
	Weight        float64 `mapstructure:"weight"`         // Threshold for weight in the protocol
	DeltaW        float64 `mapstructure:"delta_w"`        // Acceptable weight deviation
	CommitteeSize int     `mapstructure:"committee_size"` // Size of the committee to be formed
	Delay         int     `mapstructure:"delay"`          // Delay for the VDF in seconds
}

type Synchronization struct {
	Type                 SyncType      `mapstructure:"type"`                   // Type of synchronization (ChannelSync or TimeSync)
	ExAnteRoundTimeout   time.Duration `mapstructure:"ex_ante_round_timeout"`  // Timeout for ExAnte rounds in milliseconds
	ExPostRoundTimeout   time.Duration `mapstructure:"ex_post_round_timeout"`  // Timeout for ExPost rounds in milliseconds
	MDAGRoundTimeout     time.Duration `mapstructure:"mdag_round_timeout"`     // Timeout for MDAG rounds in milliseconds
	BuildingGraphTimeout time.Duration `mapstructure:"building_graph_timeout"` // Timeout for building the network graph
	StartTime            int64         `mapstructure:"start_time"`             // Start time as Unix timestamp UTC
	TimeServer           string        `mapstructure:"time_server"`            // NTP server for time synchronization
	CertificatePath      string        `mapstructure:"certificate_path"`       // Path to certificate file containing public key
	Topic                string        `mapstructure:"topic"`                  // Topic for sync messages
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
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
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
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		if t != reflect.TypeOf(time.Duration(0)) {
			return data, nil
		}

		return time.ParseDuration(data.(string))
	}
}
