package config

type Config struct {
	Network Network
}

type Network struct {
	BootstrapPeers []string
	LogLevel       string
}
