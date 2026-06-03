package config

import (
	"fmt"
	"net"
	"os"

	"gopkg.in/yaml.v3"
	"multicast-bridge/internal/logger"
)

// Common Log Config
type LogConfig struct {
	Level string `yaml:"level"`
	File  string `yaml:"file"`
}

// Common FEC Config
type FECConfig struct {
	Enabled bool `yaml:"enabled"`
	K       int  `yaml:"k"`
	N       int  `yaml:"n"`
}

// Common Encryption Config
type EncryptionConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Passphrase string `yaml:"passphrase"`
	Iterations int    `yaml:"iterations"`
}

// Common KeepAlive Config
type KeepAliveConfig struct {
	Interval          int `yaml:"interval"`
	TimeoutMultiplier int `yaml:"timeout_multiplier"`
}

// SendConfig represents the sender configuration.
type SendConfig struct {
	Multicast struct {
		Address       string `yaml:"address"`
		Port          int    `yaml:"port"`
		Interface     string `yaml:"interface"`
		SourceAddress string `yaml:"source_address"`
	} `yaml:"multicast"`
	Unicast struct {
		ControlPort int `yaml:"control_port"`
		DataPort    int `yaml:"data_port"`
		MaxSessions int `yaml:"max_sessions"`
	} `yaml:"unicast"`
	KeepAlive  KeepAliveConfig  `yaml:"keepalive"`
	FEC        FECConfig        `yaml:"fec"`
	Encryption EncryptionConfig `yaml:"encryption"`
	Log        LogConfig        `yaml:"log"`
	StatsInterval int           `yaml:"stats_interval"`
	MTU           int           `yaml:"mtu"`
	DSCP          int           `yaml:"dscp"`
	ControlDSCP   int           `yaml:"control_dscp"`
	SenderDataPort int          `yaml:"sender_data_port"`
	SocketBufferSize int        `yaml:"socket_buffer_size"`
}

// RecvConfig represents the receiver configuration.
type RecvConfig struct {
	Sender struct {
		Address     string `yaml:"address"`
		ControlPort int    `yaml:"control_port"`
		DataPort    int    `yaml:"data_port"`
	} `yaml:"sender"`
	Multicast struct {
		Address       string `yaml:"address"`
		Port          int    `yaml:"port"`
		Interface     string `yaml:"interface"`
		TTL           int    `yaml:"ttl"`
		DSCP          int    `yaml:"dscp"`
		SourceAddress string `yaml:"source_address"`
	} `yaml:"multicast"`
	KeepAlive  KeepAliveConfig  `yaml:"keepalive"`
	FEC        struct {
		Enabled bool `yaml:"enabled"`
		Test    bool `yaml:"test"`
	} `yaml:"fec"`
	Encryption EncryptionConfig `yaml:"encryption"`
	Log        LogConfig        `yaml:"log"`
	StatsInterval int           `yaml:"stats_interval"`
	ControlDSCP   int           `yaml:"control_dscp"`
	UnicastBindPort int         `yaml:"unicast_bind_port"`
	SocketBufferSize int        `yaml:"socket_buffer_size"`
}

// NewDefaultSendConfig returns a SendConfig populated with default values.
func NewDefaultSendConfig() *SendConfig {
	c := &SendConfig{}
	c.Multicast.Address = "239.0.0.1"
	c.Multicast.Port = 5004
	c.Multicast.Interface = "" // User MUST specify this
	c.Unicast.ControlPort = 5100
	c.Unicast.DataPort = 5101
	c.Unicast.MaxSessions = 8
	c.KeepAlive.Interval = 1
	c.KeepAlive.TimeoutMultiplier = 3
	c.FEC.Enabled = false
	c.FEC.K = 8
	c.FEC.N = 10
	c.Encryption.Enabled = false
	c.Encryption.Passphrase = ""
	c.Encryption.Iterations = 600000
	c.Log.Level = "INFO"
	c.Log.File = "send.log"
	c.StatsInterval = 10
	c.MTU = 1500
	c.DSCP = 0
	c.ControlDSCP = 0
	c.SenderDataPort = 0
	c.SocketBufferSize = 2097152
	return c
}

// NewDefaultRecvConfig returns a RecvConfig populated with default values.
func NewDefaultRecvConfig() *RecvConfig {
	c := &RecvConfig{}
	c.Sender.Address = "127.0.0.1"
	c.Sender.ControlPort = 5100
	c.Sender.DataPort = 5101
	c.Multicast.Address = "239.0.0.1"
	c.Multicast.Port = 5004
	c.Multicast.Interface = "" // User MUST specify this
	c.Multicast.TTL = 1
	c.Multicast.DSCP = 0
	c.KeepAlive.Interval = 1
	c.KeepAlive.TimeoutMultiplier = 3
	c.FEC.Enabled = false
	c.FEC.Test = false
	c.Encryption.Enabled = false
	c.Encryption.Passphrase = ""
	c.Encryption.Iterations = 600000
	c.Log.Level = "INFO"
	c.Log.File = "recv.log"
	c.StatsInterval = 10
	c.ControlDSCP = 0
	c.UnicastBindPort = 0
	c.SocketBufferSize = 2097152
	return c
}

// LoadSendConfig loads and validates the SendConfig from the given YAML file.
func LoadSendConfig(path string) (*SendConfig, error) {
	c := NewDefaultSendConfig()

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("[201] config file not found: %s", path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("[202] failed to read config: %w", err)
	}

	err = yaml.Unmarshal(data, c)
	if err != nil {
		return nil, fmt.Errorf("[202] failed to parse YAML: %w", err)
	}

	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("[202] validation failed: %w", err)
	}

	return c, nil
}

// LoadRecvConfig loads and validates the RecvConfig from the given YAML file.
func LoadRecvConfig(path string) (*RecvConfig, error) {
	c := NewDefaultRecvConfig()

	// Check if file exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("[201] config file not found: %s", path)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("[202] failed to read config: %w", err)
	}

	err = yaml.Unmarshal(data, c)
	if err != nil {
		return nil, fmt.Errorf("[202] failed to parse YAML: %w", err)
	}

	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("[202] validation failed: %w", err)
	}

	return c, nil
}

// Validate validates SendConfig parameters.
func (c *SendConfig) Validate() error {
	// Multicast Port validation
	if c.Multicast.Port < 1 || c.Multicast.Port > 65535 {
		return fmt.Errorf("invalid multicast port: %d (must be 1-65535)", c.Multicast.Port)
	}

	// Multicast Address validation
	if err := validateMulticastIP(c.Multicast.Address); err != nil {
		return fmt.Errorf("invalid multicast address: %w", err)
	}

	// Interface field must not be empty
	if c.Multicast.Interface == "" {
		return fmt.Errorf("multicast interface is required")
	}

	// Unicast ports validation
	if c.Unicast.ControlPort < 1 || c.Unicast.ControlPort > 65535 {
		return fmt.Errorf("invalid control_port: %d", c.Unicast.ControlPort)
	}
	if c.Unicast.DataPort < 1 || c.Unicast.DataPort > 65535 {
		return fmt.Errorf("invalid data_port: %d", c.Unicast.DataPort)
	}

	// Sessions validation
	if c.Unicast.MaxSessions <= 0 {
		return fmt.Errorf("invalid max_sessions: %d (must be > 0)", c.Unicast.MaxSessions)
	}

	// KeepAlive validation
	if c.KeepAlive.Interval <= 0 {
		return fmt.Errorf("invalid keepalive interval: %d (must be > 0)", c.KeepAlive.Interval)
	}

	// FEC validation
	if c.FEC.Enabled {
		if c.FEC.K <= 0 {
			return fmt.Errorf("invalid fec.k: %d (must be > 0)", c.FEC.K)
		}
		if c.FEC.N < c.FEC.K {
			return fmt.Errorf("invalid fec.n: %d (must be >= fec.k)", c.FEC.N)
		}
		if c.FEC.N > 16 {
			return fmt.Errorf("invalid fec.n: %d (must be <= 16 due to protocol index limit)", c.FEC.N)
		}
	}

	// Encryption validation
	if c.Encryption.Enabled {
		if c.Encryption.Passphrase == "" {
			return fmt.Errorf("encryption passphrase is required when encryption is enabled")
		}
		if c.Encryption.Iterations <= 0 {
			return fmt.Errorf("invalid encryption iterations: %d (must be > 0)", c.Encryption.Iterations)
		}
	}

	// SocketBufferSize validation
	if c.SocketBufferSize <= 0 {
		return fmt.Errorf("invalid socket_buffer_size: %d (must be > 0)", c.SocketBufferSize)
	}

	// Log Level validation
	if err := validateLogLevel(c.Log.Level); err != nil {
		return err
	}

	// Stats interval validation
	if c.StatsInterval <= 0 {
		return fmt.Errorf("invalid stats_interval: %d (must be > 0)", c.StatsInterval)
	}

	// MTU validation
	if c.MTU < 576 || c.MTU > 65535 {
		return fmt.Errorf("invalid mtu: %d (must be 576-65535)", c.MTU)
	}

	// DSCP validation
	if c.DSCP < 0 || c.DSCP > 63 {
		return fmt.Errorf("invalid dscp: %d (must be 0-63)", c.DSCP)
	}
	if c.ControlDSCP < 0 || c.ControlDSCP > 63 {
		return fmt.Errorf("invalid control_dscp: %d (must be 0-63)", c.ControlDSCP)
	}

	// SenderDataPort validation
	if c.SenderDataPort < 0 || c.SenderDataPort > 65535 {
		return fmt.Errorf("invalid sender_data_port: %d (must be 0-65535)", c.SenderDataPort)
	}

	return nil
}

// Validate validates RecvConfig parameters.
func (c *RecvConfig) Validate() error {
	// Sender IP validation
	if ip := net.ParseIP(c.Sender.Address); ip == nil {
		return fmt.Errorf("invalid sender address: %s", c.Sender.Address)
	}

	// Sender Ports validation
	if c.Sender.ControlPort < 1 || c.Sender.ControlPort > 65535 {
		return fmt.Errorf("invalid sender control_port: %d", c.Sender.ControlPort)
	}
	if c.Sender.DataPort < 1 || c.Sender.DataPort > 65535 {
		return fmt.Errorf("invalid sender data_port: %d", c.Sender.DataPort)
	}

	// Multicast Port validation
	if c.Multicast.Port < 1 || c.Multicast.Port > 65535 {
		return fmt.Errorf("invalid multicast port: %d (must be 1-65535)", c.Multicast.Port)
	}

	// Multicast Address validation
	if err := validateMulticastIP(c.Multicast.Address); err != nil {
		return fmt.Errorf("invalid multicast address: %w", err)
	}

	// Interface field must not be empty
	if c.Multicast.Interface == "" {
		return fmt.Errorf("multicast interface is required")
	}

	// KeepAlive validation
	if c.KeepAlive.Interval <= 0 {
		return fmt.Errorf("invalid keepalive interval: %d (must be > 0)", c.KeepAlive.Interval)
	}

	// Encryption validation
	if c.Encryption.Enabled {
		if c.Encryption.Passphrase == "" {
			return fmt.Errorf("encryption passphrase is required when encryption is enabled")
		}
		if c.Encryption.Iterations <= 0 {
			return fmt.Errorf("invalid encryption iterations: %d (must be > 0)", c.Encryption.Iterations)
		}
	}

	// SocketBufferSize validation
	if c.SocketBufferSize <= 0 {
		return fmt.Errorf("invalid socket_buffer_size: %d (must be > 0)", c.SocketBufferSize)
	}

	// Log Level validation
	if err := validateLogLevel(c.Log.Level); err != nil {
		return err
	}

	// Stats interval validation
	if c.StatsInterval <= 0 {
		return fmt.Errorf("invalid stats_interval: %d (must be > 0)", c.StatsInterval)
	}

	// Multicast TTL validation
	if c.Multicast.TTL < 1 || c.Multicast.TTL > 255 {
		return fmt.Errorf("invalid multicast ttl: %d (must be 1-255)", c.Multicast.TTL)
	}

	// DSCP validation
	if c.Multicast.DSCP < 0 || c.Multicast.DSCP > 63 {
		return fmt.Errorf("invalid multicast dscp: %d (must be 0-63)", c.Multicast.DSCP)
	}
	if c.ControlDSCP < 0 || c.ControlDSCP > 63 {
		return fmt.Errorf("invalid control_dscp: %d (must be 0-63)", c.ControlDSCP)
	}

	// UnicastBindPort validation
	if c.UnicastBindPort < 0 || c.UnicastBindPort > 65535 {
		return fmt.Errorf("invalid unicast_bind_port: %d (must be 0-65535)", c.UnicastBindPort)
	}

	return nil
}

func validateMulticastIP(ipStr string) error {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return fmt.Errorf("invalid IP format: %s", ipStr)
	}
	if !ip.IsMulticast() {
		return fmt.Errorf("IP address is not a multicast address: %s (must be in range 224.0.0.0-239.255.255.255)", ipStr)
	}

	// Well-Known Link-Local Multicast Address (224.0.0.0/24) check
	v4 := ip.To4()
	if v4 != nil {
		if v4[0] == 224 && v4[1] == 0 && v4[2] == 0 {
			logger.Warnf(106, "Multicast address %s is a Link-Local address (224.0.0.0/24). It might not be routed beyond the local segment.", ipStr)
		}
	}
	return nil
}

func validateLogLevel(level string) error {
	switch level {
	case "DEBUG", "INFO", "WARN", "ERROR":
		return nil
	default:
		return fmt.Errorf("invalid log level: %s (must be DEBUG, INFO, WARN, or ERROR)", level)
	}
}
