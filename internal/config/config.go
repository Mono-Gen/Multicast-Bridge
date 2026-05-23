package config

import (
	"fmt"
	"net"
	"os"

	"gopkg.in/yaml.v3"
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
}

// Common KeepAlive Config
type KeepAliveConfig struct {
	Interval int `yaml:"interval"`
}

// SendConfig represents the sender configuration.
type SendConfig struct {
	Multicast struct {
		Address   string `yaml:"address"`
		Port      int    `yaml:"port"`
		Interface string `yaml:"interface"`
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
}

// RecvConfig represents the receiver configuration.
type RecvConfig struct {
	Sender struct {
		Address     string `yaml:"address"`
		ControlPort int    `yaml:"control_port"`
		DataPort    int    `yaml:"data_port"`
	} `yaml:"sender"`
	Multicast struct {
		Address   string `yaml:"address"`
		Port      int    `yaml:"port"`
		Interface string `yaml:"interface"`
	} `yaml:"multicast"`
	KeepAlive  KeepAliveConfig  `yaml:"keepalive"`
	FEC        struct {
		Enabled bool `yaml:"enabled"`
		Test    bool `yaml:"test"`
	} `yaml:"fec"`
	Encryption EncryptionConfig `yaml:"encryption"`
	Log        LogConfig        `yaml:"log"`
	StatsInterval int           `yaml:"stats_interval"`
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
	c.FEC.Enabled = false
	c.FEC.K = 8
	c.FEC.N = 10
	c.Encryption.Enabled = false
	c.Encryption.Passphrase = ""
	c.Log.Level = "INFO"
	c.Log.File = "send.log"
	c.StatsInterval = 10
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
	c.KeepAlive.Interval = 1
	c.FEC.Enabled = false
	c.FEC.Test = false
	c.Encryption.Enabled = false
	c.Encryption.Passphrase = ""
	c.Log.Level = "INFO"
	c.Log.File = "recv.log"
	c.StatsInterval = 10
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
	}

	// Encryption validation
	if c.Encryption.Enabled && c.Encryption.Passphrase == "" {
		return fmt.Errorf("encryption passphrase is required when encryption is enabled")
	}

	// Log Level validation
	if err := validateLogLevel(c.Log.Level); err != nil {
		return err
	}

	// Stats interval validation
	if c.StatsInterval <= 0 {
		return fmt.Errorf("invalid stats_interval: %d (must be > 0)", c.StatsInterval)
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
	if c.Encryption.Enabled && c.Encryption.Passphrase == "" {
		return fmt.Errorf("encryption passphrase is required when encryption is enabled")
	}

	// Log Level validation
	if err := validateLogLevel(c.Log.Level); err != nil {
		return err
	}

	// Stats interval validation
	if c.StatsInterval <= 0 {
		return fmt.Errorf("invalid stats_interval: %d (must be > 0)", c.StatsInterval)
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
