package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSendConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(c *SendConfig)
		wantErr bool
	}{
		{
			name: "Valid default config",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
			},
			wantErr: false,
		},
		{
			name: "Invalid multicast port (too large)",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.Multicast.Port = 65536
			},
			wantErr: true,
		},
		{
			name: "Invalid multicast address (unicast IP)",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.Multicast.Address = "192.168.1.1"
			},
			wantErr: true,
		},
		{
			name: "Missing interface",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = ""
			},
			wantErr: true,
		},
		{
			name: "Invalid FEC parameters (k > n)",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.FEC.Enabled = true
				c.FEC.K = 10
				c.FEC.N = 8
			},
			wantErr: true,
		},
		{
			name: "Invalid log level",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.Log.Level = "INVALID"
			},
			wantErr: true,
		},
		{
			name: "Invalid MTU (too small)",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.MTU = 500
			},
			wantErr: true,
		},
		{
			name: "Invalid DSCP (too large)",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.DSCP = 64
			},
			wantErr: true,
		},
		{
			name: "Invalid ControlDSCP (negative)",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.ControlDSCP = -1
			},
			wantErr: true,
		},
		{
			name: "Invalid SenderDataPort (too large)",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.SenderDataPort = 70000
			},
			wantErr: true,
		},
		{
			name: "Invalid FEC N parameter (too large, >16)",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.FEC.Enabled = true
				c.FEC.K = 8
				c.FEC.N = 17
			},
			wantErr: true,
		},
		{
			name: "Invalid SocketBufferSize (negative)",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.SocketBufferSize = -1
			},
			wantErr: true,
		},
		{
			name: "Invalid Encryption Iterations (negative)",
			setup: func(c *SendConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.Encryption.Enabled = true
				c.Encryption.Passphrase = "abc"
				c.Encryption.Iterations = -100
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewDefaultSendConfig()
			tt.setup(c)
			err := c.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("SendConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestRecvConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(c *RecvConfig)
		wantErr bool
	}{
		{
			name: "Valid default config",
			setup: func(c *RecvConfig) {
				c.Multicast.Interface = "127.0.0.1"
			},
			wantErr: false,
		},
		{
			name: "Invalid sender address",
			setup: func(c *RecvConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.Sender.Address = "invalid-ip"
			},
			wantErr: true,
		},
		{
			name: "Missing multicast interface",
			setup: func(c *RecvConfig) {
				c.Multicast.Interface = ""
			},
			wantErr: true,
		},
		{
			name: "Invalid Multicast TTL (too large)",
			setup: func(c *RecvConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.Multicast.TTL = 256
			},
			wantErr: true,
		},
		{
			name: "Invalid Multicast DSCP (negative)",
			setup: func(c *RecvConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.Multicast.DSCP = -1
			},
			wantErr: true,
		},
		{
			name: "Invalid ControlDSCP (too large)",
			setup: func(c *RecvConfig) {
				c.Multicast.Interface = "127.0.0.1"
				c.ControlDSCP = 64
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewDefaultRecvConfig()
			tt.setup(c)
			err := c.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("RecvConfig.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestLoadSendConfig_FileNotFound(t *testing.T) {
	_, err := LoadSendConfig("non-existent-file.yaml")
	if err == nil {
		t.Error("Expected error for non-existent file, got nil")
	}
}

func TestLoadSendConfig_Success(t *testing.T) {
	// Create temporary directory inside the workspace (avoid absolute OS temp directories)
	tempDir, err := os.MkdirTemp(".", "test-config-")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	yamlContent := `
multicast:
  address: 239.0.0.2
  port: 6000
  interface: 127.0.0.1
unicast:
  control_port: 6100
  data_port: 6101
  max_sessions: 4
`
	tmpFile := filepath.Join(tempDir, "send.yaml")
	if err := os.WriteFile(tmpFile, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("failed to write temp file: %v", err)
	}

	cfg, err := LoadSendConfig(tmpFile)
	if err != nil {
		t.Fatalf("LoadSendConfig failed: %v", err)
	}

	if cfg.Multicast.Address != "239.0.0.2" {
		t.Errorf("Expected address 239.0.0.2, got %s", cfg.Multicast.Address)
	}
	if cfg.Multicast.Port != 6000 {
		t.Errorf("Expected port 6000, got %d", cfg.Multicast.Port)
	}
	if cfg.Unicast.MaxSessions != 4 {
		t.Errorf("Expected max_sessions 4, got %d", cfg.Unicast.MaxSessions)
	}
}
