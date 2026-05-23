package cmd

import (
	"bytes"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

func TestIntegrationPhase8_StartupErrors(t *testing.T) {
	// Build the latest binary for testing
	binaryName := "test-multicast-bridge-phase8"
	if runtime.GOOS == "windows" {
		binaryName += ".exe"
	}
	
	// Ensure we compile in the project root
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Failed to get current working directory: %v", err)
	}
	projectRoot := filepath.Dir(cwd)

	buildCmd := exec.Command("go", "build", "-o", binaryName, "main.go")
	buildCmd.Dir = projectRoot
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("Failed to build test binary: %v", err)
	}
	defer os.Remove(filepath.Join(projectRoot, binaryName))

	binaryPath := filepath.Join(projectRoot, binaryName)

	// Test 1: Config file not found [201]
	t.Run("ConfigFileNotFound_201", func(t *testing.T) {
		cmd := exec.Command(binaryPath, "send", "--config", "non_existent_config_file.yaml")
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		
		err := cmd.Run()
		exitCode := 0
		if err != nil {
			if exitError, ok := err.(*exec.ExitError); ok {
				exitCode = exitError.ExitCode()
			} else {
				t.Fatalf("Failed to execute command: %v", err)
			}
		}
		
		if exitCode != 201 {
			t.Errorf("Expected exit code 201, got %d. Output:\n%s", exitCode, out.String())
		}
	})

	// Test 2: Validation failure [202]
	t.Run("ValidationFailure_202", func(t *testing.T) {
		// Provide invalid multicast port via command line arguments
		cmd := exec.Command(binaryPath, "send", "--multicast", "239.0.0.1:999999", "--interface", "127.0.0.1")
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		
		err := cmd.Run()
		exitCode := 0
		if err != nil {
			if exitError, ok := err.(*exec.ExitError); ok {
				exitCode = exitError.ExitCode()
			} else {
				t.Fatalf("Failed to execute command: %v", err)
			}
		}
		
		if exitCode != 202 {
			t.Errorf("Expected exit code 202, got %d. Output:\n%s", exitCode, out.String())
		}
	})

	// Test 3: Port conflict [203]
	t.Run("PortConflict_203", func(t *testing.T) {
		// Listen on Unicast Control Port (5100 by default) to force conflict
		addr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:5100")
		if err != nil {
			t.Fatalf("Failed to resolve port: %v", err)
		}
		listener, err := net.ListenUDP("udp4", addr)
		if err != nil {
			t.Fatalf("Failed to force bind port 5100: %v", err)
		}
		defer listener.Close()

		cmd := exec.Command(binaryPath, "send", "--multicast", "239.0.0.1:5004", "--interface", "127.0.0.1")
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &out
		
		err = cmd.Run()
		exitCode := 0
		if err != nil {
			if exitError, ok := err.(*exec.ExitError); ok {
				exitCode = exitError.ExitCode()
			} else {
				t.Fatalf("Failed to execute command: %v", err)
			}
		}
		
		if exitCode != 203 {
			t.Errorf("Expected exit code 203, got %d. Output:\n%s", exitCode, out.String())
		}
	})
}
