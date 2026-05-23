package cmd

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"multicast-bridge/internal/config"
	"multicast-bridge/internal/logger"
)

// Test 1: Encryption Disabled (Verify backward compatibility)
func TestIntegration_Phase7_Encryption_Disabled(t *testing.T) {
	// Wait extra time for any prior sockets or goroutines in OS to fully exit
	time.Sleep(2 * time.Second)

	logFile := filepath.Join(t.TempDir(), "test_integration_phase7_disabled.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}

	ResetStats()

	// Ports
	ctrlPort := 50400
	dataPort := 50401
	mcastSendPort := 50007
	mcastRecvPort := 50008

	// Configs
	sendCfg := config.NewDefaultSendConfig()
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = mcastSendPort
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = ctrlPort
	sendCfg.Unicast.DataPort = dataPort
	sendCfg.Encryption.Enabled = false
	sendCfg.Log.File = logFile

	recvCfg := config.NewDefaultRecvConfig()
	recvCfg.Sender.Address = "127.0.0.1"
	recvCfg.Sender.ControlPort = ctrlPort
	recvCfg.Sender.DataPort = dataPort
	recvCfg.Multicast.Address = "127.0.0.1"
	recvCfg.Multicast.Port = mcastRecvPort
	recvCfg.Multicast.Interface = "127.0.0.1"
	recvCfg.Encryption.Enabled = false
	recvCfg.Log.File = logFile

	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(1 * time.Second)

	t.Log("Starting Receiver...")
	go runRecvNormal(recvCfg)
	time.Sleep(2 * time.Second)

	// Send normal multicast packet
	mcastAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", mcastSendPort))
	mcastConn, err := net.DialUDP("udp4", nil, mcastAddr)
	if err != nil {
		logger.Close()
		t.Fatalf("Failed to dial sender multicast: %v", err)
	}
	defer mcastConn.Close()

	recvMcastAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", mcastRecvPort))
	recvMcastConn, err := net.ListenUDP("udp4", recvMcastAddr)
	if err != nil {
		logger.Close()
		t.Fatalf("Failed to listen receiver multicast: %v", err)
	}
	defer recvMcastConn.Close()

	payload := []byte("Hello Flat World! No Encryption!")
	_, err = mcastConn.Write(payload)
	if err != nil {
		logger.Close()
		t.Fatalf("Failed to write to sender: %v", err)
	}

	buf := make([]byte, 2048)
	_ = recvMcastConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, _, err := recvMcastConn.ReadFrom(buf)
	if err != nil {
		logger.Close()
		t.Fatalf("Failed to read forwarded multicast packet: %v", err)
	}

	if string(buf[:n]) != string(payload) {
		t.Errorf("Expected '%s', got '%s'", string(payload), string(buf[:n]))
	}

	CleanUpRecv()
	CleanUpSender()
	logger.Close()
	time.Sleep(2 * time.Second) // Ensure all loops terminate
}

// Test 2: Encryption Enabled - Successful Authentication & Data Transmission
func TestIntegration_Phase7_Encryption_Enabled_Success(t *testing.T) {
	// Wait extra time for any prior sockets or goroutines in OS to fully exit
	time.Sleep(2 * time.Second)

	logFile := filepath.Join(t.TempDir(), "test_integration_phase7_enabled_success.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}

	ResetStats()

	// Ports
	ctrlPort := 50500
	dataPort := 50501
	mcastSendPort := 50009
	mcastRecvPort := 50010
	passphrase := "highly_secure_passphrase"

	// Configs
	sendCfg := config.NewDefaultSendConfig()
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = mcastSendPort
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = ctrlPort
	sendCfg.Unicast.DataPort = dataPort
	sendCfg.Encryption.Enabled = true
	sendCfg.Encryption.Passphrase = passphrase
	sendCfg.Log.File = logFile

	recvCfg := config.NewDefaultRecvConfig()
	recvCfg.Sender.Address = "127.0.0.1"
	recvCfg.Sender.ControlPort = ctrlPort
	recvCfg.Sender.DataPort = dataPort
	recvCfg.Multicast.Address = "127.0.0.1"
	recvCfg.Multicast.Port = mcastRecvPort
	recvCfg.Multicast.Interface = "127.0.0.1"
	recvCfg.Encryption.Enabled = true
	recvCfg.Encryption.Passphrase = passphrase
	recvCfg.Log.File = logFile

	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(1 * time.Second)

	t.Log("Starting Receiver...")
	go runRecvNormal(recvCfg)
	time.Sleep(2 * time.Second) // Let handshake complete and stabilize

	// Send multicast packet
	mcastAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", mcastSendPort))
	mcastConn, err := net.DialUDP("udp4", nil, mcastAddr)
	if err != nil {
		logger.Close()
		t.Fatalf("Failed to dial sender multicast: %v", err)
	}
	defer mcastConn.Close()

	recvMcastAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", mcastRecvPort))
	recvMcastConn, err := net.ListenUDP("udp4", recvMcastAddr)
	if err != nil {
		logger.Close()
		t.Fatalf("Failed to listen receiver multicast: %v", err)
	}
	defer recvMcastConn.Close()

	payload := []byte("AES-GCM Secret Payload!")
	_, err = mcastConn.Write(payload)
	if err != nil {
		logger.Close()
		t.Fatalf("Failed to write to sender: %v", err)
	}

	buf := make([]byte, 2048)
	_ = recvMcastConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, _, err := recvMcastConn.ReadFrom(buf)
	if err != nil {
		logger.Close()
		t.Fatalf("Failed to read decrypted forwarded multicast packet: %v", err)
	}

	if string(buf[:n]) != string(payload) {
		t.Errorf("Expected '%s', got '%s'", string(payload), string(buf[:n]))
	}

	CleanUpRecv()
	CleanUpSender()
	logger.Close()
	time.Sleep(2 * time.Second) // Ensure all loops terminate

	// Check logs to make sure handshake succeeded
	logBytes, err := os.ReadFile(logFile)
	if err == nil {
		logStr := string(logBytes)
		if !strings.Contains(logStr, "Authentication succeeded") {
			t.Error("Handshake success log was missing")
		}
	}
}

// Test 3: Encryption Enabled - Authentication Failure (Wrong Passphrase)
func TestIntegration_Phase7_Encryption_Enabled_Failure(t *testing.T) {
	// Wait extra time for any prior sockets or goroutines in OS to fully exit
	time.Sleep(2 * time.Second)

	logFile := filepath.Join(t.TempDir(), "test_integration_phase7_enabled_failure.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}

	ResetStats()

	// Ports
	ctrlPort := 50600
	dataPort := 50601
	mcastSendPort := 50011
	mcastRecvPort := 50012

	// Configs (passwords mismatch)
	sendCfg := config.NewDefaultSendConfig()
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = mcastSendPort
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = ctrlPort
	sendCfg.Unicast.DataPort = dataPort
	sendCfg.Encryption.Enabled = true
	sendCfg.Encryption.Passphrase = "sender_pass"
	sendCfg.Log.File = logFile

	recvCfg := config.NewDefaultRecvConfig()
	recvCfg.Sender.Address = "127.0.0.1"
	recvCfg.Sender.ControlPort = ctrlPort
	recvCfg.Sender.DataPort = dataPort
	recvCfg.Multicast.Address = "127.0.0.1"
	recvCfg.Multicast.Port = mcastRecvPort
	recvCfg.Multicast.Interface = "127.0.0.1"
	recvCfg.Encryption.Enabled = true
	recvCfg.Encryption.Passphrase = "wrong_recv_pass"
	recvCfg.Log.File = logFile

	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(1 * time.Second)

	t.Log("Starting Receiver (expecting auth failure)...")
	go runRecvNormal(recvCfg)
	time.Sleep(2 * time.Second) // Wait for handshake attempt

	CleanUpRecv()
	CleanUpSender()
	logger.Close()
	time.Sleep(2 * time.Second) // Ensure all loops terminate

	// Read and verify logs for [101] (authentication failed)
	logBytes, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	logStr := string(logBytes)
	t.Logf("--- Authentication Failure Log Output ---\n%s\n-----------------------", logStr)

	if !strings.Contains(logStr, "[101]") {
		t.Error("Expected [101] authentication failed error in logs, but it was missing.")
	}
}
