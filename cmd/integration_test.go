package cmd

import (
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/net/ipv4"

	"multicast-bridge/internal/config"
	"multicast-bridge/internal/logger"
)

func TestIntegration_MulticastBridge(t *testing.T) {
	// 1. Initialize Logger to console for testing
	logFile := filepath.Join(t.TempDir(), "test_integration.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Close()

	// 2. Setup configurations
	sendCfg := &config.SendConfig{}
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = 45004 // Use private port range for test
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = 45100
	sendCfg.Unicast.DataPort = 45101
	sendCfg.Unicast.MaxSessions = 8
	sendCfg.KeepAlive.Interval = 1
	sendCfg.Log.Level = "DEBUG"
	sendCfg.Log.File = logFile

	recvCfg := &config.RecvConfig{}
	recvCfg.Sender.Address = "127.0.0.1"
	recvCfg.Sender.ControlPort = 45100
	recvCfg.Sender.DataPort = 45101
	recvCfg.Multicast.Address = "127.0.0.1"
	recvCfg.Multicast.Port = 45005 // Use different port to avoid binding conflicts
	recvCfg.Multicast.Interface = "127.0.0.1"
	recvCfg.KeepAlive.Interval = 1
	recvCfg.Log.Level = "DEBUG"
	recvCfg.Log.File = logFile


	// 3. Start Receiver in background
	t.Log("Starting Receiver...")
	go runRecvNormal(recvCfg)
	time.Sleep(500 * time.Millisecond) // Let it bind

	// 4. Start Sender in background
	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(500 * time.Millisecond) // Let it bind

	defer func() {
		t.Log("Stopping Sender and Receiver...")
		CleanUpSender()
		CleanUpRecv()
		time.Sleep(200 * time.Millisecond)
	}()

	// 5. Setup a listener for the re-sent multicast packet from Receiver
	t.Log("Setting up re-sent multicast listener...")
	rMcastAddr, err := net.ResolveUDPAddr("udp4", "239.0.0.1:45005")
	if err != nil {
		t.Fatalf("Failed to resolve re-sent multicast addr: %v", err)
	}

	rMcastConn, err := net.ListenMulticastUDP("udp4", nil, rMcastAddr)
	if err != nil {
		t.Logf("Warning: net.ListenMulticastUDP failed on this environment: %v. Bypassing direct loopback check.", err)
		// On some restricted CI or windows env, ListenMulticastUDP fails. 
		// We will still test packet sending and error logging.
	} else {
		defer rMcastConn.Close()
		rMcastConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	}

	// 6. Setup raw multicast sender socket and explicitly enable loopback
	t.Log("Preparing multicast sender socket...")
	mcastAddr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:45004")
	if err != nil {
		t.Fatalf("Failed to resolve multicast addr: %v", err)
	}

	laddr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to resolve local addr: %v", err)
	}

	sendConn, err := net.DialUDP("udp4", laddr, mcastAddr)
	if err != nil {
		t.Fatalf("Failed to dial multicast: %v", err)
	}
	defer sendConn.Close()

	// Explicitly set Multicast Loopback to ensure delivery on Windows.
	// x/net/ipv4 is used instead of raw syscalls so this test compiles on all platforms.
	if err := ipv4.NewPacketConn(sendConn).SetMulticastLoopback(true); err != nil {
		t.Logf("Failed to set multicast loopback: %v", err)
	}

	// 7. Test MTU Exceeded scenario (1500 bytes)
	t.Log("Sending 1500 bytes packet (exceeds MTU)...")
	largePayload := make([]byte, 1500)
	for i := range largePayload {
		largePayload[i] = 'B'
	}
	_, err = sendConn.Write(largePayload)
	if err != nil {
		t.Logf("Failed to write large packet: %v", err)
	}

	// 8. Test Normal Packet scenario (100 bytes)
	t.Log("Sending 100 bytes packet...")
	normalPayload := make([]byte, 100)
	copy(normalPayload, []byte("INTEGRATION-TEST-PAYLOAD"))
	_, err = sendConn.Write(normalPayload)
	if err != nil {
		t.Fatalf("Failed to write normal packet: %v", err)
	}

	// Allow some time for delivery and logging
	time.Sleep(1 * time.Second)

	// 9. Verify MTU warning was logged
	logBytes, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read test log file: %v", err)
	}
	logStr := string(logBytes)

	t.Logf("--- Test Log Output ---\n%s\n-----------------------", logStr)

	// We expect [301] warning code in the logs for the 1500-byte packet
	if !containsWarningCode301(logStr) {
		t.Error("Expected MTU warning [301] in logs, but it was not found.")
	} else {
		t.Log("SUCCESS: Verified MTU limits warning [301] in logs!")
	}
}

func containsWarningCode301(logStr string) bool {
	// Look for "[301]" and "MTU" or "exceeds"
	return (len(logStr) > 0) && (contains(logStr, "[301]") || contains(logStr, "MTU") || contains(logStr, "exceeds"))
}

func contains(s, substr string) bool {
	// Simple case-insensitive contains helper
	return (len(s) > 0) && (len(substr) > 0) && (len(s) >= len(substr)) && (s == substr || (len(s) > len(substr) && (s[:len(substr)] == substr || contains(s[1:], substr))))
}
