package cmd

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"multicast-bridge/internal/config"
	"multicast-bridge/internal/data"
	"multicast-bridge/internal/logger"
)

func TestIntegration_Phase5_NormalAndLossAndVersion(t *testing.T) {
	// Initialize Log File
	logFile := filepath.Join(t.TempDir(), "test_integration_phase5.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Close()

	// Reset Stream Statistics
	ResetStats()

	// Sender Configuration
	sendCfg := &config.SendConfig{}
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = 49001
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = 49100
	sendCfg.Unicast.DataPort = 49101
	sendCfg.Unicast.MaxSessions = 2
	sendCfg.KeepAlive.Interval = 1
	sendCfg.Log.Level = "DEBUG"
	sendCfg.Log.File = logFile

	// Receiver Configuration
	recvCfg := &config.RecvConfig{}
	recvCfg.Sender.Address = "127.0.0.1"
	recvCfg.Sender.ControlPort = 49100
	recvCfg.Sender.DataPort = 49101
	recvCfg.Multicast.Address = "127.0.0.1"
	recvCfg.Multicast.Port = 49002
	recvCfg.Multicast.Interface = "127.0.0.1"
	recvCfg.KeepAlive.Interval = 1
	recvCfg.Log.Level = "DEBUG"
	recvCfg.Log.File = logFile

	// 1. Start Sender in background
	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(500 * time.Millisecond)

	// 2. Start Receiver in background
	t.Log("Starting Receiver...")
	go runRecvNormal(recvCfg)
	time.Sleep(1500 * time.Millisecond) // Let it register

	// 3. Test Case 1: Normal Encapsulation Forwarding & Statistics
	// Create UDP socket to send raw multicast to Sender
	mcastAddr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:49001")
	if err != nil {
		t.Fatalf("Failed to resolve multicast addr: %v", err)
	}
	mcastConn, err := net.DialUDP("udp4", nil, mcastAddr)
	if err != nil {
		t.Fatalf("Failed to dial multicast addr: %v", err)
	}
	defer mcastConn.Close()

	// Create UDP listener to intercept forwarded multicast on Receiver's end
	recvMcastAddr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:49002")
	if err != nil {
		t.Fatalf("Failed to resolve receive multicast addr: %v", err)
	}
	recvMcastConn, err := net.ListenUDP("udp4", recvMcastAddr)
	if err != nil {
		t.Fatalf("Failed to listen on receive multicast: %v", err)
	}
	defer recvMcastConn.Close()

	// Write multicast payload to Sender
	payload := []byte("Hello Encapsulated World!")
	_, err = mcastConn.Write(payload)
	if err != nil {
		t.Fatalf("Failed to write test data to sender: %v", err)
	}

	// Read forwarded payload on Receiver's output socket
	buf := make([]byte, 2048)
	_ = recvMcastConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := recvMcastConn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("Failed to read forwarded multicast packet: %v", err)
	}

	if string(buf[:n]) != string(payload) {
		t.Errorf("Expected payload '%s', got '%s'", string(payload), string(buf[:n]))
	}

	// Verify Statistics
	lost, total, _, _, avg := globalStats.GetStats()
	t.Logf("Stats - Lost: %d, Total: %d, Avg Latency: %v", lost, total, avg)
	if total != 1 {
		t.Errorf("Expected total packets received to be 1, got %d", total)
	}
	if lost != 0 {
		t.Errorf("Expected lost packets to be 0, got %d", lost)
	}

	// 4. Test Case 3: Sequence Loss Detection (Error 304)
	// Directly send a mocked encapsulated packet with a sequence gap to Receiver's unicast data port
	destAddr, err := net.ResolveUDPAddr("udp4", "127.0.0.1:49101")
	if err != nil {
		t.Fatalf("Failed to resolve unicast data addr: %v", err)
	}
	dataConn, err := net.DialUDP("udp4", nil, destAddr)
	if err != nil {
		t.Fatalf("Failed to dial unicast data addr: %v", err)
	}
	defer dataConn.Close()

	// Header: Version=Current, SeqNum=5 (jumped from 0), Timestamp=Now
	h := &data.EncapsulatedHeader{
		Version:    data.CurrentHeaderVersion,
		SeqNum:     5,
		Timestamp:  time.Now().UnixNano(),
		PayloadLen: uint16(len(payload)),
		FECInfo:    0,
	}
	headerBytes := h.Serialize()
	encapsulated := append(headerBytes, payload...)

	_, err = dataConn.Write(encapsulated)
	if err != nil {
		t.Fatalf("Failed to write mocked sequence jump packet: %v", err)
	}

	// Wait briefly for receiver to process
	time.Sleep(200 * time.Millisecond)

	lost, total, _, _, _ = globalStats.GetStats()
	t.Logf("Stats after sequence jump - Lost: %d, Total: %d", lost, total)
	if total != 2 {
		t.Errorf("Expected total packets to be 2, got %d", total)
	}
	if lost != 4 {
		t.Errorf("Expected lost packets to be 4 (missing 1, 2, 3, 4), got %d", lost)
	}

	// 5. Test Case 2: Version Mismatch Behavior
	// A) Minor Version Mismatch (Major=0, Minor=9 -> Version=0x09)
	t.Log("Testing minor version mismatch...")
	hMinor := &data.EncapsulatedHeader{
		Version:    (data.MajorVersion << 4) | 9, // Major 0, Minor 9
		SeqNum:     6,
		Timestamp:  time.Now().UnixNano(),
		PayloadLen: uint16(len(payload)),
		FECInfo:    0,
	}
	encMinor := append(hMinor.Serialize(), payload...)
	_, err = dataConn.Write(encMinor)
	if err != nil {
		t.Fatalf("Failed to write minor mismatch packet: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Should still be processed and not disconnect
	_, total, _, _, _ = globalStats.GetStats()
	if total != 3 {
		t.Errorf("Expected total packets to be 3 after minor mismatch, got %d", total)
	}

	// B) Major Version Mismatch (Major=2, Minor=1 -> Version=0x21) -> Should disconnect
	t.Log("Testing major version mismatch (Disconnect & Reconnect)...")
	hMajor := &data.EncapsulatedHeader{
		Version:    (2 << 4) | 1, // Major 2, Minor 1
		SeqNum:     7,
		Timestamp:  time.Now().UnixNano(),
		PayloadLen: uint16(len(payload)),
		FECInfo:    0,
	}
	encMajor := append(hMajor.Serialize(), payload...)
	_, err = dataConn.Write(encMajor)
	if err != nil {
		t.Fatalf("Failed to write major mismatch packet: %v", err)
	}

	// Wait for reconnection process to trigger (disconnect, wait 2s, retry registration)
	time.Sleep(3000 * time.Millisecond)

	// 6. Clean Up All Goroutines & Connections
	t.Log("Cleaning up and tearing down...")
	CleanUpRecv()
	CleanUpSender()
	time.Sleep(500 * time.Millisecond)

	// Read and verify logs
	logBytes, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	logStr := string(logBytes)
	t.Logf("--- Integration Log Output ---\n%s\n-----------------------", logStr)

	// Check for Error 304 (Packet loss)
	if !strings.Contains(logStr, "[304]") {
		t.Error("Expected [304] packet loss warning in logs, but it was missing.")
	}

	// Check for Warning 106 (Minor version mismatch)
	if !strings.Contains(logStr, "[106]") {
		t.Error("Expected [106] minor version mismatch warning in logs, but it was missing.")
	}

	// Check for Error 102 (Major version mismatch)
	if !strings.Contains(logStr, "[102]") {
		t.Error("Expected [102] major version mismatch error in logs, but it was missing.")
	}

	// Check for reconnection trigger log
	if !strings.Contains(logStr, "Disconnected. Waiting 2 seconds before reconnection attempt...") {
		t.Error("Expected reconnect attempt warning in logs, but it was missing.")
	}
}
