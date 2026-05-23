package cmd

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"multicast-bridge/internal/config"
	"multicast-bridge/internal/logger"
)

func TestIntegration_Phase4_Normal(t *testing.T) {
	// Initialize Logger to console for testing
	logFile := filepath.Join(t.TempDir(), "test_integration_phase4_normal.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Close()

	// Configuration
	sendCfg := &config.SendConfig{}
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = 46001
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = 46100
	sendCfg.Unicast.DataPort = 46101
	sendCfg.Unicast.MaxSessions = 2
	sendCfg.KeepAlive.Interval = 1
	sendCfg.Log.Level = "DEBUG"
	sendCfg.Log.File = logFile

	recvCfg := &config.RecvConfig{}
	recvCfg.Sender.Address = "127.0.0.1"
	recvCfg.Sender.ControlPort = 46100
	recvCfg.Sender.DataPort = 46101
	recvCfg.Multicast.Address = "127.0.0.1"
	recvCfg.Multicast.Port = 46002
	recvCfg.Multicast.Interface = "127.0.0.1"
	recvCfg.KeepAlive.Interval = 1
	recvCfg.Log.Level = "DEBUG"
	recvCfg.Log.File = logFile

	// 1. Start Sender in background
	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(500 * time.Millisecond) // Let it bind

	// 2. Start Receiver in background
	t.Log("Starting Receiver...")
	go runRecvNormal(recvCfg)
	time.Sleep(1500 * time.Millisecond) // Let it register and exchange keep-alive

	// 3. Stop Receiver cleanly (simulates Graceful Shutdown and LEAVE)
	t.Log("Stopping Receiver (LEAVE)...")
	CleanUpRecv()
	time.Sleep(500 * time.Millisecond)

	// 4. Stop Sender
	t.Log("Stopping Sender...")
	CleanUpSender()
	time.Sleep(200 * time.Millisecond)

	// 5. Verify registration, keep-alive and leave logs
	logBytes, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read test log file: %v", err)
	}
	logStr := string(logBytes)
	t.Logf("--- Normal Log Output ---\n%s\n-----------------------", logStr)

	if !strings.Contains(logStr, "Successfully registered with sender!") {
		t.Error("Expected successful registration message in logs, but it was missing.")
	}

	if !strings.Contains(logStr, "Added new forwarding session") {
		t.Error("Expected forwarding session added message in logs, but it was missing.")
	}

	if !strings.Contains(logStr, "Received LEAVE") {
		t.Error("Expected LEAVE packet received log in sender, but it was missing.")
	}
}

func TestIntegration_Phase4_MaxSessions(t *testing.T) {
	// Initialize Logger to console for testing
	logFile := filepath.Join(t.TempDir(), "test_integration_phase4_max.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Close()

	// Configuration with MaxSessions = 1
	sendCfg := &config.SendConfig{}
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = 47001
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = 47100
	sendCfg.Unicast.DataPort = 47101
	sendCfg.Unicast.MaxSessions = 1
	sendCfg.KeepAlive.Interval = 1
	sendCfg.Log.Level = "DEBUG"
	sendCfg.Log.File = logFile

	recvCfg1 := &config.RecvConfig{}
	recvCfg1.Sender.Address = "127.0.0.1"
	recvCfg1.Sender.ControlPort = 47100
	recvCfg1.Sender.DataPort = 47101
	recvCfg1.Multicast.Address = "127.0.0.1"
	recvCfg1.Multicast.Port = 47002
	recvCfg1.Multicast.Interface = "127.0.0.1"
	recvCfg1.KeepAlive.Interval = 1
	recvCfg1.Log.Level = "DEBUG"
	recvCfg1.Log.File = logFile

	recvCfg2 := &config.RecvConfig{}
	recvCfg2.Sender.Address = "127.0.0.1"
	recvCfg2.Sender.ControlPort = 47100
	recvCfg2.Sender.DataPort = 47102 // Different data port
	recvCfg2.Multicast.Address = "127.0.0.1"
	recvCfg2.Multicast.Port = 47003
	recvCfg2.Multicast.Interface = "127.0.0.1"
	recvCfg2.KeepAlive.Interval = 1
	recvCfg2.Log.Level = "DEBUG"
	recvCfg2.Log.File = logFile

	// 1. Start Sender in background
	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(500 * time.Millisecond)

	// 2. Start Receiver 1
	t.Log("Starting Receiver 1...")
	go runRecvNormal(recvCfg1)
	time.Sleep(1500 * time.Millisecond) // Let it register successfully

	// 3. Start Receiver 2 (should be rejected)
	t.Log("Starting Receiver 2 (Expect denial)...")
	// Since runRecvNormal relies on globals like globalRecvConn and globalControlConn,
	// running another normal receiver in parallel within the same process might override those globals.
	// To avoid global variable conflict in this specific multiple-receiver test, we will perform
	// a mock REGISTER connection for Receiver 2 manually.
	go func() {
		senderAddr, _ := net.ResolveUDPAddr("udp4", "127.0.0.1:47100")
		cconn, err := net.DialUDP("udp4", nil, senderAddr)
		if err != nil {
			return
		}
		defer cconn.Close()

		// Send REGISTER packet
		// Custom format for registering
		regPayload := []byte{0x00, 0x00} // Custom empty dataPort
		regPayload = append(regPayload, 0x00) // version string length 0
		regPayload = append(regPayload, 0x00) // ip string length 0
		
		// In Phase 4, common header [0]=TypeRegister, [1-2]=Payload len
		data := make([]byte, 3+len(regPayload))
		data[0] = 0x01
		data[1] = 0x00
		data[2] = uint8(len(regPayload))
		copy(data[3:], regPayload)

		_, _ = cconn.Write(data)
		time.Sleep(500 * time.Millisecond)
	}()

	time.Sleep(1000 * time.Millisecond)

	// 4. Clean up
	CleanUpRecv()
	CleanUpSender()
	time.Sleep(200 * time.Millisecond)

	// 5. Verify denial logs
	logBytes, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read test log file: %v", err)
	}
	logStr := string(logBytes)
	t.Logf("--- MaxSessions Log Output ---\n%s\n-----------------------", logStr)

	// Verify Error Code 104
	if !strings.Contains(logStr, "[104]") {
		t.Error("Expected Max Sessions Reached error [104] in logs, but it was missing.")
	}
}

func TestIntegration_Phase4_Timeout(t *testing.T) {
	// Initialize Logger to console for testing
	logFile := filepath.Join(t.TempDir(), "test_integration_phase4_timeout.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Close()

	// Configuration
	sendCfg := &config.SendConfig{}
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = 48001
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = 48100
	sendCfg.Unicast.DataPort = 48101
	sendCfg.Unicast.MaxSessions = 2
	sendCfg.KeepAlive.Interval = 1 // 1 second interval, so timeout is 3 seconds
	sendCfg.Log.Level = "DEBUG"
	sendCfg.Log.File = logFile

	// 1. Start Sender in background
	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(500 * time.Millisecond)

	// 2. Perform a mock registration and then stop sending KeepAlives (forces timeout)
	t.Log("Simulating Receiver register and silence...")
	go func() {
		senderAddr, _ := net.ResolveUDPAddr("udp4", "127.0.0.1:48100")
		cconn, err := net.DialUDP("udp4", nil, senderAddr)
		if err != nil {
			return
		}
		defer cconn.Close()

		// Register payload: dataPort=48102, version="0.1.0-draft", ip="127.0.0.1"
		vBytes := []byte("0.1.0-draft")
		ipBytes := []byte("127.0.0.1")
		payload := make([]byte, 2+1+len(vBytes)+1+len(ipBytes))
		payload[0] = 0xbc // 48102 high byte
		payload[1] = 0x46 // 48102 low byte
		payload[2] = uint8(len(vBytes))
		copy(payload[3:3+len(vBytes)], vBytes)
		idx := 3 + len(vBytes)
		payload[idx] = uint8(len(ipBytes))
		copy(payload[idx+1:], ipBytes)

		data := make([]byte, 3+len(payload))
		data[0] = 0x01 // TypeRegister
		data[1] = 0x00
		data[2] = uint8(len(payload))
		copy(data[3:], payload)

		_, _ = cconn.Write(data)
		
		// Wait for RegisterAck
		buf := make([]byte, 1024)
		_ = cconn.SetReadDeadline(time.Now().Add(1 * time.Second))
		_, _ = cconn.Read(buf)

		// Just sleep to let the sender time out this session
		time.Sleep(4 * time.Second)
	}()

	// Wait for timeout detection (interval * 3 = 3s, plus some buffer)
	time.Sleep(4500 * time.Millisecond)

	// Clean up
	CleanUpSender()
	time.Sleep(200 * time.Millisecond)

	// 3. Verify Timeout log
	logBytes, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read test log file: %v", err)
	}
	logStr := string(logBytes)
	t.Logf("--- Timeout Log Output ---\n%s\n-----------------------", logStr)

	// Verify Error Code 302
	if !strings.Contains(logStr, "[302]") {
		t.Error("Expected KeepAlive Timeout error [302] in logs, but it was missing.")
	}
}
