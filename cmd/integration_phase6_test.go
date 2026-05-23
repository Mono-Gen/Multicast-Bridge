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
	"multicast-bridge/internal/data"
	"multicast-bridge/internal/fec"
	"multicast-bridge/internal/logger"
)

// TestCase 1: FEC Disabled Mode (Default path-through behavior)
func TestIntegration_Phase6_FEC_Disabled(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "test_integration_phase6_disabled.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Close()

	ResetStats()

	// Ports
	ctrlPort := 50200
	dataPort := 50201
	mcastSendPort := 50003
	mcastRecvPort := 50004

	// Configs
	sendCfg := config.NewDefaultSendConfig()
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = mcastSendPort
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = ctrlPort
	sendCfg.Unicast.DataPort = dataPort
	sendCfg.FEC.Enabled = false
	sendCfg.Log.File = logFile

	recvCfg := config.NewDefaultRecvConfig()
	recvCfg.Sender.Address = "127.0.0.1"
	recvCfg.Sender.ControlPort = ctrlPort
	recvCfg.Sender.DataPort = dataPort
	recvCfg.Multicast.Address = "127.0.0.1"
	recvCfg.Multicast.Port = mcastRecvPort
	recvCfg.Multicast.Interface = "127.0.0.1"
	recvCfg.FEC.Enabled = false
	recvCfg.Log.File = logFile

	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(500 * time.Millisecond)

	t.Log("Starting Receiver...")
	go runRecvNormal(recvCfg)
	time.Sleep(1500 * time.Millisecond)

	// Send normal multicast packet
	mcastAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", mcastSendPort))
	mcastConn, _ := net.DialUDP("udp4", nil, mcastAddr)
	defer mcastConn.Close()

	recvMcastAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", mcastRecvPort))
	recvMcastConn, _ := net.ListenUDP("udp4", recvMcastAddr)
	defer recvMcastConn.Close()

	payload := []byte("Hello FEC Disabled World!")
	_, err := mcastConn.Write(payload)
	if err != nil {
		t.Fatalf("Failed to write to sender: %v", err)
	}

	buf := make([]byte, 2048)
	_ = recvMcastConn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _, err := recvMcastConn.ReadFrom(buf)
	if err != nil {
		t.Fatalf("Failed to read forwarded multicast packet: %v", err)
	}

	if string(buf[:n]) != string(payload) {
		t.Errorf("Expected '%s', got '%s'", string(payload), string(buf[:n]))
	}

	CleanUpRecv()
	CleanUpSender()
	time.Sleep(500 * time.Millisecond)
}

// TestCase 2: FEC Enabled Mode - Successful recovery with 2 packet drops
func TestIntegration_Phase6_FEC_Enabled_Recovery(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "test_integration_phase6_enabled.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Close()

	ResetStats()

	// Ports
	ctrlPort := 50100
	dataPort := 50101
	mcastSendPort := 50001
	mcastRecvPort := 50002

	// Configs (k=8, n=10)
	sendCfg := config.NewDefaultSendConfig()
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = mcastSendPort
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = ctrlPort
	sendCfg.Unicast.DataPort = dataPort
	sendCfg.FEC.Enabled = true
	sendCfg.FEC.K = 8
	sendCfg.FEC.N = 10
	sendCfg.Log.File = logFile

	recvCfg := config.NewDefaultRecvConfig()
	recvCfg.Sender.Address = "127.0.0.1"
	recvCfg.Sender.ControlPort = ctrlPort
	recvCfg.Sender.DataPort = dataPort
	recvCfg.Multicast.Address = "127.0.0.1"
	recvCfg.Multicast.Port = mcastRecvPort
	recvCfg.Multicast.Interface = "127.0.0.1"
	recvCfg.FEC.Enabled = true
	recvCfg.Log.File = logFile

	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(500 * time.Millisecond)

	t.Log("Starting Receiver...")
	go runRecvNormal(recvCfg)
	time.Sleep(300 * time.Millisecond)

	// We will manually inject custom encapsulated packets into Receiver's Unicast Data Port to test Reed-Solomon recovery.
	destAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", dataPort))
	dataConn, err := net.DialUDP("udp4", nil, destAddr)
	if err != nil {
		t.Fatalf("Failed to connect to receiver data port: %v", err)
	}
	defer dataConn.Close()

	// Receiver multicast listener
	recvMcastAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", mcastRecvPort))
	recvMcastConn, _ := net.ListenUDP("udp4", recvMcastAddr)
	defer recvMcastConn.Close()

	k := 8
	n := 10
	groupNum := uint8(5)

	// Create 8 packets with different payloads
	originalPayloads := make([][]byte, k)
	originalPackets := make([][]byte, k)
	for i := 0; i < k; i++ {
		originalPayloads[i] = []byte(fmt.Sprintf("fec-integration-packet-%d", i))
		h := &data.EncapsulatedHeader{
			Version:    data.CurrentHeaderVersion,
			SeqNum:     uint32(i),
			Timestamp:  time.Now().UnixNano(),
			PayloadLen: uint16(len(originalPayloads[i])),
			FECInfo:    (uint16(groupNum) << 8) | uint16(i),
		}
		originalPackets[i] = append(h.Serialize(), originalPayloads[i]...)
	}

	// Generate 2 redundant packets
	redundantPackets, err := fec.EncodeFEC(k, n, originalPackets)
	if err != nil {
		t.Fatalf("Failed to encode FEC: %v", err)
	}

	// Format redundant packet headers
	for i, rPkt := range redundantPackets {
		rh := &data.EncapsulatedHeader{
			Version:    data.CurrentHeaderVersion,
			SeqNum:     uint32(k + i),
			Timestamp:  time.Now().UnixNano(),
			PayloadLen: uint16(len(rPkt) - data.HeaderSize),
			FECInfo:    (1 << 15) | (uint16(groupNum) << 8) | uint16(k+i),
		}
		copy(rPkt[0:data.HeaderSize], rh.Serialize())
	}

	// Send data packets EXCEPT index 2 and 5 (drops)
	t.Log("Sending 6 data packets (dropping 2 and 5)...")
	for i := 0; i < k; i++ {
		if i == 2 || i == 5 {
			continue
		}
		_, _ = dataConn.Write(originalPackets[i])
		time.Sleep(10 * time.Millisecond) // Ensure order
	}

	// Send redundant packets to trigger recovery
	t.Log("Sending 2 FEC redundant packets...")
	_, _ = dataConn.Write(redundantPackets[0])
	time.Sleep(10 * time.Millisecond)
	_, _ = dataConn.Write(redundantPackets[1])

	// We expect 8 multicast packets to be re-sent by Receiver (6 path-through + 2 reconstructed)
	receivedMap := make(map[string]bool)
	buf := make([]byte, 2048)
	_ = recvMcastConn.SetReadDeadline(time.Now().Add(3 * time.Second))

	t.Log("Collecting forwarded multicast packets...")
	for i := 0; i < 8; i++ {
		readLen, _, err := recvMcastConn.ReadFrom(buf)
		if err != nil {
			t.Fatalf("Failed to read expected packet %d: %v", i, err)
		}
		receivedMap[string(buf[:readLen])] = true
	}

	// Verify all original payloads are received
	for i := 0; i < k; i++ {
		key := string(originalPayloads[i])
		if !receivedMap[key] {
			t.Errorf("Expected payload '%s' was not received by multicast listener", key)
		} else {
			t.Logf("Successfully verified receipt of payload: %s", key)
		}
	}

	CleanUpRecv()
	CleanUpSender()
	time.Sleep(500 * time.Millisecond)
}

// TestCase 3: FEC Unrecoverable Mode (3 drops) - Verify [303] permanent loss error log after timeout
func TestIntegration_Phase6_FEC_Unrecoverable_Timeout(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "test_integration_phase6_timeout.log")
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Close()

	ResetStats()

	// Ports
	ctrlPort := 50300
	dataPort := 50301
	mcastSendPort := 50005
	mcastRecvPort := 50006

	// Configs (KeepAlive = 1s, timeout = 2s)
	sendCfg := config.NewDefaultSendConfig()
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = mcastSendPort
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = ctrlPort
	sendCfg.Unicast.DataPort = dataPort
	sendCfg.FEC.Enabled = true
	sendCfg.FEC.K = 8
	sendCfg.FEC.N = 10
	sendCfg.Log.File = logFile

	recvCfg := config.NewDefaultRecvConfig()
	recvCfg.Sender.Address = "127.0.0.1"
	recvCfg.Sender.ControlPort = ctrlPort
	recvCfg.Sender.DataPort = dataPort
	recvCfg.Multicast.Address = "127.0.0.1"
	recvCfg.Multicast.Port = mcastRecvPort
	recvCfg.Multicast.Interface = "127.0.0.1"
	recvCfg.KeepAlive.Interval = 1
	recvCfg.FEC.Enabled = true
	recvCfg.Log.File = logFile

	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(500 * time.Millisecond)

	t.Log("Starting Receiver...")
	go runRecvNormal(recvCfg)
	time.Sleep(300 * time.Millisecond)

	destAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", dataPort))
	dataConn, _ := net.DialUDP("udp4", nil, destAddr)
	defer dataConn.Close()

	k := 8
	n := 10
	groupNum := uint8(12)

	originalPayloads := make([][]byte, k)
	originalPackets := make([][]byte, k)
	for i := 0; i < k; i++ {
		originalPayloads[i] = []byte(fmt.Sprintf("fec-integration-unrecoverable-%d", i))
		h := &data.EncapsulatedHeader{
			Version:    data.CurrentHeaderVersion,
			SeqNum:     uint32(i),
			Timestamp:  time.Now().UnixNano(),
			PayloadLen: uint16(len(originalPayloads[i])),
			FECInfo:    (uint16(groupNum) << 8) | uint16(i),
		}
		originalPackets[i] = append(h.Serialize(), originalPayloads[i]...)
	}

	redundantPackets, _ := fec.EncodeFEC(k, n, originalPackets)
	for i, rPkt := range redundantPackets {
		rh := &data.EncapsulatedHeader{
			Version:    data.CurrentHeaderVersion,
			SeqNum:     uint32(k + i),
			Timestamp:  time.Now().UnixNano(),
			PayloadLen: uint16(len(rPkt) - data.HeaderSize),
			FECInfo:    (1 << 15) | (uint16(groupNum) << 8) | uint16(k+i),
		}
		copy(rPkt[0:data.HeaderSize], rh.Serialize())
	}

	// Send data packets EXCEPT index 1, 3, and 5 (3 drops -> unrecoverable)
	t.Log("Sending 5 data packets (dropping 3)...")
	for i := 0; i < k; i++ {
		if i == 1 || i == 3 || i == 5 {
			continue
		}
		_, _ = dataConn.Write(originalPackets[i])
		time.Sleep(10 * time.Millisecond)
	}

	// Send redundant packets
	t.Log("Sending 2 redundant packets...")
	_, _ = dataConn.Write(redundantPackets[0])
	_, _ = dataConn.Write(redundantPackets[1])

	// Total received will be 5 + 2 = 7 packets, which is < K=8.
	// Recovery is impossible. We now wait for the 2.5 seconds timeout (KeepAlive=1s -> timeout=2s)
	t.Log("Waiting for timeout (3 seconds)...")
	time.Sleep(3500 * time.Millisecond)

	CleanUpRecv()
	CleanUpSender()
	time.Sleep(500 * time.Millisecond)

	// Read and verify logs for [303]
	logBytes, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	logStr := string(logBytes)
	t.Logf("--- Timeout Integration Log Output ---\n%s\n-----------------------", logStr)

	if !strings.Contains(logStr, "[303]") {
		t.Error("Expected [303] permanent loss / timeout warning in logs, but it was missing.")
	}
}
