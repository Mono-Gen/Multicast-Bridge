package cmd

import (
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"multicast-bridge/internal/config"
	"multicast-bridge/internal/logger"
)

// ChaosProxy simulates network impairments (loss, reordering, duplication) on a UDP channel.
type ChaosProxy struct {
	mu           sync.Mutex
	listenConn   *net.UDPConn
	targetAddr   *net.UDPAddr
	lossRate     float64 // 0.0 to 1.0
	reorderRate  float64 // 0.0 to 1.0
	dupRate      float64 // 0.0 to 1.0
	closed       bool
	reorderQueue [][]byte
}

func NewChaosProxy(listenPort int, targetPort int, lossRate, reorderRate, dupRate float64) (*ChaosProxy, error) {
	laddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", listenPort))
	if err != nil {
		return nil, err
	}
	taddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", targetPort))
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp4", laddr)
	if err != nil {
		return nil, err
	}
	return &ChaosProxy{
		listenConn:  conn,
		targetAddr:  taddr,
		lossRate:    lossRate,
		reorderRate: reorderRate,
		dupRate:     dupRate,
	}, nil
}

func (p *ChaosProxy) Start() {
	go func() {
		buf := make([]byte, 2048)
		for {
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				return
			}
			p.mu.Unlock()

			n, _, err := p.listenConn.ReadFromUDP(buf)
			if err != nil {
				return
			}

			pkt := make([]byte, n)
			copy(pkt, buf[:n])

			// 1. Packet Loss simulation
			if p.shouldDrop(p.lossRate) {
				continue // Drop packet
			}

			// 2. Duplication simulation
			if p.shouldDrop(p.dupRate) {
				_, _ = p.listenConn.WriteToUDP(pkt, p.targetAddr)
				_, _ = p.listenConn.WriteToUDP(pkt, p.targetAddr)
				continue
			}

			// 3. Reordering simulation (asymmetric delay to cause out-of-order delivery)
			if p.shouldDrop(p.reorderRate) {
				go func(delayPkt []byte) {
					// Delay by 50ms to ensure it arrives after subsequent packets
					time.Sleep(50 * time.Millisecond)
					_, _ = p.listenConn.WriteToUDP(delayPkt, p.targetAddr)
				}(pkt)
				continue
			}

			// Normal forwarding
			_, _ = p.listenConn.WriteToUDP(pkt, p.targetAddr)
		}
	}()
}

func (p *ChaosProxy) Close() {
	p.mu.Lock()
	p.closed = true
	p.listenConn.Close()
	p.mu.Unlock()
}

func (p *ChaosProxy) shouldDrop(rate float64) bool {
	if rate <= 0 {
		return false
	}
	if rate >= 1.0 {
		return true
	}
	var b [1]byte
	_, _ = rand.Read(b[:])
	val := float64(b[0]) / 255.0
	return val < rate
}

// TestScenario runs integration verification under various simulated networks.
func runFECChaosScenario(t *testing.T, encryptionEnabled bool, lossRate, reorderRate float64, testName string) {
	t.Logf("=== Running Scenario: %s (Encryption=%v, Loss=%v, Reorder=%v) ===", testName, encryptionEnabled, lossRate, reorderRate)
	
	// Wait extra time for sockets to fully release in OS
	time.Sleep(1 * time.Second)

	logFile := filepath.Join(t.TempDir(), fmt.Sprintf("test_chaos_%s.log", testName))
	if err := logger.Init("DEBUG", logFile); err != nil {
		t.Fatalf("Failed to init logger: %v", err)
	}
	defer logger.Close()

	ResetStats()

	// Ports setup with dynamic offsets based on nanoseconds to avoid overlaps/reservations on Windows
	basePort := 35000 + int((time.Now().UnixNano()/1000)%800)*10
	ctrlPort := basePort
	senderDataPort := basePort + 1
	proxyPort := basePort + 2
	receiverDataPort := basePort + 3
	mcastSendPort := basePort + 4
	mcastRecvPort := basePort + 5
	passphrase := "fec_chaos_secured_psk"

	// 1. Start Chaos Proxy
	// Sender streams data to proxyPort (52102), which forwards to receiverDataPort (52103) with impairments
	proxy, err := NewChaosProxy(proxyPort, receiverDataPort, lossRate, reorderRate, 0.0)
	if err != nil {
		t.Fatalf("Failed to create chaos proxy: %v", err)
	}
	proxy.Start()
	defer proxy.Close()

	// 2. Setup Configurations
	sendCfg := config.NewDefaultSendConfig()
	sendCfg.Multicast.Address = "127.0.0.1"
	sendCfg.Multicast.Port = mcastSendPort
	sendCfg.Multicast.Interface = "127.0.0.1"
	sendCfg.Unicast.ControlPort = ctrlPort
	sendCfg.SenderDataPort = senderDataPort // Bind source data socket explicitly
	sendCfg.Unicast.MaxSessions = 8
	sendCfg.KeepAlive.Interval = 1
	sendCfg.FEC.Enabled = true
	sendCfg.FEC.K = 8
	sendCfg.FEC.N = 10 // 2 parity packets (can recover up to 2 lost packets in a group of 10)
	sendCfg.Encryption.Enabled = encryptionEnabled
	sendCfg.Encryption.Passphrase = passphrase
	sendCfg.Log.File = logFile

	recvCfg := config.NewDefaultRecvConfig()
	recvCfg.Sender.Address = "127.0.0.1"
	recvCfg.Sender.ControlPort = ctrlPort
	recvCfg.Sender.DataPort = proxyPort // Stream goes to proxy!
	recvCfg.UnicastBindPort = receiverDataPort // Bind locally to receiverDataPort to avoid conflict!
	recvCfg.Multicast.Address = "127.0.0.1"
	recvCfg.Multicast.Port = mcastRecvPort
	recvCfg.Multicast.Interface = "127.0.0.1"
	recvCfg.KeepAlive.Interval = 1
	recvCfg.FEC.Enabled = true
	recvCfg.Encryption.Enabled = encryptionEnabled
	recvCfg.Encryption.Passphrase = passphrase
	recvCfg.Log.File = logFile

	// 3. Start Sender & Receiver
	t.Log("Starting Sender...")
	go runSendNormal(sendCfg)
	time.Sleep(500 * time.Millisecond)

	t.Log("Starting Receiver...")
	go runRecvNormal(recvCfg)
	time.Sleep(1500 * time.Millisecond) // Let handshake complete

	// 4. Setup Local Sockets for testing data transmission
	mcastAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", mcastSendPort))
	mcastConn, err := net.DialUDP("udp4", nil, mcastAddr)
	if err != nil {
		t.Fatalf("Failed to dial sender multicast: %v", err)
	}
	defer mcastConn.Close()

	recvMcastAddr, _ := net.ResolveUDPAddr("udp4", fmt.Sprintf("127.0.0.1:%d", mcastRecvPort))
	recvMcastConn, err := net.ListenUDP("udp4", recvMcastAddr)
	if err != nil {
		t.Fatalf("Failed to listen receiver multicast: %v", err)
	}
	defer recvMcastConn.Close()

	// 5. Send a block of packets (group size = 8 data packets)
	// We will send 8 packets. We expect to recover them even if some are lost due to FEC (8, 10).
	t.Log("Sending 8 data packets in a single FEC group block...")
	packets := make([]string, 8)
	for i := 0; i < 8; i++ {
		packets[i] = fmt.Sprintf("CHAOS-TEST-BLOCK-PKT-%d", i)
		_, err = mcastConn.Write([]byte(packets[i]))
		if err != nil {
			t.Fatalf("Failed to write packet %d: %v", i, err)
		}
		time.Sleep(10 * time.Millisecond) // Slight gap between sends
	}

	// 6. Receive recovered packets at destination
	// Because K=8, N=10, the receiver requires any 8 out of the 10 packets (data + parity) to reconstruct the entire group.
	// If reconstruction succeeds, we should receive all 8 distinct packets eventually.
	receivedPackets := make(map[string]bool)
	_ = recvMcastConn.SetReadDeadline(time.Now().Add(4 * time.Second))
	
	buf := make([]byte, 2048)
	startTime := time.Now()

	for len(receivedPackets) < 8 && time.Since(startTime) < 3*time.Second {
		n, _, err := recvMcastConn.ReadFrom(buf)
		if err != nil {
			break // Timeout or error
		}
		dataStr := string(buf[:n])
		receivedPackets[dataStr] = true
		t.Logf("-> Received at Destination: %s", dataStr)
	}

	CleanUpRecv()
	CleanUpSender()
	
	// Print summary
	t.Logf("Result: Recovered %d/8 distinct packets under loss/impairment environment.", len(receivedPackets))
	
	// Read and output chaos log warnings to check if FEC recovery actually occurred
	logBytes, err := os.ReadFile(logFile)
	if err == nil {
		logStr := string(logBytes)
		if strings.Contains(logStr, "FEC Reconstruct failed") {
			t.Log("Warning: FEC Reconstruct failed warning found in logs.")
		}
		if strings.Contains(logStr, "Permanent loss") {
			t.Log("Warning: Permanent loss warning found in logs.")
		}
	}

	// If loss rate is modest (e.g. 10% which is typical 1 packet lost out of 10),
	// FEC(8, 10) MUST easily recover ALL 8 packets with 100% success.
	if len(receivedPackets) < 8 {
		t.Errorf("FAIL: Failed to recover all packets. Only got %d/8. Check FEC & Encryption implementation.", len(receivedPackets))
	} else {
		t.Log("SUCCESS: All 8 packets successfully delivered and recovered under chaos conditions!")
	}
}

func TestIntegration_FEC_Chaos_Encryption_Disabled_With_Loss(t *testing.T) {
	// 10% packet loss (easily recoverable by K=8, N=10 FEC which recovers up to 2 lost packets)
	runFECChaosScenario(t, false, 0.10, 0.0, "EncDisabled_Loss10")
}

func TestIntegration_FEC_Chaos_Encryption_Enabled_With_Loss(t *testing.T) {
	// 10% packet loss + Encryption (Ensures outer plain header allows decryp-after-fec to succeed!)
	runFECChaosScenario(t, true, 0.10, 0.0, "EncEnabled_Loss10")
}

func TestIntegration_FEC_Chaos_Encryption_Enabled_With_Reordering_And_Loss(t *testing.T) {
	// 5% loss + 10% packet reordering (out-of-order) + Encryption
	// Reed-Solomon index-based buffering should align packets perfectly regardless of arrival order.
	runFECChaosScenario(t, true, 0.05, 0.10, "EncEnabled_Reorder10_Loss5")
}
