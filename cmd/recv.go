package cmd

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	"multicast-bridge/internal/config"
	"multicast-bridge/internal/control"
	"multicast-bridge/internal/crypto"
	"multicast-bridge/internal/data"
	"multicast-bridge/internal/fec"
	"multicast-bridge/internal/logger"
	"multicast-bridge/internal/multicast"
	"time"
)

// StreamStats holds statistics about the received encapsulated packets.
type StreamStats struct {
	mu            sync.Mutex
	lastSeq       uint32
	hasLastSeq    bool
	lostPackets   uint64
	totalPackets  uint64
	minLatency    time.Duration
	maxLatency    time.Duration
	totalLatency  time.Duration
	latencyCount  uint64
	lastShiftWarn time.Time
}

var (
	globalStats      = &StreamStats{}
	globalSessionKey []byte
	globalSessionKeyMu sync.RWMutex
	globalFECAnalyzer  *fec.FECAnalyzer
)

// ResetStats resets the packet statistics.
func ResetStats() {
	globalStats.mu.Lock()
	defer globalStats.mu.Unlock()
	globalStats.lastSeq = 0
	globalStats.hasLastSeq = false
	globalStats.lostPackets = 0
	globalStats.totalPackets = 0
	globalStats.minLatency = 0
	globalStats.maxLatency = 0
	globalStats.totalLatency = 0
	globalStats.latencyCount = 0
	globalStats.lastShiftWarn = time.Time{}
}

// Update processes a packet sequence number and timestamp to track packet loss and latency.
func (s *StreamStats) Update(seq uint32, timestampNano int64) {
	nowNano := time.Now().UnixNano()
	latency := time.Duration(nowNano - timestampNano)

	if latency < 0 || latency > time.Second {
		s.mu.Lock()
		now := time.Now()
		if now.Sub(s.lastShiftWarn) > 10*time.Second {
			s.lastShiftWarn = now
			s.mu.Unlock()
			logger.Warnf(0, "Significant clock drift or out-of-order time detected. Latency measurement may be inaccurate. Measured: %v", latency)
		} else {
			s.mu.Unlock()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.totalPackets++

	if s.hasLastSeq {
		diff := seq - (s.lastSeq + 1)
		if diff > 0 && diff < 0x80000000 {
			s.lostPackets += uint64(diff)
			logger.Warnf(304, "Packet loss detected. Missing %d packets. Expected sequence %d, got %d.", diff, s.lastSeq+1, seq)
		}
	} else {
		s.hasLastSeq = true
	}
	s.lastSeq = seq

	s.totalLatency += latency
	s.latencyCount++
	if s.latencyCount == 1 {
		s.minLatency = latency
		s.maxLatency = latency
	} else {
		if latency < s.minLatency {
			s.minLatency = latency
		}
		if latency > s.maxLatency {
			s.maxLatency = latency
		}
	}
}

// GetStats returns the collected packet loss and latency statistics.
func (s *StreamStats) GetStats() (lost, total uint64, min, max, avg time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	lost = s.lostPackets
	total = s.totalPackets
	min = s.minLatency
	max = s.maxLatency
	if s.latencyCount > 0 {
		avg = s.totalLatency / time.Duration(s.latencyCount)
	}
	return
}

var (
	globalRecvConn          *net.UDPConn
	globalControlConn       *net.UDPConn
	globalSenderControlAddr *net.UDPAddr
	globalRecvCtx           context.Context
	globalRecvCancel        context.CancelFunc
)

// ExecuteRecv handles the execution of the receiver command.
func ExecuteRecv(args []string, version string) {
	fs := flag.NewFlagSet("recv", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to the configuration file (YAML)")
	sender := fs.String("sender", "", "Sender address and control port (e.g. 192.168.1.10:5100)")
	multicast := fs.String("multicast", "", "Multicast address and port to re-send (e.g. 239.0.0.1:5004)")
	iface := fs.String("interface", "", "Network interface to use (IP or name)")
	loopback := fs.Bool("loopback", false, "Enable loopback test mode")
	statsInterval := fs.Int("stats-interval", 10, "Interval in seconds to print stats (Windows only, default 10)")
	fecTest := fs.Bool("fec-test", false, "Enable passive FEC simulation to analyze optimal parameters")

	// Parse flags for 'recv' subcommand
	if err := fs.Parse(args); err != nil {
		fmt.Printf("Error parsing flags: %v\n", err)
		os.Exit(202)
	}

	var cfg *config.RecvConfig
	var err error

	if *configPath != "" {
		// Load config from file
		cfg, err = config.LoadRecvConfig(*configPath)
		if err != nil {
			fmt.Println(err.Error())
			if strings.Contains(err.Error(), "[201]") {
				os.Exit(201)
			}
			os.Exit(202)
		}
	} else {
		// Minimum setup from command line arguments
		cfg = config.NewDefaultRecvConfig()

		if *sender != "" {
			host, portStr, err := net.SplitHostPort(*sender)
			if err != nil {
				fmt.Println("[202] invalid sender argument format (must be IP:port)")
				os.Exit(202)
			}
			port, err := strconv.Atoi(portStr)
			if err != nil || port < 1 || port > 65535 {
				fmt.Println("[202] invalid sender port number")
				os.Exit(202)
			}
			cfg.Sender.Address = host
			cfg.Sender.ControlPort = port
		}

		if *multicast != "" {
			host, portStr, err := net.SplitHostPort(*multicast)
			if err != nil {
				fmt.Println("[202] invalid multicast argument format (must be IP:port)")
				os.Exit(202)
			}
			port, err := strconv.Atoi(portStr)
			if err != nil || port < 1 || port > 65535 {
				fmt.Println("[202] invalid multicast port number")
				os.Exit(202)
			}
			cfg.Multicast.Address = host
			cfg.Multicast.Port = port
		}

		if *iface != "" {
			cfg.Multicast.Interface = *iface
		}
		
		if *statsInterval != 10 {
			cfg.StatsInterval = *statsInterval
		}
		
		if *fecTest {
			cfg.FEC.Test = *fecTest
		}

		// Set default interface to 127.0.0.1 for loopback if empty
		if *loopback && cfg.Multicast.Interface == "" {
			cfg.Multicast.Interface = "127.0.0.1"
		}

		// Validate the built config
		if err := cfg.Validate(); err != nil {
			fmt.Printf("[202] configuration validation failed: %v\n", err)
			os.Exit(202)
		}
	}

	// Initialize Logger
	if err := logger.Init(cfg.Log.Level, cfg.Log.File); err != nil {
		fmt.Printf("Failed to initialize logger: %v\n", err)
		os.Exit(403)
	}

	logger.Infof("multicast-bridge receiver (version %s) is starting...", version)

	if *loopback {
		logger.Infof("[LOOPBACK] Starting in loopback receiver testing mode...")
		// Receivers binds to local data port (5101) to accept dummy forwarding data
		localBindAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Sender.DataPort)
		go runRecvLoopback(localBindAddr)
	} else {
		logger.Infof("Sender address: %s (control port: %d)", cfg.Sender.Address, cfg.Sender.ControlPort)
		logger.Infof("Re-send multicast address: %s:%d (interface: %s)", cfg.Multicast.Address, cfg.Multicast.Port, cfg.Multicast.Interface)
		go runRecvNormal(cfg)
	}
}

// CleanUpRecv is called during Graceful Shutdown to release receiver resources.
func CleanUpRecv() {
	if globalRecvCancel != nil {
		globalRecvCancel()
	}
	if globalControlConn != nil {
		logger.Infof("Sending LEAVE packet to sender (IGMP Leave simulation)...")
		globalSessionKeyMu.RLock()
		key := globalSessionKey
		globalSessionKeyMu.RUnlock()
		_ = writeControlPacket(globalControlConn, control.TypeLeave, nil, key)
		globalControlConn.Close()
		globalControlConn = nil
	}
	if globalRecvConn != nil {
		globalRecvConn.Close()
		globalRecvConn = nil
	}
	removePIDFile()
}

func runRecvLoopback(bindAddr string) {
	// Create local UDP Listener (SO_REUSEADDR enabled)
	conn, err := data.CreateUDPListener("udp4", bindAddr, 0, 0)
	if err != nil {
		logger.Errorf(203, "Loopback listener bind failed: %v", err)
		return
	}
	globalRecvConn = conn
	defer conn.Close()

	logger.Infof("[LOOPBACK] Listening for dummy forwarding data on %s", bindAddr)

	buf := make([]byte, 2048)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			// Connection closed or other error
			return
		}
		logger.Infof("[LOOPBACK] Received packet from %s: %s", addr.String(), string(buf[:n]))
	}
}

func runRecvNormal(cfg *config.RecvConfig) {
	writePIDFile()
	logger.Infof("[Startup Check] Starting 10-step startup checklist for Receiver...")

	// Step 1: Config file availability verified during ExecuteRecv
	logger.Infof("[Startup Check] Step 1/10: Configuration file availability... SUCCESS")

	// Step 2: Configuration validation verified during ExecuteRecv
	logger.Infof("[Startup Check] Step 2/10: Configuration values validation... SUCCESS")

	// Step 3: Resolve network interface
	logger.Infof("[Startup Check] Step 3/10: Network interface validation...")
	ifi, err := multicast.ResolveInterface(cfg.Multicast.Interface)
	if err != nil {
		logger.Errorf(401, "Interface resolution failed: %v", err)
		os.Exit(401)
	}

	// Resolve local interface IP to bind transmission socket
	ifiIP, err := multicast.ResolveInterfaceIP(ifi)
	if err != nil {
		logger.Errorf(401, "Failed to resolve interface IP: %v", err)
		os.Exit(401)
	}
	logger.Infof("[Startup Check] Step 3/10: Network interface validation... SUCCESS")

	// Step 4: Verify control and data port availability
	logger.Infof("[Startup Check] Step 4/10: Control and Data port availability check...")
	localIP := "0.0.0.0"
	localBindAddr := fmt.Sprintf("%s:%d", localIP, cfg.Sender.DataPort)
	
	// Create UDP Unicast listener socket to accept forwarded data
	uconn, err := data.CreateUDPListener("udp4", localBindAddr, 0, 0)
	if err != nil {
		logger.Errorf(203, "Unicast listener bind failed (port conflict): %v", err)
		os.Exit(203)
	}
	globalRecvConn = uconn
	logger.Infof("[Startup Check] Step 4/10: Control and Data port availability check... SUCCESS")

	// Step 5: Listen on Multicast UDP (IGMP Join simulation / Multicast Outbound creation)
	logger.Infof("[Startup Check] Step 5/10: Multicast outbound channel validation...")
	mcastAddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", cfg.Multicast.Address, cfg.Multicast.Port))
	if err != nil {
		logger.Errorf(204, "Invalid multicast address format: %v", err)
		uconn.Close()
		os.Exit(202)
	}

	laddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:0", ifiIP))
	if err != nil {
		logger.Errorf(403, "Failed to resolve local outbound address: %v", err)
		uconn.Close()
		os.Exit(403)
	}

	mcastSendConn, err := net.DialUDP("udp4", laddr, mcastAddr)
	if err != nil {
		logger.Errorf(403, "Failed to create multicast outbound socket: %v", err)
		uconn.Close()
		os.Exit(403)
	}
	logger.Infof("[Startup Check] Step 5/10: Multicast outbound channel validation... SUCCESS")

	// Step 6: Socket creation and buffer size setting
	logger.Infof("[Startup Check] Step 6/10: Socket buffer allocation...")
	if err := uconn.SetReadBuffer(2 * 1024 * 1024); err != nil {
		logger.Errorf(403, "Failed to set unicast socket read buffer: %v", err)
		mcastSendConn.Close()
		uconn.Close()
		os.Exit(403)
	}
	if err := uconn.SetWriteBuffer(2 * 1024 * 1024); err != nil {
		logger.Errorf(403, "Failed to set unicast socket write buffer: %v", err)
		mcastSendConn.Close()
		uconn.Close()
		os.Exit(403)
	}
	if err := mcastSendConn.SetWriteBuffer(2 * 1024 * 1024); err != nil {
		logger.Errorf(403, "Failed to set multicast socket write buffer: %v", err)
		mcastSendConn.Close()
		uconn.Close()
		os.Exit(403)
	}
	logger.Infof("[Startup Check] Step 6/10: Socket buffer allocation... SUCCESS")

	// Explicitly set TTL=1 on the multicast outbound socket
	if err := multicast.SetMulticastTTL(mcastSendConn, 1); err != nil {
		logger.Warnf(0, "Failed to set multicast TTL to 1: %v. Continuing...", err)
	}

	logger.Infof("[Startup Check] Steps 7-9: Dynamic checks (Time sync [105], Version [102], Auth [101]) will be verified on handshake with Sender.")

	// Step 10: Startup forwarding process
	logger.Infof("[Startup Check] Step 10/10: Initializing registration flow...")
	senderAddrStr := fmt.Sprintf("%s:%d", cfg.Sender.Address, cfg.Sender.ControlPort)
	senderControlAddr, err := net.ResolveUDPAddr("udp4", senderAddrStr)
	if err != nil {
		logger.Errorf(0, "Failed to resolve sender control address: %v", err)
		os.Exit(202)
	}
	globalSenderControlAddr = senderControlAddr

	// 8. Start dynamic control plane loop (reconnection / registration / keep-alive)
	globalRecvCtx, globalRecvCancel = context.WithCancel(context.Background())
	
	var fecMu sync.RWMutex
	var fecManager *fec.FECManager

	triggerReconnect := make(chan struct{}, 1)
	go func() {
		for {
			if globalControlConn != nil {
				globalControlConn.Close()
			}
			select {
			case <-globalRecvCtx.Done():
				return
			default:
			}
			cconn, err := net.DialUDP("udp4", nil, globalSenderControlAddr)
			if err != nil {
				logger.Errorf(403, "Failed to dial control socket: %v. Retrying in 2 seconds...", err)
				select {
				case <-globalRecvCtx.Done():
					return
				case <-time.After(2 * time.Second):
				}
				continue
			}
			globalControlConn = cconn

			k, n, key, err := registerWithSender(globalRecvCtx, globalControlConn, globalSenderControlAddr, cfg, "0.1.0-draft", ifiIP)
			if err != nil {
				logger.Warnf(0, "Registration failed: %v. Entering exponential backoff...", err)
				
				backoff := 1 * time.Second
				for {
					select {
					case <-globalRecvCtx.Done():
						return
					case <-time.After(backoff):
					}
					logger.Infof("Attempting registration retry after backoff...")

					if globalControlConn != nil {
						globalControlConn.Close()
					}
					cconn, err = net.DialUDP("udp4", nil, globalSenderControlAddr)
					if err == nil {
						globalControlConn = cconn
						k, n, key, err = registerWithSender(globalRecvCtx, globalControlConn, globalSenderControlAddr, cfg, "0.1.0-draft", ifiIP)
						if err == nil {
							break // Success
						}
					}
					
					backoff *= 2
					if backoff > 16*time.Second {
						backoff = 16 * time.Second
					}
				}
			}

			globalSessionKeyMu.Lock()
			globalSessionKey = key
			globalSessionKeyMu.Unlock()

			select {
			case <-globalRecvCtx.Done():
				return
			default:
			}

			// Initialize or reset FEC manager
			fecMu.Lock()
			if cfg.FEC.Enabled {
				fecManager = fec.NewFECManager(true, int(k), int(n))
				logger.Infof("FEC enabled: k=%d, n=%d", k, n)
			} else {
				fecManager = fec.NewFECManager(false, 0, 0)
				logger.Infof("FEC disabled by config.")
			}
			fecMu.Unlock()

			sessionCtx, sessionCancel := context.WithCancel(globalRecvCtx)
			
			globalSessionKeyMu.RLock()
			keyCopy := globalSessionKey
			globalSessionKeyMu.RUnlock()

			go keepAliveLoop(sessionCtx, globalControlConn, cfg.KeepAlive.Interval, keyCopy)
			go listenForDisconnect(sessionCtx, globalControlConn, triggerReconnect, cfg.KeepAlive.Interval, keyCopy)
			
			fecMu.RLock()
			fm := fecManager
			fecMu.RUnlock()
			if cfg.FEC.Enabled && fm != nil {
				go func(ctx context.Context, mgr *fec.FECManager) {
					ticker := time.NewTicker(1 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-ticker.C:
							mgr.CleanUpTimeouts(2 * time.Duration(cfg.KeepAlive.Interval) * time.Second)
						}
					}
				}(sessionCtx, fm)
			}

			select {
			case <-globalRecvCtx.Done():
				sessionCancel()
				return
			case <-triggerReconnect:
			}
			sessionCancel()

			logger.Infof("Disconnected. Waiting 2 seconds before reconnection attempt...")
			select {
			case <-globalRecvCtx.Done():
				return
			case <-time.After(2 * time.Second):
			}
		}
	}()

	ifiName := "any"
	if ifi != nil {
		ifiName = ifi.Name
	}
	// Initialize FEC simulation analyzer if enabled
	globalFECAnalyzer = fec.NewFECAnalyzer(cfg.FEC.Test)
	if cfg.FEC.Test {
		logger.Infof("FEC Analyzer simulation started. Testing optimal settings passively...")
	}

	// Start stats reporting thread (Linux: SIGUSR1, Windows: timer)
	startStatsReporting("receiver", cfg.StatsInterval)

	logger.Infof("Unicast listener and Multicast forwarding initialized. Listener: %s, Multicast out: %s on %s (IP: %s)", localBindAddr, mcastAddr.String(), ifiName, ifiIP)
	logger.Infof("Service is running. Press Ctrl+C to stop.")

	// 9. Receive from unicast and send to multicast group
	buf := make([]byte, 2048)
	for {
		n, _, err := uconn.ReadFromUDP(buf)
		if err != nil {
			// Check if closed
			if globalRecvConn == nil {
				break
			}
			logger.Errorf(403, "Failed to read from unicast socket: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		var payloadBytes []byte
		globalSessionKeyMu.RLock()
		key := globalSessionKey
		globalSessionKeyMu.RUnlock()

		if cfg.Encryption.Enabled && len(key) > 0 {
			plaintext, decErr := crypto.DecryptGCM(buf[:n], key)
			if decErr != nil {
				logger.Warnf(0, "Failed to decrypt data packet: %v", decErr)
				continue
			}
			payloadBytes = plaintext
		} else {
			payloadBytes = buf[:n]
		}

		if len(payloadBytes) < data.HeaderSize {
			logger.Warnf(0, "Received packet too short for encapsulation header: %d bytes", len(payloadBytes))
			continue
		}

		// Deserialize encapsulation header
		header, err := data.DeserializeHeader(payloadBytes[:data.HeaderSize])
		if err != nil {
			logger.Warnf(0, "Failed to deserialize encapsulation header: %v", err)
			continue
		}

		// Version check
		major := header.Version >> 4
		minor := header.Version & 0x0F

		if major != data.MajorVersion {
			logger.Errorf(102, "Major version mismatch. Expected %d, got %d. Disconnecting...", data.MajorVersion, major)
			if globalControlConn != nil {
				globalControlConn.Close()
				select {
				case triggerReconnect <- struct{}{}:
				default:
				}
			}
			continue
		}

		if minor != data.MinorVersion {
			logger.Warnf(106, "Minor version mismatch. Expected %d, got %d. Continuing...", data.MinorVersion, minor)
		}

		// Update stream statistics
		globalStats.Update(header.SeqNum, header.Timestamp)
		logger.GlobalReceiverStats.AddPacket(header.SeqNum, header.Timestamp, len(payloadBytes)-data.HeaderSize)
		if globalFECAnalyzer != nil {
			globalFECAnalyzer.RecordPacket(header.SeqNum)
		}

		// FEC Processing
		fecMu.RLock()
		fm := fecManager
		fecMu.RUnlock()

		var packetsToRelease [][]byte
		if fm != nil {
			isFEC := (header.FECInfo & 0x8000) != 0
			groupNum := uint8((header.FECInfo >> 8) & 0x7F)
			index := int(header.FECInfo & 0xFF)

			var fecErr error
			packetsToRelease, fecErr = fm.AddPacket(payloadBytes, isFEC, groupNum, index)
			if fecErr != nil {
				// Reconstruct failed is already logged in fm.AddPacket
			}
		} else {
			packetsToRelease = [][]byte{payloadBytes}
		}

		for _, pkt := range packetsToRelease {
			if len(pkt) < data.HeaderSize {
				continue
			}
			h, err := data.DeserializeHeader(pkt[:data.HeaderSize])
			if err != nil {
				continue
			}
			if len(pkt) < data.HeaderSize+int(h.PayloadLen) {
				continue
			}
			payload := pkt[data.HeaderSize : data.HeaderSize+int(h.PayloadLen)]
			if len(payload) > 0 {
				_, err = mcastSendConn.Write(payload)
				if err != nil {
					logger.Errorf(403, "Failed to re-send multicast packet: %v", err)
				}
			}
		}
	}
}

func registerWithSender(ctx context.Context, conn *net.UDPConn, senderAddr *net.UDPAddr, cfg *config.RecvConfig, version string, ipAddr string) (k uint8, n uint8, key []byte, err error) {
	regPayload := control.EncodeRegisterPayload(uint16(cfg.Sender.DataPort), version, ipAddr)
	regPacket := control.BuildPacket(control.TypeRegister, regPayload)

	buf := make([]byte, 1024)
	for attempt := 1; attempt <= 3; attempt++ {
		select {
		case <-ctx.Done():
			return 0, 0, nil, ctx.Err()
		default:
		}

		logger.Infof("Sending REGISTER packet (attempt %d/3)...", attempt)
		_, err := conn.Write(regPacket)
		if err != nil {
			logger.Warnf(0, "Failed to write REGISTER packet: %v", err)
			select {
			case <-ctx.Done():
				return 0, 0, nil, ctx.Err()
			case <-time.After(1 * time.Second):
			}
			continue
		}

		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		nRead, err := conn.Read(buf)
		if err == nil {
			packet, err := control.ParsePacket(buf[:nRead])
			if err == nil {
				if packet.Type == control.TypeAuthChallenge && cfg.Encryption.Enabled {
					randA, t1, err := control.DecodeAuthChallengePayload(packet.Payload)
					if err == nil {
						t2 := time.Now().UnixNano()
						sessionKey := crypto.DeriveKey(cfg.Encryption.Passphrase, randA)
						hmacVal := crypto.ComputeHMAC(randA, cfg.Encryption.Passphrase)
						t3 := time.Now().UnixNano()

						respPayload := control.EncodeAuthResponsePayload(hmacVal, t2, t3)
						respPacket := control.BuildPacket(control.TypeAuthResponse, respPayload)
						_, _ = conn.Write(respPacket)
						logger.Infof("Sent AUTH_RESPONSE, waiting for AUTH_RESULT...")

						// Wait for AuthResult
						_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
						nResult, err := conn.Read(buf)
						if err == nil {
							resPacket, err := control.ParsePacket(buf[:nResult])
							if err == nil && resPacket.Type == control.TypeAuthResult {
								status, t4, kAck, nAck, err := control.DecodeAuthResultPayload(resPacket.Payload)
								if err == nil {
									if status == control.StatusSuccess {
										logger.Infof("Successfully authenticated and registered with sender!")
										_ = conn.SetReadDeadline(time.Time{}) // Clear deadline

										// Calculate clock drift
										offset := ((t2 - t1) + (t3 - t4)) / 2
										absOffset := offset
										if absOffset < 0 {
											absOffset = -absOffset
										}
										if absOffset > 100*int64(time.Millisecond) {
											logger.Warnf(105, "Clock drift warning [105]: offset between receiver and sender is %v (> 100ms)", time.Duration(offset))
										}

										return kAck, nAck, sessionKey, nil
									} else {
										logger.Errorf(101, "Authentication failed (wrong passphrase or rejected by sender).")
										return 0, 0, nil, fmt.Errorf("[101] authentication failed")
									}
								}
							}
						}
					}
				} else if packet.Type == control.TypeRegisterAck && !cfg.Encryption.Enabled {
					status, kAck, nAck, err := control.DecodeRegisterAckPayload(packet.Payload)
					if err == nil {
						if status == control.StatusSuccess {
							logger.Infof("Successfully registered with sender! FEC Parameters: k=%d, n=%d", kAck, nAck)
							_ = conn.SetReadDeadline(time.Time{}) // Clear deadline
							return kAck, nAck, nil, nil
						} else {
							logger.Errorf(104, "Registration rejected by sender (max_sessions reached or other).")
							return 0, 0, nil, fmt.Errorf("[104] registration rejected")
						}
					}
				}
			}
		}
		logger.Warnf(0, "Timeout or invalid packet waiting for response, retrying...")
	}
	return 0, 0, nil, fmt.Errorf("failed to register after 3 attempts")
}

func keepAliveLoop(ctx context.Context, conn *net.UDPConn, intervalSec int, key []byte) {
	interval := time.Duration(intervalSec) * time.Second
	if interval <= 0 {
		interval = 1 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			err := writeControlPacket(conn, control.TypeKeepAlive, nil, key)
			if err != nil {
				logger.Warnf(0, "Failed to send KeepAlive packet: %v", err)
			}
		}
	}
}

func listenForDisconnect(ctx context.Context, conn *net.UDPConn, triggerReconnect chan struct{}, intervalSec int, key []byte) {
	interval := time.Duration(intervalSec) * time.Second
	if interval <= 0 {
		interval = 1 * time.Second
	}
	timeoutDuration := interval * 3

	buf := make([]byte, 1024)
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Set timeout to detect silent connection drop (KeepAlive interval * 3)
		_ = conn.SetReadDeadline(time.Now().Add(timeoutDuration))
		n, err := conn.Read(buf)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			logger.Warnf(0, "Control channel connection lost or timeout: %v. Triggering reconnect...", err)
			select {
			case triggerReconnect <- struct{}{}:
			default:
			}
			return
		}

		var payloadBytes []byte
		if len(key) > 0 {
			plaintext, decErr := crypto.DecryptGCM(buf[:n], key)
			if decErr != nil {
				logger.Warnf(0, "Failed to decrypt control packet in listenForDisconnect: %v", decErr)
				continue
			}
			payloadBytes = plaintext
		} else {
			payloadBytes = buf[:n]
		}

		packet, err := control.ParsePacket(payloadBytes)
		if err == nil {
			if packet.Type == control.TypeDisconnect {
				logger.Warnf(0, "Received DISCONNECT packet from sender. Triggering reconnect...")
				select {
				case triggerReconnect <- struct{}{}:
				default:
				}
				return
			}
		}
	}
}

// writeControlPacket helper sends control packet, GCM encrypted if key is present
func writeControlPacket(conn *net.UDPConn, pType uint8, payload []byte, key []byte) error {
	plain := control.BuildPacket(pType, payload)
	if len(key) > 0 {
		ciphertext, err := crypto.EncryptGCM(plain, key)
		if err != nil {
			return err
		}
		_, err = conn.Write(ciphertext)
		return err
	}
	_, err := conn.Write(plain)
	return err
}
