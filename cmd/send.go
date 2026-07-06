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
	"time"

	"multicast-bridge/internal/config"
	"multicast-bridge/internal/control"
	"multicast-bridge/internal/crypto"
	"multicast-bridge/internal/data"
	"multicast-bridge/internal/logger"
	"multicast-bridge/internal/multicast"
)

var (
	globalSessionManager  *data.SessionManager
	globalMcastConn       *net.UDPConn
	globalDataConn        *net.UDPConn
	globalSendControlConn *net.UDPConn
	globalSendCtx         context.Context
	globalSendCancel      context.CancelFunc
	globalMcastConnMu     sync.RWMutex
)

// ExecuteSend handles the execution of the sender command.
func ExecuteSend(args []string, version string) {
	fs := flag.NewFlagSet("send", flag.ExitOnError)
	configPath := fs.String("config", "", "Path to the configuration file (YAML)")
	multicast := fs.String("multicast", "", "Multicast address and port (e.g. 239.0.0.1:5004)")
	target := fs.String("target", "", "Target unicast address and port (e.g. 192.168.1.20:5101)")
	iface := fs.String("interface", "", "Network interface to use (IP or name)")
	loopback := fs.Bool("loopback", false, "Enable loopback test mode with dummy data")
	statsInterval := fs.Int("stats-interval", 10, "Interval in seconds to print stats (Windows only, default 10)")

	// Parse flags for 'send' subcommand
	if err := fs.Parse(args); err != nil {
		fmt.Printf("Error parsing flags: %v\n", err)
		os.Exit(202)
	}

	var cfg *config.SendConfig
	var err error

	if *configPath != "" {
		// Load config from file
		cfg, err = config.LoadSendConfig(*configPath)
		if err != nil {
			fmt.Println(err.Error())
			if strings.Contains(err.Error(), "[201]") {
				os.Exit(201)
			}
			os.Exit(202)
		}
	} else {
		// Minimum setup from command line arguments
		cfg = config.NewDefaultSendConfig()

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

		if *target != "" {
			_, portStr, err := net.SplitHostPort(*target)
			if err != nil {
				fmt.Println("[202] invalid target argument format (must be IP:port)")
				os.Exit(202)
			}
			port, err := strconv.Atoi(portStr)
			if err != nil || port < 1 || port > 65535 {
				fmt.Println("[202] invalid target port number")
				os.Exit(202)
			}
			cfg.Unicast.DataPort = port
		}

		if *iface != "" {
			cfg.Multicast.Interface = *iface
		}
		
		if *statsInterval != 10 {
			cfg.StatsInterval = *statsInterval
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

	logger.Infof("multicast-bridge sender (version %s) is starting...", version)

	if *loopback {
		logger.Infof("[LOOPBACK] Starting in loopback testing mode...")
		targetAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Unicast.DataPort)
		if *target != "" {
			targetAddr = *target
		}
		go runSendLoopback(cfg, targetAddr)
	} else {
		logger.Infof("Normal mode: Multicast address: %s:%d (interface: %s)", cfg.Multicast.Address, cfg.Multicast.Port, cfg.Multicast.Interface)
		logger.Infof("Unicast control port: %d, data port: %d", cfg.Unicast.ControlPort, cfg.Unicast.DataPort)
		go runSendNormal(cfg)
	}
}

// CleanUpSender is called during Graceful Shutdown to release sender resources.
func CleanUpSender() {
	if globalSendCancel != nil {
		globalSendCancel()
	}

	globalMcastConnMu.Lock()
	if globalMcastConn != nil {
		logger.Infof("Closing multicast listener socket (IGMP Leave)...")
		globalMcastConn.Close()
		globalMcastConn = nil
	}
	if globalDataConn != nil {
		logger.Infof("Closing unicast data sending socket...")
		globalDataConn.Close()
		globalDataConn = nil
	}
	if globalSendControlConn != nil {
		logger.Infof("Closing unicast control listening socket...")
		globalSendControlConn.Close()
		globalSendControlConn = nil
	}
	globalMcastConnMu.Unlock()
	if globalSessionManager != nil {
		globalSessionManager.CloseAll()
	}
	removePIDFile()
}

func runSendLoopback(cfg *config.SendConfig, targetAddr string) {
	// Create UDP connection (sender unicast socket)
	localBindAddr := fmt.Sprintf("127.0.0.1:%d", cfg.Unicast.ControlPort)
	// We bind local port using CreateUDPListener to test SO_REUSEADDR
	conn, err := data.CreateUDPListener("udp4", localBindAddr, 0, 0)
	if err != nil {
		logger.Errorf(203, "Loopback bind failed: %v", err)
		return
	}
	defer conn.Close()

	// Initialize SessionManager with max 8 sessions
	sm := data.NewSessionManager(context.Background(), cfg.Unicast.MaxSessions, conn, cfg.FEC.Enabled, cfg.FEC.K, cfg.FEC.N, cfg.Encryption.Enabled)
	globalSessionManager = sm
	defer sm.CloseAll()

	raddr, err := net.ResolveUDPAddr("udp4", targetAddr)
	if err != nil {
		logger.Errorf(0, "Failed to resolve target: %v", err)
		return
	}

	// Add test session with small queue size 10 to test buffer overflow
	queueSize := 10
	if err := sm.AddSession(raddr, nil, queueSize, nil); err != nil {
		logger.Errorf(0, "Failed to add loopback session: %v", err)
		return
	}

	logger.Infof("[LOOPBACK] Target session registered: %s (Queue size: %d)", raddr.String(), queueSize)

	// --- 1. Test Buffer Overflow (Enqueue Burst) ---
	logger.Infof("[LOOPBACK] Triggering packet burst (50 packets) to test real-time buffer overflow...")
	for i := 1; i <= 50; i++ {
		payload := []byte(fmt.Sprintf("BURST-PACKET-%d", i))
		sm.Broadcast(payload)
	}
	time.Sleep(500 * time.Millisecond) // Let worker process what's left

	// --- 2. Periodic Dummy Data Transmission ---
	logger.Infof("[LOOPBACK] Entering periodic transmission loop (1 packet per 500ms)...")
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	index := 1
	for {
		<-ticker.C
		payload := []byte(fmt.Sprintf("PERIODIC-PACKET-%d", index))
		sm.Broadcast(payload)
		logger.Debugf("[LOOPBACK] Broadcasted packet index %d", index)
		index++
	}
}

func runSendNormal(cfg *config.SendConfig) {
	globalSendCtx, globalSendCancel = context.WithCancel(context.Background())
	writePIDFile()
	logger.Infof("[Startup Check] Starting 10-step startup checklist for Sender...")

	// Step 1: Config file availability is verified during ExecuteSend (error code 201)
	logger.Infof("[Startup Check] Step 1/10: Configuration file availability... SUCCESS")

	// Step 2: Configuration validation is verified during ExecuteSend (error code 202)
	logger.Infof("[Startup Check] Step 2/10: Configuration values validation... SUCCESS")

	// Step 3: Resolve network interface
	logger.Infof("[Startup Check] Step 3/10: Network interface validation...")
	ifi, err := multicast.ResolveInterface(cfg.Multicast.Interface)
	if err != nil {
		logger.Errorf(401, "Interface resolution failed: %v", err)
		os.Exit(401)
	}
	logger.Infof("[Startup Check] Step 3/10: Network interface validation... SUCCESS")

	// Step 4: Verify control and data port availability
	logger.Infof("[Startup Check] Step 4/10: Control and Data port availability check...")
	localIP := "0.0.0.0"
	if ip, err := multicast.ResolveInterfaceIP(ifi); err == nil {
		localIP = ip
	}
	localBindAddr := fmt.Sprintf("%s:%d", localIP, cfg.Unicast.ControlPort)
	
	// Create UDP socket for control plane with SO_REUSEADDR
	uconn, err := data.CreateUDPListener("udp4", localBindAddr, 0, 0)
	if err != nil {
		logger.Errorf(203, "Unicast control listener bind failed (port conflict): %v", err)
		os.Exit(203)
	}
	globalMcastConnMu.Lock()
	globalSendControlConn = uconn
	globalMcastConnMu.Unlock()

	// Apply DSCP (QoS) for Control Plane
	if cfg.ControlDSCP > 0 {
		if err := multicast.SetDSCP(uconn, cfg.ControlDSCP); err != nil {
			logger.Warnf(0, "Failed to set QoS DSCP %d on control socket: %v (Windows policies may restrict this)", cfg.ControlDSCP, err)
		} else {
			logger.Infof("Successfully set QoS DSCP %d on control socket", cfg.ControlDSCP)
		}
	}

	// Create independent UDP socket for data plane
	localDataBindAddr := fmt.Sprintf("%s:%d", localIP, cfg.SenderDataPort)
	dataConn, err := data.CreateUDPListener("udp4", localDataBindAddr, 0, 0)
	if err != nil {
		logger.Errorf(203, "Unicast data socket bind failed: %v", err)
		uconn.Close()
		os.Exit(203)
	}
	globalMcastConnMu.Lock()
	globalDataConn = dataConn
	globalMcastConnMu.Unlock()

	// Apply DSCP (QoS) for Data Plane
	if cfg.DSCP > 0 {
		if err := multicast.SetDSCP(dataConn, cfg.DSCP); err != nil {
			logger.Warnf(0, "Failed to set QoS DSCP %d on data socket: %v (Windows policies may restrict this)", cfg.DSCP, err)
		} else {
			logger.Infof("Successfully set QoS DSCP %d on data socket", cfg.DSCP)
		}
	}

	logger.Infof("[Startup Check] Step 4/10: Control and Data port availability check... SUCCESS")

	// Step 5: Listen on Multicast UDP (IGMP Join)
	logger.Infof("[Startup Check] Step 5/10: IGMP Join execution...")
	mcastAddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", cfg.Multicast.Address, cfg.Multicast.Port))
	if err != nil {
		logger.Errorf(204, "Invalid multicast address format: %v", err)
		uconn.Close()
		os.Exit(202)
	}
	conn, err := multicast.ListenMulticast("udp4", ifi, mcastAddr, cfg.Multicast.SourceAddress)
	if err != nil {
		logger.Errorf(401, "Failed to join multicast group: %v", err)
		uconn.Close()
		os.Exit(401)
	}
	globalMcastConnMu.Lock()
	globalMcastConn = conn
	globalMcastConnMu.Unlock()
	logger.Infof("[Startup Check] Step 5/10: IGMP Join execution... SUCCESS")

	// Step 6: Socket creation and buffer size setting
	logger.Infof("[Startup Check] Step 6/10: Socket buffer allocation...")
	if err := conn.SetReadBuffer(cfg.SocketBufferSize); err != nil {
		logger.Errorf(403, "Failed to set multicast socket read buffer to %d: %v. Please adjust OS network limits (e.g. sysctl net.core.rmem_max).", cfg.SocketBufferSize, err)
		conn.Close()
		uconn.Close()
		os.Exit(403)
	}
	if err := uconn.SetReadBuffer(cfg.SocketBufferSize); err != nil {
		logger.Errorf(403, "Failed to set unicast socket read buffer to %d: %v. Please adjust OS network limits (e.g. sysctl net.core.rmem_max).", cfg.SocketBufferSize, err)
		conn.Close()
		uconn.Close()
		os.Exit(403)
	}
	if err := uconn.SetWriteBuffer(cfg.SocketBufferSize); err != nil {
		logger.Errorf(403, "Failed to set unicast socket write buffer to %d: %v. Please adjust OS network limits (e.g. sysctl net.core.wmem_max).", cfg.SocketBufferSize, err)
		conn.Close()
		uconn.Close()
		os.Exit(403)
	}
	logger.Infof("[Startup Check] Step 6/10: Socket buffer allocation... SUCCESS")

	logger.Infof("[Startup Check] Steps 7-9: Dynamic checks (Time sync [105], Version [102], Auth [101]) will be verified on receiver connections.")

	// Dynamic MTU calculation to prevent IP fragmentation
	// Outbound packet structure has plain headers.
	// For redundant (FEC) packets, the format is: [FEC Header (17B)] + [Data Header (17B)] + [Encrypted/Plain Payload] + [Encryption overhead]
	// Therefore, total network encapsulation overhead under FEC is: IP (20B) + UDP (8B) + FEC Header (17B) + Data Header (17B) + Encryption overhead
	linkMTU := cfg.MTU
	if linkMTU < 576 {
		linkMTU = 1500 // Fallback to standard MTU if uninitialized or invalid in tests
	}
	encryptionOverhead := 0
	if cfg.Encryption.Enabled {
		encryptionOverhead = 28 // AES-GCM (12B Nonce + 16B Auth Tag)
	}
	maxPayloadSize := linkMTU - 20 - 8 - 17 - 17 - encryptionOverhead

	logger.Infof("[Startup Check] Step 10/10: Initializing forwarding engine...")

	// 5. Initialize SessionManager (using independent dataConn for streaming)
	sm := data.NewSessionManager(context.Background(), cfg.Unicast.MaxSessions, dataConn, cfg.FEC.Enabled, cfg.FEC.K, cfg.FEC.N, cfg.Encryption.Enabled)
	globalSessionManager = sm

	defer func() {
		sm.CloseAll()
		globalSessionManager = nil
	}()

	// Start control packet listener
	go handleControlPackets(uconn, sm, cfg)

	// Start session timeout monitoring thread
	go monitorSessionTimeouts(sm.Context(), sm, cfg)

	ifiName := "any"
	if ifi != nil {
		ifiName = ifi.Name
	}
	
	// Start stats reporting thread (Linux: SIGUSR1, Windows: timer)
	startStatsReporting("sender", cfg.StatsInterval)

	logger.Infof("[Startup Check] Step 10/10: Forwarding engine started successfully!")
	logger.Infof("Multicast listener and Unicast sender initialized. Group: %s, Interface: %s (IP: %s)", mcastAddr.String(), ifiName, localIP)
	logger.Infof("Dynamic MTU configured: Link MTU=%d, Max Multicast Payload Size=%d bytes (Encryption overhead=%d bytes)", linkMTU, maxPayloadSize, encryptionOverhead)
	logger.Infof("Service is running. Press Ctrl+C to stop.")

	// 6. Recv Loop from Multicast and Broadcast to unicast sessions
	// Buffer must be large enough to hold any UDP payload (max 65535 bytes).
	// We pick the larger of (configured MTU + overhead) and the UDP maximum.
	bufSize := cfg.MTU + 128
	if bufSize < 65535 {
		bufSize = 65535 // Always at least 65535 to support jumbo frames and large packets
	}
	buf := make([]byte, bufSize)
	for {
		select {
		case <-globalSendCtx.Done():
			return
		default:
		}

		globalMcastConnMu.RLock()
		mconn := globalMcastConn
		globalMcastConnMu.RUnlock()

		if mconn == nil {
			break
		}

		n, _, err := mconn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-globalSendCtx.Done():
				return
			default:
			}
			logger.Errorf(403, "Failed to read from multicast socket: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// MTU Limit Check (avoid IP fragmentation based on dynamic maxPayloadSize)
		if n > maxPayloadSize {
			logger.Warnf(301, "Packet size (%d bytes) exceeds dynamic MTU limits (max payload %d bytes), discarded.", n, maxPayloadSize)
			continue
		}

		// Broadcast to all active sessions
		sm.Broadcast(buf[:n])
	}
}

type pendingAuth struct {
	randA     []byte
	t1        int64
	dataPort  uint16
	version   string
	ipAddr    string
	createdAt time.Time
	controlIP string
}

func handleControlPackets(conn *net.UDPConn, sm *data.SessionManager, cfg *config.SendConfig) {
	buf := make([]byte, 1024)
	pendingAuths := make(map[string]pendingAuth)
	lastRegisterTime := make(map[string]time.Time)

	for {
		n, raddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			// Socket closed during graceful shutdown
			break
		}

		// Clean up expired pending authentications (older than 1 minute) to prevent memory leaks/DoS
		now := time.Now()
		for addr, pending := range pendingAuths {
			if now.Sub(pending.createdAt) > 1*time.Minute {
				delete(pendingAuths, addr)
			}
		}

		var payloadBytes []byte
		session := sm.GetSessionByControlAddr(raddr)

		// Decrypt if encryption is enabled and session has a key
		decrypted := false
		if cfg.Encryption.Enabled && session != nil && len(session.Key) > 0 {
			plaintext, decErr := crypto.DecryptGCM(buf[:n], session.Key)
			if decErr == nil {
				payloadBytes = plaintext
				decrypted = true
			} else {
				logger.Warnf(0, "Failed to decrypt control packet from %s: %v", raddr.String(), decErr)
				continue // Ignore tampered/undecryptable packets after session is established
			}
		} else {
			payloadBytes = buf[:n]
		}

		packet, err := control.ParsePacket(payloadBytes)
		if err != nil {
			logger.Warnf(0, "Failed to parse control packet from %s: %v (decrypted=%v)", raddr.String(), err, decrypted)
			continue
		}

		switch packet.Type {
		case control.TypeRegister:
			dataPort, version, ipAddr, err := control.DecodeRegisterPayload(packet.Payload)
			if err != nil {
				logger.Warnf(0, "Failed to decode Register payload from %s: %v", raddr.String(), err)
				continue
			}

			logger.Infof("Received REGISTER from %s: version=%s, ip=%s, dataPort=%d", raddr.String(), version, ipAddr, dataPort)

			// Simple Rate-limiting and connection limit per IP
			ipStr := raddr.IP.String()
			if lastTime, exists := lastRegisterTime[ipStr]; exists && time.Since(lastTime) < 1*time.Second {
				logger.Warnf(0, "Rate limit exceeded for REGISTER from %s, ignoring", ipStr)
				continue
			}
			lastRegisterTime[ipStr] = time.Now()

			if cfg.Encryption.Enabled {
				// limit max pending auths per IP (max 1 pending auth per IP)
				pendingCount := 0
				for _, pending := range pendingAuths {
					if pending.controlIP == ipStr {
						pendingCount++
					}
				}
				if pendingCount >= 1 {
					logger.Warnf(0, "Too many pending authentications for IP %s, ignoring REGISTER", ipStr)
					continue
				}

				// Challenge-Response Authentication starts
				randA, err := crypto.GenerateRandomBytes(32)
				if err != nil {
					logger.Errorf(0, "Failed to generate random challenge: %v", err)
					continue
				}
				t1 := time.Now().UnixNano()
				pendingAuths[raddr.String()] = pendingAuth{
					randA:     randA,
					t1:        t1,
					dataPort:  dataPort,
					version:   version,
					ipAddr:    ipAddr,
					createdAt: time.Now(),
					controlIP: ipStr,
				}

				challengePayload := control.EncodeAuthChallengePayload(randA, t1)
				challengePacket := control.BuildPacket(control.TypeAuthChallenge, challengePayload)
				_, _ = conn.WriteToUDP(challengePacket, raddr)
				logger.Infof("Sent AUTH_CHALLENGE to %s", raddr.String())
			} else {
				// Normal registration (No encryption)
				targetIP := raddr.IP.String()
				if ipAddr != "" && ipAddr != "127.0.0.1" && ipAddr != "0.0.0.0" {
					if ipAddr != raddr.IP.String() {
						logger.Warnf(0, "REGISTER declared ipAddress (%s) does not match connection source IP (%s). Overwriting with connection IP to prevent IP spoofing (H5).", ipAddr, raddr.IP.String())
						targetIP = raddr.IP.String()
					} else {
						targetIP = ipAddr
					}
				}
				targetDataAddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", targetIP, dataPort))
				if err != nil {
					logger.Errorf(0, "Failed to resolve data target address: %v", err)
					continue
				}

				// Check if session already exists for this address to prevent DoS (C3)
				if existingS := sm.GetSessionByControlAddr(raddr); existingS != nil {
					sm.UpdateLastActiveByControlAddr(raddr)
					ackPayload := control.EncodeRegisterAckPayload(control.StatusSuccess, uint8(cfg.FEC.K), uint8(cfg.FEC.N))
					ackPacket := control.BuildPacket(control.TypeRegisterAck, ackPayload)
					_, _ = conn.WriteToUDP(ackPacket, raddr)
					continue
				}

				err = sm.AddSession(targetDataAddr, raddr, 100, nil)
				status := control.StatusSuccess
				if err != nil {
					status = control.StatusDenied
				}

				ackPayload := control.EncodeRegisterAckPayload(status, uint8(cfg.FEC.K), uint8(cfg.FEC.N))
				ackPacket := control.BuildPacket(control.TypeRegisterAck, ackPayload)
				_, _ = conn.WriteToUDP(ackPacket, raddr)
			}

		case control.TypeAuthResponse:
			if !cfg.Encryption.Enabled {
				logger.Warnf(0, "Received AUTH_RESPONSE from %s but encryption is disabled", raddr.String())
				continue
			}

			hmacVal, t2, t3, err := control.DecodeAuthResponsePayload(packet.Payload)
			if err != nil {
				logger.Warnf(0, "Failed to decode AuthResponse payload from %s: %v", raddr.String(), err)
				continue
			}

			pending, exists := pendingAuths[raddr.String()]
			if !exists {
				logger.Warnf(0, "No pending authentication found for %s", raddr.String())
				continue
			}

			// Derive session key (moved up to verify HMAC using it)
			sessionKey := crypto.DeriveKey(cfg.Encryption.Passphrase, pending.randA, cfg.Encryption.Iterations)

			// Verify HMAC
			if !crypto.VerifyHMAC(pending.randA, hmacVal, sessionKey) {
				logger.Errorf(101, "Authentication failed for %s (invalid HMAC)", raddr.String())
				
				// Send AuthResult with denied status
				resultPayload := control.EncodeAuthResultPayload(control.StatusDenied, 0, 0, 0)
				resultPacket := control.BuildPacket(control.TypeAuthResult, resultPayload)
				_, _ = conn.WriteToUDP(resultPacket, raddr)
				
				delete(pendingAuths, raddr.String())
				continue
			}

			// HMAC verified!
			t4 := time.Now().UnixNano()
			offset := ((t2 - pending.t1) + (t3 - t4)) / 2
			absOffset := offset
			if absOffset < 0 {
				absOffset = -absOffset
			}
			if absOffset > 100*int64(time.Millisecond) {
				logger.Warnf(105, "Clock drift warning [105]: offset between sender and receiver %s is %v (> 100ms)", raddr.String(), time.Duration(offset))
			}

			targetIP := raddr.IP.String()
			if pending.ipAddr != "" && pending.ipAddr != "127.0.0.1" && pending.ipAddr != "0.0.0.0" {
				if pending.ipAddr != raddr.IP.String() {
					logger.Warnf(0, "AUTH_RESPONSE declared ipAddress (%s) does not match connection source IP (%s). Overwriting with connection IP to prevent IP spoofing (H5).", pending.ipAddr, raddr.IP.String())
					targetIP = raddr.IP.String()
				} else {
					targetIP = pending.ipAddr
				}
			}
			targetDataAddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", targetIP, pending.dataPort))
			if err != nil {
				logger.Errorf(0, "Failed to resolve data target address: %v", err)
				delete(pendingAuths, raddr.String())
				continue
			}

			// Add formal session
			err = sm.AddSession(targetDataAddr, raddr, 100, sessionKey)
			status := control.StatusSuccess
			if err != nil {
				status = control.StatusDenied
			}

			// Send AuthResult back
			resultPayload := control.EncodeAuthResultPayload(status, t4, uint8(cfg.FEC.K), uint8(cfg.FEC.N))
			resultPacket := control.BuildPacket(control.TypeAuthResult, resultPayload)
			_, _ = conn.WriteToUDP(resultPacket, raddr)

			delete(pendingAuths, raddr.String())
			logger.Infof("Authentication succeeded for %s! Session established.", raddr.String())

		case control.TypeKeepAlive:
			sm.UpdateLastActiveByControlAddr(raddr)
			
			// Send KeepAlive response back to receiver to prevent control channel timeout
			ackPacket := control.BuildPacket(control.TypeKeepAlive, nil)
			if cfg.Encryption.Enabled {
				session := sm.GetSessionByControlAddr(raddr)
				if session != nil && len(session.Key) > 0 {
					ciphertext, err := crypto.EncryptGCM(ackPacket, session.Key)
					if err == nil {
						_, _ = conn.WriteToUDP(ciphertext, raddr)
					}
				}
			} else {
				_, _ = conn.WriteToUDP(ackPacket, raddr)
			}


		case control.TypeLeave:
			logger.Infof("Received LEAVE from %s", raddr.String())
			sm.RemoveSessionByControlAddr(raddr)
		}
	}
}

func monitorSessionTimeouts(ctx context.Context, sm *data.SessionManager, cfg *config.SendConfig) {
	interval := time.Duration(cfg.KeepAlive.Interval) * time.Second
	if interval <= 0 {
		interval = 1 * time.Second
	}
	multiplier := cfg.KeepAlive.TimeoutMultiplier
	if multiplier <= 0 {
		multiplier = 3
	}
	timeoutDuration := interval * time.Duration(multiplier)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sm.CheckTimeouts(timeoutDuration)
		}
	}
}
