package cmd

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"multicast-bridge/internal/config"
	"multicast-bridge/internal/control"
	"multicast-bridge/internal/crypto"
	"multicast-bridge/internal/data"
	"multicast-bridge/internal/logger"
	"multicast-bridge/internal/multicast"
)

var (
	globalSessionManager *data.SessionManager
	globalMcastConn      *net.UDPConn
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
	if globalMcastConn != nil {
		logger.Infof("Closing multicast listener socket (IGMP Leave)...")
		globalMcastConn.Close()
	}
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
	sm := data.NewSessionManager(context.Background(), cfg.Unicast.MaxSessions, conn, cfg.FEC.Enabled, cfg.FEC.K, cfg.FEC.N, cfg.Encryption.Enabled, cfg.Encryption.Passphrase)
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
	
	// Create UDP socket with SO_REUSEADDR
	uconn, err := data.CreateUDPListener("udp4", localBindAddr, 0, 0)
	if err != nil {
		logger.Errorf(203, "Unicast listener bind failed (port conflict): %v", err)
		os.Exit(203)
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
	conn, err := multicast.ListenMulticast("udp4", ifi, mcastAddr)
	if err != nil {
		logger.Errorf(401, "Failed to join multicast group: %v", err)
		uconn.Close()
		os.Exit(401)
	}
	globalMcastConn = conn
	logger.Infof("[Startup Check] Step 5/10: IGMP Join execution... SUCCESS")

	// Step 6: Socket creation and buffer size setting
	logger.Infof("[Startup Check] Step 6/10: Socket buffer allocation...")
	if err := conn.SetReadBuffer(2 * 1024 * 1024); err != nil {
		logger.Errorf(403, "Failed to set multicast socket read buffer: %v", err)
		conn.Close()
		uconn.Close()
		os.Exit(403)
	}
	if err := uconn.SetReadBuffer(2 * 1024 * 1024); err != nil {
		logger.Errorf(403, "Failed to set unicast socket read buffer: %v", err)
		conn.Close()
		uconn.Close()
		os.Exit(403)
	}
	if err := uconn.SetWriteBuffer(2 * 1024 * 1024); err != nil {
		logger.Errorf(403, "Failed to set unicast socket write buffer: %v", err)
		conn.Close()
		uconn.Close()
		os.Exit(403)
	}
	logger.Infof("[Startup Check] Step 6/10: Socket buffer allocation... SUCCESS")

	logger.Infof("[Startup Check] Steps 7-9: Dynamic checks (Time sync [105], Version [102], Auth [101]) will be verified on receiver connections.")

	// Step 10: Startup forwarding process
	logger.Infof("[Startup Check] Step 10/10: Initializing forwarding engine...")

	// 5. Initialize SessionManager
	sm := data.NewSessionManager(context.Background(), cfg.Unicast.MaxSessions, uconn, cfg.FEC.Enabled, cfg.FEC.K, cfg.FEC.N, cfg.Encryption.Enabled, cfg.Encryption.Passphrase)
	globalSessionManager = sm

	defer func() {
		sm.CloseAll()
		globalSessionManager = nil
	}()

	// Start control packet listener
	go handleControlPackets(uconn, sm, cfg)

	// Start session timeout monitoring thread
	go monitorSessionTimeouts(sm, cfg)

	ifiName := "any"
	if ifi != nil {
		ifiName = ifi.Name
	}
	
	// Start stats reporting thread (Linux: SIGUSR1, Windows: timer)
	startStatsReporting("sender", cfg.StatsInterval)

	logger.Infof("[Startup Check] Step 10/10: Forwarding engine started successfully!")
	logger.Infof("Multicast listener and Unicast sender initialized. Group: %s, Interface: %s (IP: %s)", mcastAddr.String(), ifiName, localIP)
	logger.Infof("Service is running. Press Ctrl+C to stop.")

	// 6. Recv Loop from Multicast and Broadcast to unicast sessions
	buf := make([]byte, 2048)
	for {
		n, _, err := globalMcastConn.ReadFromUDP(buf)
		if err != nil {
			// Check if closed
			if globalMcastConn == nil {
				break
			}
			logger.Errorf(403, "Failed to read from multicast socket: %v", err)
			time.Sleep(100 * time.Millisecond)
			continue
		}

		// MTU Limit Check (1438 bytes)
		if n > 1438 {
			logger.Warnf(301, "Packet size exceeds MTU limits (%d bytes), discarded.", n)
			continue
		}

		// Broadcast to all active sessions
		sm.Broadcast(buf[:n])
	}
}

type pendingAuth struct {
	randA    []byte
	t1       int64
	dataPort uint16
	version  string
	ipAddr   string
}

func handleControlPackets(conn *net.UDPConn, sm *data.SessionManager, cfg *config.SendConfig) {
	buf := make([]byte, 1024)
	pendingAuths := make(map[string]pendingAuth)

	for {
		n, raddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			// Socket closed during graceful shutdown
			break
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

			if cfg.Encryption.Enabled {
				// Challenge-Response Authentication starts
				randA, err := crypto.GenerateRandomBytes(32)
				if err != nil {
					logger.Errorf(0, "Failed to generate random challenge: %v", err)
					continue
				}
				t1 := time.Now().UnixNano()
				pendingAuths[raddr.String()] = pendingAuth{
					randA:    randA,
					t1:       t1,
					dataPort: dataPort,
					version:  version,
					ipAddr:   ipAddr,
				}

				challengePayload := control.EncodeAuthChallengePayload(randA, t1)
				challengePacket := control.BuildPacket(control.TypeAuthChallenge, challengePayload)
				_, _ = conn.WriteToUDP(challengePacket, raddr)
				logger.Infof("Sent AUTH_CHALLENGE to %s", raddr.String())
			} else {
				// Normal registration (No encryption)
				targetIP := raddr.IP.String()
				if ipAddr != "" && ipAddr != "127.0.0.1" && ipAddr != "0.0.0.0" {
					targetIP = ipAddr
				}
				targetDataAddr, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", targetIP, dataPort))
				if err != nil {
					logger.Errorf(0, "Failed to resolve data target address: %v", err)
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

			// Verify HMAC
			if !crypto.VerifyHMAC(pending.randA, hmacVal, cfg.Encryption.Passphrase) {
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

			// Derive session key
			sessionKey := crypto.DeriveKey(cfg.Encryption.Passphrase, pending.randA)

			targetIP := raddr.IP.String()
			if pending.ipAddr != "" && pending.ipAddr != "127.0.0.1" && pending.ipAddr != "0.0.0.0" {
				targetIP = pending.ipAddr
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

		case control.TypeLeave:
			logger.Infof("Received LEAVE from %s", raddr.String())
			sm.RemoveSessionByControlAddr(raddr)
		}
	}
}

func monitorSessionTimeouts(sm *data.SessionManager, cfg *config.SendConfig) {
	interval := time.Duration(cfg.KeepAlive.Interval) * time.Second
	if interval <= 0 {
		interval = 1 * time.Second
	}
	timeoutDuration := interval * 3

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			sm.CheckTimeouts(timeoutDuration)
		}
	}
}
