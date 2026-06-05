package data

import (
	"context"
	"crypto/cipher"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	"multicast-bridge/internal/crypto"
	"multicast-bridge/internal/fec"
	"multicast-bridge/internal/logger"
)

// Session represents a connection from a receiver.
type Session struct {
	ID          string
	Addr        *net.UDPAddr
	ControlAddr *net.UDPAddr
	Queue       *PacketQueue
	Ctx         context.Context
	Cancel      context.CancelFunc
	LastActive  time.Time
	SeqNum      uint32
	Key         []byte // AES key derived during handshake
	AEAD        cipher.AEAD // Cached AEAD instance for zero-allocation AES-GCM
}

// SessionManager manages active forwarding sessions.
type SessionManager struct {
	mu                sync.Mutex
	sessions          map[string]*Session
	maxSessions       int
	ctx               context.Context
	cancel            context.CancelFunc
	conn              *net.UDPConn // Unicast data socket used to send packets
	fecEnabled        bool
	fecK              int
	fecN              int
	fecEncoder        reedsolomon.Encoder // Cached reedsolomon.Encoder
	encryptionEnabled bool
}

// NewSessionManager creates a new SessionManager.
func NewSessionManager(parentCtx context.Context, maxSessions int, conn *net.UDPConn, fecEnabled bool, fecK, fecN int, encryptionEnabled bool) *SessionManager {
	ctx, cancel := context.WithCancel(parentCtx)

	var fecEnc reedsolomon.Encoder
	if fecEnabled && fecK > 0 && fecN > fecK {
		var err error
		fecEnc, err = reedsolomon.New(fecK, fecN-fecK)
		if err != nil {
			logger.Errorf(403, "Failed to initialize reedsolomon encoder for sender: %v. Disabling FEC...", err)
			fecEnabled = false
		}
	} else {
		fecEnabled = false
	}

	return &SessionManager{
		sessions:          make(map[string]*Session),
		maxSessions:       maxSessions,
		ctx:               ctx,
		cancel:            cancel,
		conn:              conn,
		fecEnabled:        fecEnabled,
		fecK:              fecK,
		fecN:              fecN,
		fecEncoder:        fecEnc,
		encryptionEnabled: encryptionEnabled,
	}
}

// Context returns the internal context of the SessionManager.
func (sm *SessionManager) Context() context.Context {
	return sm.ctx
}

// AddSession creates and starts a new session for the given receiver address.
func (sm *SessionManager) AddSession(addr *net.UDPAddr, controlAddr *net.UDPAddr, queueSize int, key []byte) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	select {
	case <-sm.ctx.Done():
		return fmt.Errorf("session manager is stopped")
	default:
	}

	keyStr := addr.String()
	if oldSession, exists := sm.sessions[keyStr]; exists {
		logger.Infof("Session already exists for %s. Re-registering session (stopping old worker)...", keyStr)
		delete(sm.sessions, keyStr)
		logger.GlobalSenderStats.SessionRemoved(keyStr)
		oldSession.Cancel()
		oldSession.Queue.Close()
	}

	if len(sm.sessions) >= sm.maxSessions {
		logger.Errorf(104, "Max sessions reached (%d). Connection rejected for %s", sm.maxSessions, keyStr)
		return fmt.Errorf("[104] max sessions reached")
	}

	var aead cipher.AEAD
	if len(key) > 0 {
		var err error
		aead, err = crypto.NewAEAD(key)
		if err != nil {
			return fmt.Errorf("failed to initialize AEAD for session: %w", err)
		}
	}

	sessionCtx, sessionCancel := context.WithCancel(sm.ctx)
	session := &Session{
		ID:          keyStr,
		Addr:        addr,
		ControlAddr: controlAddr,
		Queue:       NewPacketQueue(queueSize),
		Ctx:         sessionCtx,
		Cancel:      sessionCancel,
		LastActive:  time.Now(),
		Key:         key,
		AEAD:        aead,
	}

	sm.sessions[keyStr] = session
	logger.GlobalSenderStats.SessionJoined(keyStr)

	// Start the session sender worker goroutine
	go sm.sessionWorker(session)

	logger.Infof("Added new forwarding session: %s (total: %d/%d)", keyStr, len(sm.sessions), sm.maxSessions)
	return nil
}

// RemoveSession terminates and removes the session for the given key.
func (sm *SessionManager) RemoveSession(key string) {
	sm.mu.Lock()
	session, exists := sm.sessions[key]
	if !exists {
		sm.mu.Unlock()
		return
	}

	delete(sm.sessions, key)
	sm.mu.Unlock()
	logger.GlobalSenderStats.SessionRemoved(key)

	// Stop worker goroutine by canceling its context and closing the queue
	session.Cancel()
	session.Queue.Close()

	logger.Infof("Removed session: %s (remaining active: %d)", key, len(sm.sessions))
}

// Broadcast dispatches a packet to all active session queues.
func (sm *SessionManager) Broadcast(data []byte) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, session := range sm.sessions {
		// Deep copy packet data to prevent race conditions from shared buffer reuse in the main receive loop
		copiedData := make([]byte, len(data))
		copy(copiedData, data)

		pkt := &Packet{
			Data: copiedData,
			Addr: session.Addr,
		}
		// Enqueue is non-blocking (automatically drops oldest on overflow)
		session.Queue.Enqueue(pkt)
	}
}

// ActiveSessions returns a list of active session keys.
func (sm *SessionManager) ActiveSessions() []string {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	keys := make([]string, 0, len(sm.sessions))
	for k := range sm.sessions {
		keys = append(keys, k)
	}
	return keys
}

// CloseAll stops all active sessions and shuts down the manager.
func (sm *SessionManager) CloseAll() {
	sm.mu.Lock()
	sm.cancel() // Cancel parent context to signal all workers

	sessionsToClose := make([]*Session, 0, len(sm.sessions))
	for _, s := range sm.sessions {
		sessionsToClose = append(sessionsToClose, s)
	}
	sm.sessions = make(map[string]*Session) // Clear sessions map
	sm.mu.Unlock()

	// Stop all sessions cleanly
	for _, s := range sessionsToClose {
		s.Cancel()
		s.Queue.Close()
	}

	logger.Infof("Closed all forwarding sessions.")
}

// UpdateLastActive updates the last active timestamp of a session.
func (sm *SessionManager) UpdateLastActive(key string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if session, exists := sm.sessions[key]; exists {
		session.LastActive = time.Now()
		logger.GlobalSenderStats.SessionKeepAlive(key)
	}
}

// CheckTimeouts scans for sessions that have timed out and removes them.
func (sm *SessionManager) CheckTimeouts(timeoutDuration time.Duration) {
	sm.mu.Lock()
	var toRemove []string
	now := time.Now()
	for key, session := range sm.sessions {
		if now.Sub(session.LastActive) > timeoutDuration {
			toRemove = append(toRemove, key)
		}
	}
	sm.mu.Unlock()

	for _, key := range toRemove {
		logger.Warnf(302, "Session timeout detected (KeepAlive missing). Removing session: %s", key)
		logger.GlobalSenderStats.SessionTimeout(key)
		sm.RemoveSession(key)
	}
}

// UpdateLastActiveByControlAddr updates LastActive for a session matching the control address.
func (sm *SessionManager) UpdateLastActiveByControlAddr(controlAddr *net.UDPAddr) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, session := range sm.sessions {
		if session.ControlAddr != nil && session.ControlAddr.IP.Equal(controlAddr.IP) && session.ControlAddr.Port == controlAddr.Port {
			session.LastActive = time.Now()
			logger.GlobalSenderStats.SessionKeepAlive(session.ID)
			return
		}
	}
}

// RemoveSessionByControlAddr removes a session matching the control address.
func (sm *SessionManager) RemoveSessionByControlAddr(controlAddr *net.UDPAddr) {
	var targetKey string
	sm.mu.Lock()
	for key, session := range sm.sessions {
		if session.ControlAddr != nil && session.ControlAddr.IP.Equal(controlAddr.IP) && session.ControlAddr.Port == controlAddr.Port {
			targetKey = key
			break
		}
	}
	sm.mu.Unlock()

	if targetKey != "" {
		sm.RemoveSession(targetKey)
	}
}

// GetSessionByControlAddr returns the session matching the control address.
func (sm *SessionManager) GetSessionByControlAddr(controlAddr *net.UDPAddr) *Session {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for _, session := range sm.sessions {
		if session.ControlAddr != nil && session.ControlAddr.IP.Equal(controlAddr.IP) && session.ControlAddr.Port == controlAddr.Port {
			return session
		}
	}
	return nil
}


// sessionWorker is the per-session goroutine responsible for draining the packet queue
// and transmitting packets over the unicast socket.
func (sm *SessionManager) sessionWorker(s *Session) {
	defer func() {
		logger.Debugf("Session worker terminated for %s", s.ID)
	}()

	var groupNum uint16 = 0
	var groupPackets [][]byte

	if sm.fecEnabled && sm.fecK > 0 {
		groupPackets = make([][]byte, 0, sm.fecK)
	}

	for {
		select {
		case <-s.Ctx.Done():
			return
		default:
		}

		// Dequeue blocks until a packet is ready or queue is closed
		pkt, err := s.Queue.Dequeue()
		if err != nil {
			// Queue closed or ErrQueueClosed
			return
		}

		// Get current sequence number and increment
		seq := s.SeqNum
		s.SeqNum++ // Go automatically wraps uint32 on overflow

		// Determine FECInfo
		var fecInfo uint16 = 0
		if sm.fecEnabled && sm.fecK > 0 && sm.fecN > sm.fecK {
			index := len(groupPackets)
			// Bit 15: FEC flag (0 for data)
			// Bit 14-4: groupNum (11 bits, 0-2047)
			// Bit 3-0: index (4 bits, 0-15)
			fecInfo = (uint16(groupNum) << 4) | uint16(index)
		}

		// Encrypt payload only, if encryption is enabled
		var payloadToSend []byte
		if sm.encryptionEnabled && s.AEAD != nil {
			ciphertext, err := crypto.EncryptGCMWithAEAD(pkt.Data, s.AEAD)
			if err != nil {
				logger.Errorf(0, "Failed to encrypt data packet: %v", err)
				continue // Skip sending corrupted packet
			}
			payloadToSend = ciphertext
		} else {
			payloadToSend = pkt.Data
		}

		// Build plain encapsulation header (17 bytes)
		h := &EncapsulatedHeader{
			Version:    CurrentHeaderVersion,
			SeqNum:     seq,
			Timestamp:  time.Now().UnixNano(),
			PayloadLen: uint16(len(payloadToSend)),
			FECInfo:    fecInfo,
		}
		headerBytes := h.Serialize()

		// Merge header and encrypted (or plain) payload
		packetToSend := make([]byte, len(headerBytes)+len(payloadToSend))
		copy(packetToSend, headerBytes)
		copy(packetToSend[len(headerBytes):], payloadToSend)

		// Send packet via the shared unicast UDP socket
		if sm.conn != nil {
			_, err = sm.conn.WriteToUDP(packetToSend, s.Addr)
			if err == nil {
				logger.GlobalSenderStats.AddTraffic(int64(len(packetToSend)))
			}
			if err != nil {
				select {
				case <-s.Ctx.Done():
					return
				default:
					logger.Warnf(0, "Failed to send packet to %s: %v", s.ID, err)
				}
			}
		}

		// Buffer packet for FEC redundant generation
		if sm.fecEnabled && sm.fecK > 0 && sm.fecN > sm.fecK {
			groupPackets = append(groupPackets, packetToSend)

			if len(groupPackets) == sm.fecK {
				// Use cached reedsolomon.Encoder to avoid high allocation overhead
				redundantPackets, err := fec.EncodeFECWithEncoder(sm.fecEncoder, sm.fecK, sm.fecN, groupPackets)
				if err != nil {
					logger.Errorf(0, "Failed to encode FEC for group %d: %v", groupNum, err)
				} else {
					for i, rData := range redundantPackets {
						rSeq := s.SeqNum
						s.SeqNum++

						// Bit 15: FEC flag (1 for redundant packet)
						// Bit 14-4: groupNum (11 bits, 0-2047)
						// Bit 3-0: index (4 bits, 0-15) (k + i)
						rFecInfo := (1 << 15) | (uint16(groupNum) << 4) | uint16(sm.fecK+i)

						// Build plain FEC Header (17 bytes)
						rh := &EncapsulatedHeader{
							Version:    CurrentHeaderVersion,
							SeqNum:     rSeq,
							Timestamp:  time.Now().UnixNano(),
							PayloadLen: uint16(len(rData)),
							FECInfo:    uint16(rFecInfo),
						}
						rhBytes := rh.Serialize()

						// Prepend the FEC header to the redundant data block (Do NOT overwrite rData)
						fecPacket := make([]byte, len(rhBytes)+len(rData))
						copy(fecPacket, rhBytes)
						copy(fecPacket[len(rhBytes):], rData)

						if sm.conn != nil {
							_, err = sm.conn.WriteToUDP(fecPacket, s.Addr)
							if err == nil {
								logger.GlobalSenderStats.AddTraffic(int64(len(fecPacket)))
							}
							if err != nil {
								select {
								case <-s.Ctx.Done():
									return
								default:
									logger.Warnf(0, "Failed to send FEC redundant packet to %s: %v", s.ID, err)
								}
							}
						}
					}
				}
				// Clear group state for next group
				groupPackets = groupPackets[:0]
				groupNum = (groupNum + 1) & 0x7FF // 11 bits wrap-around (0-2047)
			}
		}
	}

	// Clean up and send remaining redundant packets if there are any packets in groupPackets (M10)
	if sm.fecEnabled && sm.fecK > 0 && sm.fecN > sm.fecK && len(groupPackets) > 0 {
		baseSize := len(groupPackets[0])
		for len(groupPackets) < sm.fecK {
			dummy := make([]byte, baseSize)
			dh := &EncapsulatedHeader{
				Version:    CurrentHeaderVersion,
				SeqNum:     0,
				Timestamp:  time.Now().UnixNano(),
				PayloadLen: 0,
				FECInfo:    (uint16(groupNum) << 4) | uint16(len(groupPackets)),
			}
			copy(dummy, dh.Serialize())
			groupPackets = append(groupPackets, dummy)
		}

		redundantPackets, err := fec.EncodeFECWithEncoder(sm.fecEncoder, sm.fecK, sm.fecN, groupPackets)
		if err == nil {
			for i, rData := range redundantPackets {
				rSeq := s.SeqNum
				s.SeqNum++

				rFecInfo := (1 << 15) | (uint16(groupNum) << 4) | uint16(sm.fecK+i)
				rh := &EncapsulatedHeader{
					Version:    CurrentHeaderVersion,
					SeqNum:     rSeq,
					Timestamp:  time.Now().UnixNano(),
					PayloadLen: uint16(len(rData)),
					FECInfo:    uint16(rFecInfo),
				}
				rhBytes := rh.Serialize()

				fecPacket := make([]byte, len(rhBytes)+len(rData))
				copy(fecPacket, rhBytes)
				copy(fecPacket[len(rhBytes):], rData)

				if sm.conn != nil {
					_, _ = sm.conn.WriteToUDP(fecPacket, s.Addr)
				}
			}
		}
	}
}
