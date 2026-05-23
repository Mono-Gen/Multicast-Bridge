package data

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

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
	encryptionEnabled bool
	passphrase        string
}

// NewSessionManager creates a new SessionManager.
func NewSessionManager(parentCtx context.Context, maxSessions int, conn *net.UDPConn, fecEnabled bool, fecK, fecN int, encryptionEnabled bool, passphrase string) *SessionManager {
	ctx, cancel := context.WithCancel(parentCtx)
	return &SessionManager{
		sessions:          make(map[string]*Session),
		maxSessions:       maxSessions,
		ctx:               ctx,
		cancel:            cancel,
		conn:              conn,
		fecEnabled:        fecEnabled,
		fecK:              fecK,
		fecN:              fecN,
		encryptionEnabled: encryptionEnabled,
		passphrase:        passphrase,
	}
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
	if _, exists := sm.sessions[keyStr]; exists {
		return fmt.Errorf("session already exists for %s", keyStr)
	}

	if len(sm.sessions) >= sm.maxSessions {
		logger.Errorf(104, "Max sessions reached (%d). Connection rejected for %s", sm.maxSessions, keyStr)
		return fmt.Errorf("[104] max sessions reached")
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
		pkt := &Packet{
			Data: data,
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

	var groupNum uint8 = 0
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
			// Bit 14-8: groupNum
			// Bit 7-0: index
			fecInfo = (uint16(groupNum) << 8) | uint16(index)
		}

		// Build encapsulation header (17 bytes)
		h := &EncapsulatedHeader{
			Version:    CurrentHeaderVersion,
			SeqNum:     seq,
			Timestamp:  time.Now().UnixNano(),
			PayloadLen: uint16(len(pkt.Data)),
			FECInfo:    fecInfo,
		}
		headerBytes := h.Serialize()

		// Merge header and payload
		encapsulated := make([]byte, len(headerBytes)+len(pkt.Data))
		copy(encapsulated, headerBytes)
		copy(encapsulated[len(headerBytes):], pkt.Data)

		// Encrypt if enabled
		var packetToSend []byte
		if sm.encryptionEnabled && len(s.Key) > 0 {
			ciphertext, err := crypto.EncryptGCM(encapsulated, s.Key)
			if err != nil {
				logger.Errorf(0, "Failed to encrypt data packet: %v", err)
				continue // Skip sending corrupted packet
			}
			packetToSend = ciphertext
		} else {
			packetToSend = encapsulated
		}

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
				redundantPackets, err := fec.EncodeFEC(sm.fecK, sm.fecN, groupPackets)
				if err != nil {
					logger.Errorf(0, "Failed to encode FEC for group %d: %v", groupNum, err)
				} else {
					for i, rPkt := range redundantPackets {
						rSeq := s.SeqNum
						s.SeqNum++

						// Bit 15: FEC flag (1 for redundant packet)
						// Bit 14-8: groupNum
						// Bit 7-0: index (k + i)
						rFecInfo := (1 << 15) | (uint16(groupNum) << 8) | uint16(sm.fecK+i)

						// Overwrite the header of the redundant packet
						rh := &EncapsulatedHeader{
							Version:    CurrentHeaderVersion,
							SeqNum:     rSeq,
							Timestamp:  time.Now().UnixNano(),
							PayloadLen: uint16(len(rPkt) - HeaderSize),
							FECInfo:    uint16(rFecInfo),
						}
						rhBytes := rh.Serialize()
						copy(rPkt[0:HeaderSize], rhBytes)

						if sm.conn != nil {
							_, err = sm.conn.WriteToUDP(rPkt, s.Addr)
							if err == nil {
								logger.GlobalSenderStats.AddTraffic(int64(len(rPkt)))
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
				groupNum = (groupNum + 1) & 0x7F
			}
		}
	}
}
