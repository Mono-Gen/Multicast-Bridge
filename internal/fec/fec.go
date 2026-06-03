package fec

import (
	"fmt"
	"sync"
	"time"

	"github.com/klauspost/reedsolomon"
	"multicast-bridge/internal/logger"
)

// GroupState represents the reception state of a single FEC group.
type GroupState struct {
	GroupNum  uint16
	K, N      int
	Packets   [][]byte // Length N array of packet bytes (includes encapsulation header + payload)
	Received  []bool   // Reception flags for each index
	Sent      []bool   // Release flags (to prevent double forwarding of data packets)
	RecvCount int
	MaxLen    int      // Maximum length seen to align padding
	CreatedAt time.Time
}

// FECManager handles buffering, timeout, and Reed-Solomon recovery for Receiver.
type FECManager struct {
	mu           sync.Mutex
	groups       map[uint16]*GroupState
	k, n         int
	enabled      bool
	decoder      reedsolomon.Encoder
	groupTimeout time.Duration // Max age of a GroupState before it's treated as stale
}

// NewFECManager creates a new FECManager.
// groupTimeout controls the stale-group detection window: any GroupState older than
// this duration is treated as a leftover from a previous wraparound generation.
// A value of 0 uses the default of 2 seconds.
func NewFECManager(enabled bool, k, n int, groupTimeout time.Duration) *FECManager {
	if groupTimeout <= 0 {
		groupTimeout = 2 * time.Second
	}
	var dec reedsolomon.Encoder
	var err error
	if enabled && k > 0 && n > k {
		dec, err = reedsolomon.New(k, n-k)
		if err != nil {
			logger.Errorf(403, "Failed to initialize reedsolomon engine: %v. Bypassing FEC...", err)
			enabled = false
		}
	} else {
		enabled = false
	}

	return &FECManager{
		groups:       make(map[uint16]*GroupState),
		k:            k,
		n:            n,
		enabled:      enabled,
		decoder:      dec,
		groupTimeout: groupTimeout,
	}
}

// AddPacket processes an incoming encapsulated packet bytes.
// It returns a slice of data packets that are ready to be released (deserialized and forwarded to multicast).
func (m *FECManager) AddPacket(pkt []byte, isFEC bool, groupNum uint16, index int) ([][]byte, error) {
	if !m.enabled {
		return [][]byte{pkt}, nil
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	g, exists := m.groups[groupNum]
	if exists {
		// Generation check: If a GroupState is older than groupTimeout, it is treated as a
		// leftover from a previous wraparound generation. Delete and recreate to prevent
		// new packets from mixing with stale legacy state.
		if time.Since(g.CreatedAt) > m.groupTimeout {
			lostCount := 0
			for i := 0; i < g.K; i++ {
				if !g.Sent[i] {
					lostCount++
				}
			}
			if lostCount > 0 {
				logger.Warnf(303, "FEC group %d timed out during AddPacket. Permanent loss of %d packets.", groupNum, lostCount)
			}
			delete(m.groups, groupNum)
			exists = false
		}
	}

	if !exists {
		g = &GroupState{
			GroupNum:  groupNum,
			K:         m.k,
			N:         m.n,
			Packets:   make([][]byte, m.n),
			Received:  make([]bool, m.n),
			Sent:      make([]bool, m.k),
			CreatedAt: time.Now(),
		}
		m.groups[groupNum] = g
	}

	if index < 0 || index >= m.n {
		return nil, fmt.Errorf("invalid packet index %d for group %d", index, groupNum)
	}

	// Sanitize index relationship with isFEC (M12)
	if isFEC && index < m.k {
		return nil, fmt.Errorf("invalid packet: redundant packet (isFEC=true) must have index >= K (%d), got index %d", m.k, index)
	}
	if !isFEC && index >= m.k {
		return nil, fmt.Errorf("invalid packet: data packet (isFEC=false) must have index < K (%d), got index %d", m.k, index)
	}

	// Ignore duplicate packet receptions
	if g.Received[index] {
		return nil, nil
	}

	// Buffer the packet
	g.Packets[index] = make([]byte, len(pkt))
	copy(g.Packets[index], pkt)
	g.Received[index] = true
	g.RecvCount++
	if len(pkt) > g.MaxLen {
		g.MaxLen = len(pkt)
	}

	var toRelease [][]byte

	// Pass-through: If it is a normal data packet, forward it immediately to keep low latency
	if !isFEC && index < g.K {
		toRelease = append(toRelease, pkt)
		g.Sent[index] = true
	}

	// Check if reconstruction is possible and needed:
	// We need at least K packets received, and there must be some missing data packets (index < K)
	hasMissingData := false
	for i := 0; i < g.K; i++ {
		if !g.Sent[i] {
			hasMissingData = true
			break
		}
	}

	if g.RecvCount >= g.K && hasMissingData {
		// Align padding length for Reed-Solomon engine
		for i := 0; i < g.N; i++ {
			if g.Received[i] && len(g.Packets[i]) < g.MaxLen {
				padded := make([]byte, g.MaxLen)
				copy(padded, g.Packets[i])
				g.Packets[i] = padded
			}
		}

		// Reconstruct missing blocks
		err := m.decoder.Reconstruct(g.Packets)
		if err != nil {
			logger.Warnf(303, "FEC Reconstruct failed for group %d: %v", groupNum, err)
			return toRelease, err
		}

		// Reconstruct success! Collect all recovered packets that were not sent yet
		for i := 0; i < g.K; i++ {
			if !g.Sent[i] {
				// The reconstructed data packet is now in g.Packets[i]
				toRelease = append(toRelease, g.Packets[i])
				g.Sent[i] = true
			}
		}
	}

	// Immediate cleanup: If all data packets in this group have been released/sent,
	// we no longer need to retain this GroupState. Purge it from the map immediately.
	allSent := true
	for i := 0; i < g.K; i++ {
		if !g.Sent[i] {
			allSent = false
			break
		}
	}
	if allSent {
		delete(m.groups, groupNum)
	}

	return toRelease, nil
}

// CleanUpTimeouts purges expired groups and logs warning [303] if packet recovery permanently failed.
func (m *FECManager) CleanUpTimeouts() {
	if !m.enabled {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for num, g := range m.groups {
		if now.Sub(g.CreatedAt) > m.groupTimeout {
			// Find if there are any data packets that were never released
			lostCount := 0
			for i := 0; i < g.K; i++ {
				if !g.Sent[i] {
					lostCount++
				}
			}

			if lostCount > 0 {
				logger.Warnf(303, "FEC group %d timed out. Permanent loss of %d packets.", num, lostCount)
			}

			delete(m.groups, num)
		}
	}
}

// EncodeFEC encapsulates the Reed-Solomon redundant packet generation for Sender.
//
// Deprecated: Use EncodeFECWithEncoder with a cached reedsolomon.Encoder for better
// performance. This function creates a new Encoder on every call (expensive GF arithmetic).
// It is retained for test code only.
func EncodeFEC(k, n int, packets [][]byte) ([][]byte, error) {
	enc, err := reedsolomon.New(k, n-k)
	if err != nil {
		return nil, err
	}

	// 1. Find max length
	maxLen := 0
	for _, p := range packets {
		if len(p) > maxLen {
			maxLen = len(p)
		}
	}

	// 2. Pad packets to maxLen
	padded := make([][]byte, n)
	for i := 0; i < k; i++ {
		padded[i] = make([]byte, maxLen)
		copy(padded[i], packets[i])
	}
	for i := k; i < n; i++ {
		padded[i] = make([]byte, maxLen)
	}

	// 3. Generate redundant packets
	err = enc.Encode(padded)
	if err != nil {
		return nil, err
	}

	// 4. Return redundant packets slice
	redundant := make([][]byte, n-k)
	for i := 0; i < n-k; i++ {
		redundant[i] = padded[k+i]
	}
	return redundant, nil
}

// EncodeFECWithEncoder encapsulates the Reed-Solomon redundant packet generation using a cached Encoder.
func EncodeFECWithEncoder(enc reedsolomon.Encoder, k, n int, packets [][]byte) ([][]byte, error) {
	if enc == nil {
		return nil, fmt.Errorf("reedsolomon encoder is nil")
	}

	// 1. Find max length
	maxLen := 0
	for _, p := range packets {
		if len(p) > maxLen {
			maxLen = len(p)
		}
	}

	// 2. Pad packets to maxLen
	padded := make([][]byte, n)
	for i := 0; i < k; i++ {
		padded[i] = make([]byte, maxLen)
		copy(padded[i], packets[i])
	}
	for i := k; i < n; i++ {
		padded[i] = make([]byte, maxLen)
	}

	// 3. Generate redundant packets
	err := enc.Encode(padded)
	if err != nil {
		return nil, err
	}

	// 4. Return redundant packets slice
	redundant := make([][]byte, n-k)
	for i := 0; i < n-k; i++ {
		redundant[i] = padded[k+i]
	}
	return redundant, nil
}
