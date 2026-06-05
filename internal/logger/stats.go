package logger

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// TrafficStats calculates sliding-window throughput (bps)
type TrafficStats struct {
	mu      sync.Mutex
	history []struct {
		timestamp time.Time
		bytes     int64
	}
}

func NewTrafficStats() *TrafficStats {
	return &TrafficStats{}
}

func (t *TrafficStats) Add(bytes int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	t.history = append(t.history, struct {
		timestamp time.Time
		bytes     int64
	}{now, bytes})
	t.cleanOld(now)
}

func (t *TrafficStats) cleanOld(now time.Time) {
	cutoff := now.Add(-1 * time.Second)
	idx := 0
	for idx < len(t.history) && t.history[idx].timestamp.Before(cutoff) {
		idx++
	}
	if idx > 0 {
		t.history = t.history[idx:]
	}
}

func (t *TrafficStats) Bps() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.cleanOld(time.Now())
	var total int64
	for _, entry := range t.history {
		total += entry.bytes
	}
	return total * 8
}

// LatencyStats calculates latency metrics
type LatencyStats struct {
	mu         sync.Mutex
	min        time.Duration
	max        time.Duration
	total      time.Duration
	count      int64
	recentRing [100]time.Duration
	recentIdx  int
	recentSize int
}

func NewLatencyStats() *LatencyStats {
	return &LatencyStats{}
}

func (l *LatencyStats) Add(d time.Duration) {
	// Skip invalid latency caused by clock drift or anomalies (e.g. negative or > 1 minute)
	if d < 0 || d > 1*time.Minute {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.count == 0 {
		l.min = d
		l.max = d
	} else {
		if d < l.min {
			l.min = d
		}
		if d > l.max {
			l.max = d
		}
	}
	l.total += d
	l.count++

	l.recentRing[l.recentIdx] = d
	l.recentIdx = (l.recentIdx + 1) % 100
	if l.recentSize < 100 {
		l.recentSize++
	}
}

func (l *LatencyStats) GetMetrics() (min, max, avg, recentMin, recentMax, recentAvg time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.count == 0 {
		return 0, 0, 0, 0, 0, 0
	}

	avg = l.total / time.Duration(l.count)
	min = l.min
	max = l.max

	if l.recentSize == 0 {
		return min, max, avg, 0, 0, 0
	}

	recentMin = l.recentRing[0]
	recentMax = l.recentRing[0]
	var recentTotal time.Duration
	for i := 0; i < l.recentSize; i++ {
		val := l.recentRing[i]
		if val < recentMin {
			recentMin = val
		}
		if val > recentMax {
			recentMax = val
		}
		recentTotal += val
	}
	recentAvg = recentTotal / time.Duration(l.recentSize)

	return min, max, avg, recentMin, recentMax, recentAvg
}

// SessionInfo keeps track of an active session
type SessionInfo struct {
	Addr       string
	JoinedAt   time.Time
	LastActive time.Time
}

// TimeoutHistory keeps track of session timeout events
type TimeoutHistory struct {
	Addr      string
	TimeoutAt time.Time
}

// SenderStats keeps track of sessions and timeout history for the Sender
type SenderStats struct {
	mu             sync.Mutex
	sessions       map[string]*SessionInfo
	timeoutHistory []TimeoutHistory
	traffic        *TrafficStats
}

func NewSenderStats() *SenderStats {
	return &SenderStats{
		sessions: make(map[string]*SessionInfo),
		traffic:  NewTrafficStats(),
	}
}

func (s *SenderStats) SessionJoined(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[addr] = &SessionInfo{
		Addr:       addr,
		JoinedAt:   time.Now(),
		LastActive: time.Now(),
	}
}

func (s *SenderStats) SessionKeepAlive(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if info, exists := s.sessions[addr]; exists {
		info.LastActive = time.Now()
	}
}

func (s *SenderStats) SessionTimeout(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, addr)
	s.timeoutHistory = append(s.timeoutHistory, TimeoutHistory{
		Addr:      addr,
		TimeoutAt: time.Now(),
	})
	// Keep timeout history to last 20
	if len(s.timeoutHistory) > 20 {
		s.timeoutHistory = s.timeoutHistory[1:]
	}
}

func (s *SenderStats) SessionRemoved(addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, addr)
}

func (s *SenderStats) AddTraffic(bytes int64) {
	s.traffic.Add(bytes)
}

func (s *SenderStats) GetSessions() []SessionInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]SessionInfo, 0, len(s.sessions))
	for _, info := range s.sessions {
		res = append(res, *info)
	}
	return res
}

func (s *SenderStats) GetTimeoutHistory() []TimeoutHistory {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := make([]TimeoutHistory, len(s.timeoutHistory))
	copy(res, s.timeoutHistory)
	return res
}

func (s *SenderStats) Bps() int64 {
	return s.traffic.Bps()
}

func (s *SenderStats) DumpString() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	var sb strings.Builder
	sb.WriteString("\n=== Sender Statistics ===\n")
	sb.WriteString(fmt.Sprintf("Throughput: %.2f Kbps\n", float64(s.traffic.Bps())/1000.0))
	sb.WriteString(fmt.Sprintf("Active Sessions (%d):\n", len(s.sessions)))
	for _, info := range s.sessions {
		sb.WriteString(fmt.Sprintf("  - %s (Joined: %s, LastActive: %s)\n", 
			info.Addr, 
			info.JoinedAt.Format("15:04:05"), 
			info.LastActive.Format("15:04:05")))
	}
	sb.WriteString(fmt.Sprintf("Timeout History (%d):\n", len(s.timeoutHistory)))
	for _, hist := range s.timeoutHistory {
		sb.WriteString(fmt.Sprintf("  - %s (Timed out at: %s)\n", 
			hist.Addr, 
			hist.TimeoutAt.Format("15:04:05")))
	}
	sb.WriteString("=========================")
	return sb.String()
}

// ReceiverStats keeps track of receiver metrics (packet loss, traffic, latency)
type ReceiverStats struct {
	mu             sync.Mutex
	traffic        *TrafficStats
	latency        *LatencyStats
	expectedSeqNum uint32
	receivedPackets int64
	lostPackets     int64
	isFirstPacket   bool
}

func NewReceiverStats() *ReceiverStats {
	return &ReceiverStats{
		traffic:       NewTrafficStats(),
		latency:       NewLatencyStats(),
		isFirstPacket: true,
	}
}

func (r *ReceiverStats) AddPacket(seqNum uint32, timestamp int64, payloadSize int) {
	// 1. Record throughput
	r.traffic.Add(int64(payloadSize))

	// 2. Calculate latency
	if timestamp > 0 {
		now := time.Now().UnixNano()
		diff := now - timestamp
		r.latency.Add(time.Duration(diff))
	}

	// 3. Calculate packet loss
	r.mu.Lock()
	defer r.mu.Unlock()
	r.receivedPackets++

	if r.isFirstPacket {
		r.expectedSeqNum = seqNum + 1
		r.isFirstPacket = false
		return
	}

	if seqNum > r.expectedSeqNum {
		// Packet loss occurred
		lost := int64(seqNum - r.expectedSeqNum)
		r.lostPackets += lost
		r.expectedSeqNum = seqNum + 1
	} else if seqNum == r.expectedSeqNum {
		r.expectedSeqNum++
	} else {
		// Handle out-of-order/delayed packet arrival or sequence reset upon reconnection.
		// Track the expected sequence number forward for simplicity.
		r.expectedSeqNum = seqNum + 1
	}
}

func (r *ReceiverStats) GetLossRate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := r.receivedPackets + r.lostPackets
	if total == 0 {
		return 0.0
	}
	return float64(r.lostPackets) / float64(total) * 100.0
}

func (r *ReceiverStats) Bps() int64 {
	return r.traffic.Bps()
}

func (r *ReceiverStats) GetLatency() *LatencyStats {
	return r.latency
}

func (r *ReceiverStats) DumpString() string {
	min, max, avg, recentMin, recentMax, recentAvg := r.latency.GetMetrics()

	var sb strings.Builder
	sb.WriteString("\n=== Receiver Statistics ===\n")
	sb.WriteString(fmt.Sprintf("Throughput: %.2f Kbps\n", float64(r.Bps())/1000.0))
	sb.WriteString(fmt.Sprintf("Packet Loss Rate: %.2f%%\n", r.GetLossRate()))
	sb.WriteString("Latency:\n")
	sb.WriteString(fmt.Sprintf("  Overall - Min: %v, Max: %v, Avg: %v\n", min, max, avg))
	sb.WriteString(fmt.Sprintf("  Recent 100 - Min: %v, Max: %v, Avg: %v\n", recentMin, recentMax, recentAvg))
	sb.WriteString("=========================")
	return sb.String()
}

var (
	GlobalSenderStats   = NewSenderStats()
	GlobalReceiverStats = NewReceiverStats()
)
