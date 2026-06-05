package data

import (
	"errors"
	"net"
	"sync"
	"sync/atomic"

	"multicast-bridge/internal/logger"
)

var ErrQueueClosed = errors.New("queue is closed")

// Packet represents a raw UDP packet data and its destination or source address.
type Packet struct {
	Data []byte
	Addr *net.UDPAddr
}

// PacketQueue is a thread-safe, lock-based queue for UDP packets.
// It prioritizes real-time delivery by discarding the oldest packets when full.
type PacketQueue struct {
	mu           sync.Mutex
	cond         *sync.Cond
	packets      []*Packet
	maxSize      int
	closed       bool
	droppedCount int64
}

// NewPacketQueue creates a new PacketQueue with the specified maximum size.
func NewPacketQueue(maxSize int) *PacketQueue {
	pq := &PacketQueue{
		packets: make([]*Packet, 0, maxSize),
		maxSize: maxSize,
	}
	pq.cond = sync.NewCond(&pq.mu)
	return pq
}

// Enqueue adds a packet to the queue. If the queue is full, the oldest packet
// is discarded to make room for the new packet, prioritizing real-time delivery.
func (pq *PacketQueue) Enqueue(pkt *Packet) error {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return ErrQueueClosed
	}

	if len(pq.packets) >= pq.maxSize {
		// Queue is full, discard the oldest packet (index 0)
		pq.packets[0] = nil // Avoid GC retention
		pq.packets = pq.packets[1:]
		newDropped := atomic.AddInt64(&pq.droppedCount, 1)

		// Throttled logging to avoid spamming logs under high packet loss
		if newDropped%100 == 1 {
			logger.Warnf(301, "Queue overflow. Discarded oldest packet. (Total dropped: %d)", newDropped)
		}
	}

	pq.packets = append(pq.packets, pkt)
	pq.cond.Signal() // Wake up one waiting Dequeue goroutine
	return nil
}

// Dequeue retrieves and removes a packet from the queue.
// It blocks until a packet is available or the queue is closed.
func (pq *PacketQueue) Dequeue() (*Packet, error) {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	for len(pq.packets) == 0 && !pq.closed {
		pq.cond.Wait()
	}

	if pq.closed {
		return nil, ErrQueueClosed
	}

	pkt := pq.packets[0]
	pq.packets[0] = nil // Avoid GC retention
	pq.packets = pq.packets[1:]
	return pkt, nil
}

// Close closes the queue and wakes up all Dequeue goroutines waiting for packets.
func (pq *PacketQueue) Close() {
	pq.mu.Lock()
	defer pq.mu.Unlock()

	if pq.closed {
		return
	}

	pq.closed = true
	pq.packets = nil // Allow garbage collection
	pq.cond.Broadcast()
}

// DroppedCount returns the total number of packets dropped due to queue overflow.
func (pq *PacketQueue) DroppedCount() int64 {
	return atomic.LoadInt64(&pq.droppedCount)
}

// Size returns the current number of packets in the queue.
func (pq *PacketQueue) Size() int {
	pq.mu.Lock()
	defer pq.mu.Unlock()
	return len(pq.packets)
}
