package data

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestPacketQueue_EnqueueDequeue(t *testing.T) {
	pq := NewPacketQueue(3)

	// Test Enqueue
	pkt1 := &Packet{Data: []byte("packet1")}
	pkt2 := &Packet{Data: []byte("packet2")}

	if err := pq.Enqueue(pkt1); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}
	if err := pq.Enqueue(pkt2); err != nil {
		t.Fatalf("Enqueue failed: %v", err)
	}

	if pq.Size() != 2 {
		t.Errorf("Expected size 2, got %d", pq.Size())
	}

	// Test Dequeue
	out1, err := pq.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if string(out1.Data) != "packet1" {
		t.Errorf("Expected packet1, got %s", string(out1.Data))
	}

	out2, err := pq.Dequeue()
	if err != nil {
		t.Fatalf("Dequeue failed: %v", err)
	}
	if string(out2.Data) != "packet2" {
		t.Errorf("Expected packet2, got %s", string(out2.Data))
	}

	if pq.Size() != 0 {
		t.Errorf("Expected size 0, got %d", pq.Size())
	}
}

func TestPacketQueue_OverflowDiscardOldest(t *testing.T) {
	maxSize := 3
	pq := NewPacketQueue(maxSize)

	// Fill the queue
	for i := 1; i <= maxSize; i++ {
		pkt := &Packet{Data: []byte(fmt.Sprintf("packet%d", i))}
		if err := pq.Enqueue(pkt); err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
	}

	if pq.Size() != maxSize {
		t.Errorf("Expected size %d, got %d", maxSize, pq.Size())
	}

	// Enqueue one more to trigger overflow (packet1 should be discarded, packet4 added)
	pkt4 := &Packet{Data: []byte("packet4")}
	if err := pq.Enqueue(pkt4); err != nil {
		t.Fatalf("Enqueue overflow failed: %v", err)
	}

	if pq.Size() != maxSize {
		t.Errorf("Expected size to remain %d, got %d", maxSize, pq.Size())
	}

	if pq.DroppedCount() != 1 {
		t.Errorf("Expected 1 dropped packet, got %d", pq.DroppedCount())
	}

	// Verify that the queue contains: packet2, packet3, packet4 (packet1 was discarded)
	expected := []string{"packet2", "packet3", "packet4"}
	for _, exp := range expected {
		out, err := pq.Dequeue()
		if err != nil {
			t.Fatalf("Dequeue failed: %v", err)
		}
		if string(out.Data) != exp {
			t.Errorf("Expected %s, got %s", exp, string(out.Data))
		}
	}
}

func TestPacketQueue_CloseAndBlockRescue(t *testing.T) {
	pq := NewPacketQueue(5)

	// Goroutine waiting on Dequeue
	errChan := make(chan error, 1)
	go func() {
		_, err := pq.Dequeue()
		errChan <- err
	}()

	// Allow goroutine to start waiting
	time.Sleep(50 * time.Millisecond)

	// Close queue, which should wake up the waiting goroutine and return ErrQueueClosed
	pq.Close()

	select {
	case err := <-errChan:
		if !errors.Is(err, ErrQueueClosed) {
			t.Errorf("Expected ErrQueueClosed, got %v", err)
		}
	case <-time.After(1 * time.Second):
		t.Error("Blocked Dequeue was not rescued after closing queue")
	}

	// Enqueueing after close should return ErrQueueClosed
	pkt := &Packet{Data: []byte("blocked")}
	if err := pq.Enqueue(pkt); !errors.Is(err, ErrQueueClosed) {
		t.Errorf("Expected ErrQueueClosed for post-close Enqueue, got %v", err)
	}
}

func TestPacket_WithAddr(t *testing.T) {
	pq := NewPacketQueue(1)
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9999}
	pkt := &Packet{Data: []byte("data"), Addr: addr}

	pq.Enqueue(pkt)
	out, _ := pq.Dequeue()

	if out.Addr.String() != addr.String() {
		t.Errorf("Expected address %s, got %s", addr.String(), out.Addr.String())
	}
}
