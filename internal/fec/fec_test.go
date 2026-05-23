package fec_test

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"multicast-bridge/internal/data"
	"multicast-bridge/internal/fec"
)

func TestEncodeAndReconstruct(t *testing.T) {
	k := 8
	n := 10

	// 1. Create dummy data payloads
	originalPayloads := make([][]byte, k)
	originalPackets := make([][]byte, k)
	for i := 0; i < k; i++ {
		originalPayloads[i] = []byte(fmt.Sprintf("fec-test-payload-packet-%d", i))

		// Create encapsulation header for data packet
		h := &data.EncapsulatedHeader{
			Version:    data.CurrentHeaderVersion,
			SeqNum:     uint32(i),
			Timestamp:  time.Now().UnixNano(),
			PayloadLen: uint16(len(originalPayloads[i])),
			FECInfo:    uint16(i), // groupNum=0, index=i, FEC flag=0
		}
		headerBytes := h.Serialize()
		pkt := make([]byte, len(headerBytes)+len(originalPayloads[i]))
		copy(pkt, headerBytes)
		copy(pkt[len(headerBytes):], originalPayloads[i])
		originalPackets[i] = pkt
	}

	// 2. Generate redundant packets
	redundantPackets, err := fec.EncodeFEC(k, n, originalPackets)
	if err != nil {
		t.Fatalf("Failed to EncodeFEC: %v", err)
	}
	if len(redundantPackets) != n-k {
		t.Fatalf("Expected %d redundant packets, got %d", n-k, len(redundantPackets))
	}

	// Overwrite redundant packets' headers with redundant FECInfo
	for i, rPkt := range redundantPackets {
		rh := &data.EncapsulatedHeader{
			Version:    data.CurrentHeaderVersion,
			SeqNum:     uint32(k + i),
			Timestamp:  time.Now().UnixNano(),
			PayloadLen: uint16(len(rPkt) - data.HeaderSize),
			FECInfo:    (1 << 15) | uint16(k+i), // groupNum=0, index=k+i, FEC flag=1
		}
		copy(rPkt[0:data.HeaderSize], rh.Serialize())
	}

	// 3. Initialize FECManager
	mgr := fec.NewFECManager(true, k, n)

	// We simulate a loss of 2 packets: index 2 and index 5
	receivedPayloads := make(map[int][]byte)

	var released [][]byte
	for i := 0; i < k; i++ {
		if i == 2 || i == 5 {
			continue
		}
		rel, err := mgr.AddPacket(originalPackets[i], false, 0, i)
		if err != nil {
			t.Errorf("AddPacket error: %v", err)
		}
		released = append(released, rel...)
	}

	// At this point, we shouldn't have reconstructed anything since we lack packets.
	// But we should have received the pass-through packets (0, 1, 3, 4, 6, 7).
	if len(released) != 6 {
		t.Errorf("Expected 6 pass-through packets, got %d", len(released))
	}
	for _, pkt := range released {
		h, _ := data.DeserializeHeader(pkt)
		idx := int(h.FECInfo & 0xFF)
		receivedPayloads[idx] = pkt[data.HeaderSize : data.HeaderSize+int(h.PayloadLen)]
	}

	// Now we feed the first redundant packet (index 8).
	// Total received will be 6 + 1 = 7. K is 8. Reconstruction shouldn't trigger.
	rel, err := mgr.AddPacket(redundantPackets[0], true, 0, 8)
	if err != nil {
		t.Errorf("AddPacket error: %v", err)
	}
	if len(rel) > 0 {
		t.Errorf("Reconstruction triggered early with 7 packets")
	}

	// Now we feed the second redundant packet (index 9).
	// Total received will be 7 + 1 = 8. Reconstruction should trigger and return the 2 missing packets (index 2 and 5).
	rel, err = mgr.AddPacket(redundantPackets[1], true, 0, 9)
	if err != nil {
		t.Errorf("AddPacket error: %v", err)
	}
	if len(rel) != 2 {
		t.Fatalf("Expected 2 reconstructed packets, got %d", len(rel))
	}

	for _, pkt := range rel {
		h, _ := data.DeserializeHeader(pkt)
		idx := int(h.FECInfo & 0xFF)
		receivedPayloads[idx] = pkt[data.HeaderSize : data.HeaderSize+int(h.PayloadLen)]
		if idx != 2 && idx != 5 {
			t.Errorf("Unexpected reconstructed packet index %d", idx)
		}
	}

	// Verify all 8 original packets are recovered correctly
	for i := 0; i < k; i++ {
		payload, exists := receivedPayloads[i]
		if !exists {
			t.Errorf("Packet %d was not recovered", i)
			continue
		}
		if !bytes.Equal(payload, originalPayloads[i]) {
			t.Errorf("Recovered payload for packet %d mismatch.\nExpected: %q\nGot: %q", i, originalPayloads[i], payload)
		}
	}
}

func TestUnrecoverableLoss(t *testing.T) {
	k := 8
	n := 10

	// Create dummy packets
	originalPackets := make([][]byte, k)
	for i := 0; i < k; i++ {
		h := &data.EncapsulatedHeader{
			Version:    data.CurrentHeaderVersion,
			SeqNum:     uint32(i),
			Timestamp:  time.Now().UnixNano(),
			PayloadLen: 10,
			FECInfo:    uint16(i),
		}
		pkt := make([]byte, data.HeaderSize+10)
		copy(pkt, h.Serialize())
		originalPackets[i] = pkt
	}

	redundantPackets, _ := fec.EncodeFEC(k, n, originalPackets)
	for i, rPkt := range redundantPackets {
		rh := &data.EncapsulatedHeader{
			Version:    data.CurrentHeaderVersion,
			SeqNum:     uint32(k + i),
			Timestamp:  time.Now().UnixNano(),
			PayloadLen: uint16(len(rPkt) - data.HeaderSize),
			FECInfo:    (1 << 15) | uint16(k+i),
		}
		copy(rPkt[0:data.HeaderSize], rh.Serialize())
	}

	mgr := fec.NewFECManager(true, k, n)

	// Simulate 3 packet losses: index 1, 3, 5
	// We only receive 5 data packets. Even with both redundant packets, total is 5 + 2 = 7 packets, which is less than K=8.
	// So reconstruction should fail/not trigger.
	for i := 0; i < k; i++ {
		if i == 1 || i == 3 || i == 5 {
			continue
		}
		_, _ = mgr.AddPacket(originalPackets[i], false, 0, i)
	}

	// Add the 2 redundant packets
	_, _ = mgr.AddPacket(redundantPackets[0], true, 0, 8)
	rel, err := mgr.AddPacket(redundantPackets[1], true, 0, 9)
	if err != nil {
		t.Errorf("Unexpected error during AddPacket: %v", err)
	}
	if len(rel) > 0 {
		t.Errorf("Reconstruction should not have succeeded with only 7 packets, but got %d", len(rel))
	}
}

func TestFECTimeout(t *testing.T) {
	k := 8
	n := 10

	mgr := fec.NewFECManager(true, k, n)

	// Add one packet to create a group
	h := &data.EncapsulatedHeader{
		Version:    data.CurrentHeaderVersion,
		SeqNum:     1,
		Timestamp:  time.Now().UnixNano(),
		PayloadLen: 10,
		FECInfo:    0,
	}
	pkt := make([]byte, data.HeaderSize+10)
	copy(pkt, h.Serialize())

	rel, err := mgr.AddPacket(pkt, false, 0, 0)
	if err != nil {
		t.Fatalf("AddPacket failed: %v", err)
	}
	if len(rel) != 1 {
		t.Fatalf("Expected 1 released packet, got %d", len(rel))
	}

	// Sending duplicate packet should be ignored (return nil, nil)
	rel, err = mgr.AddPacket(pkt, false, 0, 0)
	if err != nil {
		t.Fatalf("AddPacket failed: %v", err)
	}
	if len(rel) != 0 {
		t.Fatalf("Duplicate packet should be ignored, but got %d", len(rel))
	}

	// Wait 10ms and clean up with 5ms timeout to force timeout
	time.Sleep(10 * time.Millisecond)
	mgr.CleanUpTimeouts(5 * time.Millisecond)

	// Sending same packet again: if group was purged, it will be treated as new,
	// so it should be released again (pass-through).
	rel, err = mgr.AddPacket(pkt, false, 0, 0)
	if err != nil {
		t.Fatalf("AddPacket failed: %v", err)
	}
	if len(rel) != 1 {
		t.Errorf("Expected 1 released packet after timeout purge, got %d", len(rel))
	}
}
