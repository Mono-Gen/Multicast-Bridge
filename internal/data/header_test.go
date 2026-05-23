package data

import (
	"testing"
	"time"
)

func TestHeaderSerialization(t *testing.T) {
	h := &EncapsulatedHeader{
		Version:    CurrentHeaderVersion,
		SeqNum:     123456,
		Timestamp:  time.Now().UnixNano(),
		PayloadLen: 512,
		FECInfo:    0x8102, // FEC flag set, group 1, index 2
	}

	serialized := h.Serialize()
	if len(serialized) != HeaderSize {
		t.Fatalf("Expected serialized length %d, got %d", HeaderSize, len(serialized))
	}

	parsed, err := DeserializeHeader(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize header: %v", err)
	}

	if parsed.Version != h.Version {
		t.Errorf("Version mismatch: expected %d, got %d", h.Version, parsed.Version)
	}
	if parsed.SeqNum != h.SeqNum {
		t.Errorf("SeqNum mismatch: expected %d, got %d", h.SeqNum, parsed.SeqNum)
	}
	if parsed.Timestamp != h.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", h.Timestamp, parsed.Timestamp)
	}
	if parsed.PayloadLen != h.PayloadLen {
		t.Errorf("PayloadLen mismatch: expected %d, got %d", h.PayloadLen, parsed.PayloadLen)
	}
	if parsed.FECInfo != h.FECInfo {
		t.Errorf("FECInfo mismatch: expected 0x%x, got 0x%x", h.FECInfo, parsed.FECInfo)
	}
}

func TestHeaderDeserializeTooShort(t *testing.T) {
	tooShort := []byte{0x01, 0x00, 0x00}
	_, err := DeserializeHeader(tooShort)
	if err == nil {
		t.Error("Expected error when deserializing too short buffer, got nil")
	}
}
