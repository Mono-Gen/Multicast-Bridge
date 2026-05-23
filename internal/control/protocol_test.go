package control

import (
	"bytes"
	"testing"
)

func TestParseAndBuildPacket(t *testing.T) {
	payload := []byte("hello-world")
	data := BuildPacket(TypeRegister, payload)

	if len(data) != 3+len(payload) {
		t.Fatalf("BuildPacket length unexpected: got %d, want %d", len(data), 3+len(payload))
	}

	packet, err := ParsePacket(data)
	if err != nil {
		t.Fatalf("ParsePacket failed: %v", err)
	}

	if packet.Type != TypeRegister {
		t.Errorf("Unexpected type: got %d, want %d", packet.Type, TypeRegister)
	}

	if !bytes.Equal(packet.Payload, payload) {
		t.Errorf("Unexpected payload: got %v, want %v", packet.Payload, payload)
	}
}

func TestRegisterPayload(t *testing.T) {
	dataPort := uint16(5101)
	version := "1.2.3"
	ipAddr := "192.168.1.100"

	payload := EncodeRegisterPayload(dataPort, version, ipAddr)
	
	decPort, decVersion, decIP, err := DecodeRegisterPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRegisterPayload failed: %v", err)
	}

	if decPort != dataPort {
		t.Errorf("Decoded port mismatch: got %d, want %d", decPort, dataPort)
	}

	if decVersion != version {
		t.Errorf("Decoded version mismatch: got %q, want %q", decVersion, version)
	}

	if decIP != ipAddr {
		t.Errorf("Decoded IP mismatch: got %q, want %q", decIP, ipAddr)
	}
}

func TestRegisterAckPayload(t *testing.T) {
	status := StatusDenied
	kVal := uint8(8)
	nVal := uint8(10)
	payload := EncodeRegisterAckPayload(status, kVal, nVal)

	decStatus, decK, decN, err := DecodeRegisterAckPayload(payload)
	if err != nil {
		t.Fatalf("DecodeRegisterAckPayload failed: %v", err)
	}

	if decStatus != status {
		t.Errorf("Decoded status mismatch: got %d, want %d", decStatus, status)
	}

	if decK != kVal {
		t.Errorf("Decoded k mismatch: got %d, want %d", decK, kVal)
	}

	if decN != nVal {
		t.Errorf("Decoded n mismatch: got %d, want %d", decN, nVal)
	}
}

func TestAuthChallengePayload(t *testing.T) {
	randA := []byte("01234567890123456789012345678901") // 32 bytes
	t1 := int64(1716382910000000000)

	payload := EncodeAuthChallengePayload(randA, t1)

	decRandA, decT1, err := DecodeAuthChallengePayload(payload)
	if err != nil {
		t.Fatalf("DecodeAuthChallengePayload failed: %v", err)
	}

	if !bytes.Equal(decRandA, randA) {
		t.Errorf("Decoded randA mismatch: got %v, want %v", decRandA, randA)
	}

	if decT1 != t1 {
		t.Errorf("Decoded t1 mismatch: got %d, want %d", decT1, t1)
	}
}

func TestAuthResponsePayload(t *testing.T) {
	hmacVal := []byte("01234567890123456789012345678901") // 32 bytes
	t2 := int64(1716382910000000001)
	t3 := int64(1716382910000000002)

	payload := EncodeAuthResponsePayload(hmacVal, t2, t3)

	decHMAC, decT2, decT3, err := DecodeAuthResponsePayload(payload)
	if err != nil {
		t.Fatalf("DecodeAuthResponsePayload failed: %v", err)
	}

	if !bytes.Equal(decHMAC, hmacVal) {
		t.Errorf("Decoded HMAC mismatch: got %v, want %v", decHMAC, hmacVal)
	}

	if decT2 != t2 {
		t.Errorf("Decoded t2 mismatch: got %d, want %d", decT2, t2)
	}

	if decT3 != t3 {
		t.Errorf("Decoded t3 mismatch: got %d, want %d", decT3, t3)
	}
}

func TestAuthResultPayload(t *testing.T) {
	status := uint8(0)
	t4 := int64(1716382910000000003)
	k := uint8(8)
	n := uint8(10)

	payload := EncodeAuthResultPayload(status, t4, k, n)

	decStatus, decT4, decK, decN, err := DecodeAuthResultPayload(payload)
	if err != nil {
		t.Fatalf("DecodeAuthResultPayload failed: %v", err)
	}

	if decStatus != status {
		t.Errorf("Decoded status mismatch: got %d, want %d", decStatus, status)
	}

	if decT4 != t4 {
		t.Errorf("Decoded t4 mismatch: got %d, want %d", decT4, t4)
	}

	if decK != k {
		t.Errorf("Decoded k mismatch: got %d, want %d", decK, k)
	}

	if decN != n {
		t.Errorf("Decoded n mismatch: got %d, want %d", decN, n)
	}
}

