package control

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Control Packet Types
const (
	TypeRegister      uint8 = 0x01
	TypeRegisterAck   uint8 = 0x02
	TypeKeepAlive     uint8 = 0x03
	TypeLeave         uint8 = 0x04
	TypeDisconnect    uint8 = 0x05
	TypeAuthChallenge uint8 = 0x06
	TypeAuthResponse  uint8 = 0x07
	TypeAuthResult    uint8 = 0x08
)

// RegisterAck Status Codes
const (
	StatusSuccess uint8 = 0x00
	StatusDenied  uint8 = 0x01
)

var (
	ErrPacketTooShort = errors.New("packet too short")
	ErrInvalidType    = errors.New("invalid control packet type")
	ErrPayloadMismatch = errors.New("payload length mismatch")
)

// Packet represents a parsed control packet.
type Packet struct {
	Type    uint8
	Payload []byte
}

// ParsePacket decodes a byte array into a Packet.
func ParsePacket(data []byte) (*Packet, error) {
	if len(data) < 3 {
		return nil, ErrPacketTooShort
	}

	pType := data[0]
	if pType < TypeRegister || pType > TypeAuthResult {
		return nil, ErrInvalidType
	}

	payloadLen := binary.BigEndian.Uint16(data[1:3])
	if len(data[3:]) < int(payloadLen) {
		return nil, ErrPayloadMismatch
	}

	// Extract payload exactly matching payloadLen
	payload := data[3 : 3+payloadLen]

	return &Packet{
		Type:    pType,
		Payload: payload,
	}, nil
}

// BuildPacket encodes a packet type and payload into bytes.
func BuildPacket(pType uint8, payload []byte) []byte {
	payloadLen := len(payload)
	data := make([]byte, 3+payloadLen)
	data[0] = pType
	binary.BigEndian.PutUint16(data[1:3], uint16(payloadLen))
	copy(data[3:], payload)
	return data
}

// EncodeRegisterPayload builds payload for Register packet.
func EncodeRegisterPayload(dataPort uint16, version string, ipAddr string) []byte {
	vBytes := []byte(version)
	ipBytes := []byte(ipAddr)

	payload := make([]byte, 2+1+len(vBytes)+1+len(ipBytes))
	binary.BigEndian.PutUint16(payload[0:2], dataPort)
	
	payload[2] = uint8(len(vBytes))
	copy(payload[3:3+len(vBytes)], vBytes)

	idx := 3 + len(vBytes)
	payload[idx] = uint8(len(ipBytes))
	copy(payload[idx+1:], ipBytes)

	return payload
}

// DecodeRegisterPayload extracts fields from a Register packet payload.
func DecodeRegisterPayload(payload []byte) (dataPort uint16, version string, ipAddr string, err error) {
	if len(payload) < 4 {
		return 0, "", "", ErrPacketTooShort
	}

	dataPort = binary.BigEndian.Uint16(payload[0:2])
	
	vLen := int(payload[2])
	if len(payload) < 3+vLen+1 {
		return 0, "", "", ErrPayloadMismatch
	}
	version = string(payload[3 : 3+vLen])

	idx := 3 + vLen
	ipLen := int(payload[idx])
	if len(payload) < idx+1+ipLen {
		return 0, "", "", ErrPayloadMismatch
	}
	ipAddr = string(payload[idx+1 : idx+1+ipLen])

	return dataPort, version, ipAddr, nil
}

// EncodeRegisterAckPayload builds payload for RegisterAck packet.
func EncodeRegisterAckPayload(status uint8, k uint8, n uint8) []byte {
	return []byte{status, k, n}
}

// DecodeRegisterAckPayload extracts status, k, and n from a RegisterAck packet payload.
func DecodeRegisterAckPayload(payload []byte) (status uint8, k uint8, n uint8, err error) {
	if len(payload) < 1 {
		return 0, 0, 0, ErrPacketTooShort
	}
	status = payload[0]
	if len(payload) >= 3 {
		k = payload[1]
		n = payload[2]
	}
	return status, k, n, nil
}

// FormatPacketType returns a human readable packet type name.
func FormatPacketType(pType uint8) string {
	switch pType {
	case TypeRegister:
		return "REGISTER"
	case TypeRegisterAck:
		return "REGISTER_ACK"
	case TypeKeepAlive:
		return "KEEP_ALIVE"
	case TypeLeave:
		return "LEAVE"
	case TypeDisconnect:
		return "DISCONNECT"
	case TypeAuthChallenge:
		return "AUTH_CHALLENGE"
	case TypeAuthResponse:
		return "AUTH_RESPONSE"
	case TypeAuthResult:
		return "AUTH_RESULT"
	default:
		return fmt.Sprintf("UNKNOWN(0x%02x)", pType)
	}
}

// EncodeAuthChallengePayload builds payload for AuthChallenge packet.
func EncodeAuthChallengePayload(randA []byte, t1 int64) []byte {
	payload := make([]byte, 32+8)
	copy(payload[0:32], randA)
	binary.BigEndian.PutUint64(payload[32:40], uint64(t1))
	return payload
}

// DecodeAuthChallengePayload extracts randA and t1 from an AuthChallenge packet payload.
func DecodeAuthChallengePayload(payload []byte) (randA []byte, t1 int64, err error) {
	if len(payload) < 40 {
		return nil, 0, ErrPacketTooShort
	}
	randA = make([]byte, 32)
	copy(randA, payload[0:32])
	t1 = int64(binary.BigEndian.Uint64(payload[32:40]))
	return randA, t1, nil
}

// EncodeAuthResponsePayload builds payload for AuthResponse packet.
func EncodeAuthResponsePayload(hmacVal []byte, t2 int64, t3 int64) []byte {
	payload := make([]byte, 32+8+8)
	copy(payload[0:32], hmacVal)
	binary.BigEndian.PutUint64(payload[32:40], uint64(t2))
	binary.BigEndian.PutUint64(payload[40:48], uint64(t3))
	return payload
}

// DecodeAuthResponsePayload extracts hmacVal, t2, and t3 from an AuthResponse packet payload.
func DecodeAuthResponsePayload(payload []byte) (hmacVal []byte, t2 int64, t3 int64, err error) {
	if len(payload) < 48 {
		return nil, 0, 0, ErrPacketTooShort
	}
	hmacVal = make([]byte, 32)
	copy(hmacVal, payload[0:32])
	t2 = int64(binary.BigEndian.Uint64(payload[32:40]))
	t3 = int64(binary.BigEndian.Uint64(payload[40:48]))
	return hmacVal, t2, t3, nil
}

// EncodeAuthResultPayload builds payload for AuthResult packet.
func EncodeAuthResultPayload(status uint8, t4 int64, k uint8, n uint8) []byte {
	payload := make([]byte, 1+8+1+1)
	payload[0] = status
	binary.BigEndian.PutUint64(payload[1:9], uint64(t4))
	payload[9] = k
	payload[10] = n
	return payload
}

// DecodeAuthResultPayload extracts status, t4, k, and n from an AuthResult packet payload.
func DecodeAuthResultPayload(payload []byte) (status uint8, t4 int64, k uint8, n uint8, err error) {
	if len(payload) < 11 {
		return 0, 0, 0, 0, ErrPacketTooShort
	}
	status = payload[0]
	t4 = int64(binary.BigEndian.Uint64(payload[1:9]))
	k = payload[9]
	n = payload[10]
	return status, t4, k, n, nil
}
