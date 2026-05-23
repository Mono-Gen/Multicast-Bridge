package data

import (
	"encoding/binary"
	"fmt"
)

// Header version constants
const (
	MajorVersion = 0
	MinorVersion = 8
	// CurrentHeaderVersion combines major (upper 4 bits) and minor (lower 4 bits)
	CurrentHeaderVersion = (MajorVersion << 4) | MinorVersion
)

// HeaderSize is the fixed size of the custom encapsulation header (17 bytes)
const HeaderSize = 17

// EncapsulatedHeader represents the 17-byte custom protocol header
type EncapsulatedHeader struct {
	Version    uint8  // [0]: Major (upper 4 bits), Minor (lower 4 bits)
	SeqNum     uint32 // [1-4]: Sequence number (big endian)
	Timestamp  int64  // [5-12]: Unix timestamp in nanoseconds (big endian)
	PayloadLen uint16 // [13-14]: Original packet length (big endian)
	FECInfo    uint16 // [15-16]: FEC flags & Group info (big endian)
}

// Serialize serializes the header into a 17-byte buffer
func (h *EncapsulatedHeader) Serialize() []byte {
	buf := make([]byte, HeaderSize)
	buf[0] = h.Version
	binary.BigEndian.PutUint32(buf[1:5], h.SeqNum)
	binary.BigEndian.PutUint64(buf[5:13], uint64(h.Timestamp))
	binary.BigEndian.PutUint16(buf[13:15], h.PayloadLen)
	binary.BigEndian.PutUint16(buf[15:17], h.FECInfo)
	return buf
}

// DeserializeHeader parses a 17-byte buffer into an EncapsulatedHeader
func DeserializeHeader(buf []byte) (*EncapsulatedHeader, error) {
	if len(buf) < HeaderSize {
		return nil, fmt.Errorf("buffer too short: %d < %d", len(buf), HeaderSize)
	}

	h := &EncapsulatedHeader{
		Version:    buf[0],
		SeqNum:     binary.BigEndian.Uint32(buf[1:5]),
		Timestamp:  int64(binary.BigEndian.Uint64(buf[5:13])),
		PayloadLen: binary.BigEndian.Uint16(buf[13:15]),
		FECInfo:    binary.BigEndian.Uint16(buf[15:17]),
	}
	return h, nil
}
