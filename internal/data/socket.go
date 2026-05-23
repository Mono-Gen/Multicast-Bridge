package data

import (
	"context"
	"fmt"
	"net"
	"strings"
	"syscall"

	"multicast-bridge/internal/logger"
)

// CreateUDPListener creates a UDP socket bound to the specified address with SO_REUSEADDR and buffer sizes set.
func CreateUDPListener(network string, address string, rcvBuf int, sndBuf int) (*net.UDPConn, error) {
	lc := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var err error
			c.Control(func(fd uintptr) {
				err = setReuseAddr(fd)
			})
			return err
		},
	}

	// Try binding to port
	conn, err := lc.ListenPacket(context.Background(), network, address)
	if err != nil {
		errStr := err.Error()
		// Handle port in use (error code 203)
		if strings.Contains(errStr, "address already in use") ||
			strings.Contains(errStr, "WSAEADDRINUSE") ||
			strings.Contains(errStr, "already in use") {
			logger.Errorf(203, "Port is already in use: %s (%v)", address, err)
			return nil, fmt.Errorf("[203] port already in use: %w", err)
		}
		return nil, fmt.Errorf("failed to bind UDP socket: %w", err)
	}

	udpConn, ok := conn.(*net.UDPConn)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("socket is not a UDP connection")
	}

	// Set receive buffer size if requested
	if rcvBuf > 0 {
		if err := udpConn.SetReadBuffer(rcvBuf); err != nil {
			logger.Warnf(0, "Failed to set socket read buffer to %d: %v", rcvBuf, err)
		}
	}

	// Set send buffer size if requested
	if sndBuf > 0 {
		if err := udpConn.SetWriteBuffer(sndBuf); err != nil {
			logger.Warnf(0, "Failed to set socket write buffer to %d: %v", sndBuf, err)
		}
	}

	return udpConn, nil
}

// CreateUDPConnection creates a UDP connection socket to a target address.
func CreateUDPConnection(network string, targetAddr string, sndBuf int) (*net.UDPConn, error) {
	raddr, err := net.ResolveUDPAddr(network, targetAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve target address %s: %w", targetAddr, err)
	}

	conn, err := net.DialUDP(network, nil, raddr)
	if err != nil {
		return nil, fmt.Errorf("failed to dial target %s: %w", targetAddr, err)
	}

	if sndBuf > 0 {
		if err := conn.SetWriteBuffer(sndBuf); err != nil {
			logger.Warnf(0, "Failed to set socket send buffer to %d: %v", sndBuf, err)
		}
	}

	return conn, nil
}
