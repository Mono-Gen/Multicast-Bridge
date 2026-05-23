//go:build !windows

package multicast

import (
	"fmt"
	"net"
	"syscall"
)

// SetMulticastTTL sets the IP_MULTICAST_TTL option on POSIX platforms.
func SetMulticastTTL(conn *net.UDPConn, ttl int) error {
	sysConn, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("failed to get raw connection: %w", err)
	}

	var setErr error
	err = sysConn.Control(func(fd uintptr) {
		setErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_MULTICAST_TTL, ttl)
	})

	if err != nil {
		return err
	}
	return setErr
}
