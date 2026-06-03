//go:build !windows

package multicast

import (
	"fmt"
	"net"
	"syscall"
)

// SetMulticastTTL sets the IP_MULTICAST_TTL option on POSIX platforms.
func SetMulticastTTL(conn net.Conn, ttl int) error {
	sc, ok := conn.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	if !ok {
		return fmt.Errorf("connection does not support SyscallConn")
	}

	sysConn, err := sc.SyscallConn()
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

// SetDSCP sets the IP_TOS option for QoS DSCP setting on POSIX platforms.
func SetDSCP(conn net.Conn, dscp int) error {
	if dscp < 0 || dscp > 63 {
		return fmt.Errorf("invalid DSCP value: %d (must be 0-63)", dscp)
	}
	tos := dscp << 2

	sc, ok := conn.(interface {
		SyscallConn() (syscall.RawConn, error)
	})
	if !ok {
		return fmt.Errorf("connection does not support SyscallConn")
	}

	sysConn, err := sc.SyscallConn()
	if err != nil {
		return fmt.Errorf("failed to get raw connection: %w", err)
	}

	var setErr error
	err = sysConn.Control(func(fd uintptr) {
		setErr = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_IP, syscall.IP_TOS, tos)
	})

	if err != nil {
		return err
	}
	return setErr
}

