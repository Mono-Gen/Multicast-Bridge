package multicast

import (
	"fmt"
	"net"
	"strings"

	"golang.org/x/net/ipv4"
	"multicast-bridge/internal/logger"
)

// ResolveInterface finds the network interface based on name (Linux) or IP address (Windows).
func ResolveInterface(ifaceNameOrIP string) (*net.Interface, error) {
	if ifaceNameOrIP == "" {
		return nil, fmt.Errorf("interface name or IP is empty")
	}
	if strings.EqualFold(ifaceNameOrIP, "any") || ifaceNameOrIP == "0.0.0.0" {
		return nil, nil // Return nil to let the OS choose the interface automatically
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, fmt.Errorf("failed to list network interfaces: %w", err)
	}

	// Try checking by name and by IP addresses
	for _, ifi := range ifaces {
		// Check by Name
		if strings.EqualFold(ifi.Name, ifaceNameOrIP) {
			return &ifi, nil
		}

		// Check by IP addresses associated with the interface
		addrs, err := ifi.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			ipNet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ipStr := ipNet.IP.String()
			if ipStr == ifaceNameOrIP {
				return &ifi, nil
			}
		}
	}

	return nil, fmt.Errorf("[401] interface not found: %s", ifaceNameOrIP)
}

// ResolveInterfaceIP resolves the first IPv4 address associated with a given network interface.
func ResolveInterfaceIP(ifi *net.Interface) (string, error) {
	if ifi == nil {
		return "0.0.0.0", nil
	}
	addrs, err := ifi.Addrs()
	if err != nil {
		return "", fmt.Errorf("failed to get interface addresses: %w", err)
	}

	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok {
			continue
		}
		ipv4 := ipNet.IP.To4()
		if ipv4 != nil {
			return ipv4.String(), nil
		}
	}

	return "", fmt.Errorf("no IPv4 address associated with interface %s", ifi.Name)
}

// ListenMulticast creates a UDP socket bound to the multicast address and joins the multicast group.
// If sourceIPStr is provided (not empty), it will join using Source-Specific Multicast (SSM / IGMPv3).
func ListenMulticast(network string, ifi *net.Interface, mcastAddr *net.UDPAddr, sourceIPStr string) (*net.UDPConn, error) {
	if mcastAddr == nil || mcastAddr.IP == nil {
		return nil, fmt.Errorf("invalid nil multicast address")
	}

	// Fallback to unicast UDP listener if the address is not a multicast address (useful for local testing)
	if !mcastAddr.IP.IsMulticast() {
		logger.Infof("Address %s is not multicast. Falling back to normal UDP listener (Test/Debug mode)...", mcastAddr.String())
		conn, err := net.ListenUDP(network, &net.UDPAddr{IP: net.IPv4zero, Port: mcastAddr.Port})
		if err != nil {
			return nil, fmt.Errorf("unicast fallback listener failed: %w", err)
		}
		return conn, nil
	}

	// SSM (Source-Specific Multicast) Join if source IP is specified
	if sourceIPStr != "" {
		srcIP := net.ParseIP(sourceIPStr)
		if srcIP == nil {
			return nil, fmt.Errorf("invalid SSM source IP format: %s", sourceIPStr)
		}
		// Validate that the source IP is a proper unicast address
		if srcIP.IsMulticast() {
			return nil, fmt.Errorf("SSM source IP must be unicast, got multicast address: %s", sourceIPStr)
		}
		if srcIP.IsUnspecified() {
			return nil, fmt.Errorf("SSM source IP must be a specific unicast address, got unspecified (0.0.0.0): %s", sourceIPStr)
		}
		if srcIP.IsLoopback() {
			logger.Warnf(0, "SSM source IP %s is a loopback address; this is only valid for local testing", sourceIPStr)
		}

		logger.Infof("Joining SSM (Source-Specific Multicast) Group: %s from Source: %s", mcastAddr.String(), srcIP.String())

		// Create a standard UDP connection bound to the port
		c, err := net.ListenUDP(network, &net.UDPAddr{IP: net.IPv4zero, Port: mcastAddr.Port})
		if err != nil {
			return nil, fmt.Errorf("failed to bind UDP socket for SSM: %w", err)
		}

		p := ipv4.NewPacketConn(c)
		err = p.JoinSourceSpecificGroup(ifi, mcastAddr, &net.UDPAddr{IP: srcIP})
		if err != nil {
			c.Close()
			logger.Errorf(401, "Failed to join SSM group %s from source %s: %v", mcastAddr.String(), srcIP.String(), err)
			return nil, fmt.Errorf("[401] SSM IGMP Join failed: %w", err)
		}

		return c, nil
	}

	// Default ASM (Any-Source Multicast) Join
	conn, err := net.ListenMulticastUDP(network, ifi, mcastAddr)
	if err != nil {
		var ifiName string
		if ifi != nil {
			ifiName = ifi.Name
		} else {
			ifiName = "default"
		}
		logger.Errorf(401, "Failed to join multicast group %s on interface %s: %v", mcastAddr.String(), ifiName, err)
		return nil, fmt.Errorf("[401] IGMP Join failed: %w", err)
	}

	return conn, nil
}
