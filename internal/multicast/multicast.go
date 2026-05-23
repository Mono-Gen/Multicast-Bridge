package multicast

import (
	"fmt"
	"net"
	"strings"

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
func ListenMulticast(network string, ifi *net.Interface, mcastAddr *net.UDPAddr) (*net.UDPConn, error) {
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

	conn, err := net.ListenMulticastUDP(network, ifi, mcastAddr)
	if err != nil {
		logger.Errorf(401, "Failed to join multicast group %s on interface %s: %v", mcastAddr.String(), ifi.Name, err)
		return nil, fmt.Errorf("[401] IGMP Join failed: %w", err)
	}

	return conn, nil
}
