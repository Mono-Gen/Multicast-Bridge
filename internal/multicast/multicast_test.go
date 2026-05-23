package multicast

import (
	"net"
	"strings"
	"testing"
)

func TestResolveInterface(t *testing.T) {
	// 1. Test empty interface
	_, err := ResolveInterface("")
	if err == nil {
		t.Error("expected error for empty interface name, got nil")
	}

	// 2. Test invalid interface name
	_, err = ResolveInterface("non_existent_interface_xyz")
	if err == nil {
		t.Error("expected error for non existent interface, got nil")
	} else if !strings.Contains(err.Error(), "[401]") {
		t.Errorf("expected [401] error, got %v", err)
	}

	// 3. Test loopback interface resolution (by IP and possibly by name)
	// We check if "127.0.0.1" is resolved.
	ifi, err := ResolveInterface("127.0.0.1")
	if err != nil {
		// On some containers or test environments loopback may not have 127.0.0.1 binded
		// But in a standard local machine it should work. Let's log instead of failing.
		t.Logf("127.0.0.1 resolution got error (might be test env): %v", err)
	} else {
		if ifi == nil {
			t.Error("resolved interface is nil")
		} else {
			t.Logf("Successfully resolved loopback interface: name=%s, index=%d", ifi.Name, ifi.Index)
			
			// Test ResolveInterfaceIP
			ip, err := ResolveInterfaceIP(ifi)
			if err != nil {
				t.Errorf("ResolveInterfaceIP failed on resolved loopback: %v", err)
			} else {
				t.Logf("Resolved IP: %s", ip)
				if ip == "" {
					t.Error("resolved IP is empty")
				}
			}
		}
	}
}

func TestResolveInterfaceByName(t *testing.T) {
	ifaces, err := net.Interfaces()
	if err != nil {
		t.Fatalf("failed to list interfaces: %v", err)
	}
	if len(ifaces) == 0 {
		t.Skip("No network interfaces found, skipping name test")
	}

	// Pick the first available interface and try resolving by name
	targetIface := ifaces[0]
	ifi, err := ResolveInterface(targetIface.Name)
	if err != nil {
		t.Errorf("failed to resolve interface by existing name %s: %v", targetIface.Name, err)
	} else {
		if ifi.Index != targetIface.Index {
			t.Errorf("resolved interface index mismatch: expected %d, got %d", targetIface.Index, ifi.Index)
		}
	}
}
