package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"time"
)

func main() {
	sizeFlag := flag.Int("size", 100, "Packet payload size")
	flag.Parse()

	maddr, err := net.ResolveUDPAddr("udp4", "239.0.0.1:5004")
	if err != nil {
		fmt.Printf("Failed to resolve multicast addr: %v\n", err)
		os.Exit(1)
	}

	conn, err := net.DialUDP("udp4", nil, maddr)
	if err != nil {
		fmt.Printf("Failed to dial: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	payload := make([]byte, *sizeFlag)
	for i := range payload {
		payload[i] = 'A'
	}
	// Add unique text at start
	copy(payload, []byte(fmt.Sprintf("TEST-MCAST-PAYLOAD-SIZE-%d:", *sizeFlag)))

	fmt.Printf("Sending multicast packet of size %d to 239.0.0.1:5004...\n", *sizeFlag)
	_, err = conn.Write(payload)
	if err != nil {
		fmt.Printf("Failed to write: %v\n", err)
		os.Exit(1)
	}

	// Wait a bit to let it propagate
	time.Sleep(200 * time.Millisecond)
	fmt.Println("Sent successfully.")
}
