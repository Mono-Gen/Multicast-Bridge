package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"multicast-bridge/cmd"
	"multicast-bridge/internal/logger"
)

const Version = "0.9.0"

func main() {
	// Parse global version flag first
	versionFlag := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Printf("multicast-bridge version %s\n", Version)
		os.Exit(0)
	}

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(202)
	}

	subcommand := args[0]
	subArgs := args[1:]

	// Set up signal channel for Graceful Shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Clean up goroutine for Graceful Shutdown
	go func() {
		sig := <-sigChan
		logger.Warnf(0, "Received signal %v. Initiating graceful shutdown...", sig)

		// Phase 1 graceful shutdown steps:
		// 1. Disconnect notification to receiver (Phase 4)
		// 2. IGMP Leave (Phase 3)
				// 3. Logger Flush & Close (Done below)
		logger.Infof("Flushing logs and closing sockets...")
		cmd.CleanUpSender()
		cmd.CleanUpRecv()
		logger.Close()

		// 4. Delete PID file (Linux only, optional in Phase 1 skeleton)
		// 5. Exit
		os.Exit(0)
	}()

	switch subcommand {
	case "send":
		cmd.ExecuteSend(subArgs, Version)
	case "recv":
		cmd.ExecuteRecv(subArgs, Version)
	default:
		fmt.Printf("Unknown subcommand: %s\n", subcommand)
		printUsage()
		os.Exit(202)
	}

	// Keep the main goroutine alive to wait for signals (simulating process run)
	logger.Infof("Service is running. Press Ctrl+C to stop.")
	select {}
}

func printUsage() {
	fmt.Println("Usage: multicast-bridge [--version] <command> [arguments]")
	fmt.Println("Commands:")
	fmt.Println("  send    Start sender daemon")
	fmt.Println("  recv    Start receiver daemon")
	fmt.Println("\nUse 'multicast-bridge <command> --help' for more information on a command.")
}
