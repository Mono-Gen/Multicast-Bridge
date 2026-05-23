//go:build !windows

package cmd

import (
	"os"
	"os/signal"
	"syscall"
	
	"multicast-bridge/internal/logger"
)

func startSignalMonitoring(dumpFunc func()) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGUSR1)
	go func() {
		for range sigChan {
			logger.Infof("Received SIGUSR1 signal. Dumping statistics.")
			dumpFunc()
		}
	}()
}
