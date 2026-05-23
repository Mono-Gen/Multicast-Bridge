package cmd

import (
	"os"
	"runtime"
	"strconv"
	
	"multicast-bridge/internal/logger"
)

var pidFilePath = "/var/run/multicast-bridge.pid"

func writePIDFile() {
	if runtime.GOOS != "linux" {
		return
	}

	pid := os.Getpid()
	pidStr := strconv.Itoa(pid)

	err := os.WriteFile(pidFilePath, []byte(pidStr), 0644)
	if err != nil {
		logger.Warnf(0, "Failed to write PID file to %s: %v. Retrying in current directory...", pidFilePath, err)
		pidFilePath = "multicast-bridge.pid"
		err = os.WriteFile(pidFilePath, []byte(pidStr), 0644)
		if err != nil {
			logger.Warnf(0, "Failed to write fallback PID file: %v. Continuing without PID file.", err)
			pidFilePath = ""
			return
		}
	}
	logger.Infof("Successfully wrote PID file: %s (PID: %d)", pidFilePath, pid)
}

func removePIDFile() {
	if runtime.GOOS != "linux" || pidFilePath == "" {
		return
	}

	err := os.Remove(pidFilePath)
	if err != nil {
		logger.Warnf(0, "Failed to remove PID file %s: %v", pidFilePath, err)
	} else {
		logger.Infof("Successfully removed PID file: %s", pidFilePath)
	}
}
