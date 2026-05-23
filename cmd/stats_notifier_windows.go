//go:build windows

package cmd

func startSignalMonitoring(dumpFunc func()) {
	// SIGUSR1 is not supported on Windows.
}
