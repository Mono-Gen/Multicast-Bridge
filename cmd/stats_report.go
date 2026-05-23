package cmd

import (
	"runtime"
	"time"
	
	"multicast-bridge/internal/logger"
)

func startStatsReporting(statsType string, statsInterval int) {
	if statsInterval <= 0 {
		statsInterval = 10
	}

	dumpFunc := func() {
		if statsType == "sender" {
			logger.Infof("%s", logger.GlobalSenderStats.DumpString())
		} else {
			logger.Infof("%s", logger.GlobalReceiverStats.DumpString())
			if globalFECAnalyzer != nil {
				report := globalFECAnalyzer.DumpReport()
				if report != "" {
					logger.Infof("%s", report)
				}
			}
		}
	}

	// 1. Unix/Linux 固有: SIGUSR1 によるシグナル監視起動
	startSignalMonitoring(dumpFunc)

	// 2. Windows 固有: stats_interval によるタイマー起動
	if runtime.GOOS == "windows" {
		logger.Infof("Windows detected. Starting stats periodic dump timer every %d seconds.", statsInterval)
		go func() {
			ticker := time.NewTicker(time.Duration(statsInterval) * time.Second)
			defer ticker.Stop()
			for range ticker.C {
				dumpFunc()
			}
		}()
	}
}
