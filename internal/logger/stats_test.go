package logger

import (
	"testing"
	"time"
)

func TestLatencyStats_Filtering(t *testing.T) {
	l := NewLatencyStats()

	// 1. Add valid latencies
	l.Add(10 * time.Millisecond)
	l.Add(20 * time.Millisecond)
	l.Add(30 * time.Millisecond)

	// 2. Add invalid latencies (should be skipped)
	l.Add(-5 * time.Millisecond)
	l.Add(65 * time.Second) // > 1 minute

	min, max, avg, recentMin, recentMax, recentAvg := l.GetMetrics()

	if min != 10*time.Millisecond {
		t.Errorf("expected min to be 10ms, got %v", min)
	}
	if max != 30*time.Millisecond {
		t.Errorf("expected max to be 30ms, got %v", max)
	}
	if avg != 20*time.Millisecond {
		t.Errorf("expected avg to be 20ms, got %v", avg)
	}

	// Verify recent metrics (recent size should be 3)
	if recentMin != 10*time.Millisecond {
		t.Errorf("expected recentMin to be 10ms, got %v", recentMin)
	}
	if recentMax != 30*time.Millisecond {
		t.Errorf("expected recentMax to be 30ms, got %v", recentMax)
	}
	if recentAvg != 20*time.Millisecond {
		t.Errorf("expected recentAvg to be 20ms, got %v", recentAvg)
	}
}
