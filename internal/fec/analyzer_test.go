package fec

import (
	"strings"
	"testing"
)

func TestFECAnalyzer_Simulation(t *testing.T) {
	// 1. Create a FECAnalyzer with enabled = true
	analyzer := NewFECAnalyzer(true)
	if !analyzer.enabled {
		t.Fatalf("FECAnalyzer should be enabled")
	}

	// We will simulate sequence numbers 0 to 99 (10 groups for N=10, 25 groups for N=4, etc.)
	// Group 0 for (8, 10): seqNum 0 to 9
	// We will drop index 2 and index 5 (2 packets lost out of 10).
	// Since 8/10 packets are received, it should be fully recovered for (8, 10).
	
	// Group 1 for (8, 10): seqNum 10 to 19
	// We will drop index 12, 13, and 15 (3 packets lost out of 10).
	// Since only 7/10 packets are received, it should NOT be recovered for (8, 10) -> permanently lost.

	for seq := uint32(0); seq < 100; seq++ {
		indexInGroup10 := seq % 10

		// Simulate group 0 (0-9) drops
		if seq < 10 {
			if indexInGroup10 == 2 || indexInGroup10 == 5 {
				// Drop packet
				continue
			}
		}

		// Simulate group 1 (10-19) drops
		if seq >= 10 && seq < 20 {
			if indexInGroup10 == 2 || indexInGroup10 == 3 || indexInGroup10 == 5 {
				// Drop packet
				continue
			}
		}

		analyzer.RecordPacket(seq)
	}

	// 2. Dump Report and verify metrics
	report := analyzer.DumpReport()
	t.Log(report)

	if !strings.Contains(report, "FEC Optimization Analysis Matrix") {
		t.Errorf("Report does not contain title")
	}

	// Find the (8, 10) simulator to verify exact calculations
	var sim8_10 *VirtualFEC
	for _, sim := range analyzer.sims {
		if sim.k == 8 && sim.n == 10 {
			sim8_10 = sim
			break
		}
	}

	if sim8_10 == nil {
		t.Fatalf("Could not find (8, 10) simulator")
	}

	sim8_10.FinalizeAll()
	_, recoveryRate, finalLossRate := sim8_10.Metrics()

	// In Group 0: 2 lost, recovered = 2.
	// In Group 1: 3 lost, recovered = 0, permanently lost = 3.
	// Overall needed recovery = 2 + 3 = 5 packets.
	// Actually recovered = 2 packets (Group 0).
	// Recovery rate = 2 / 5 = 40.0%
	expectedRecoveryRate := 40.0
	if recoveryRate != expectedRecoveryRate {
		t.Errorf("Expected recovery rate %.2f%%, got %.2f%%", expectedRecoveryRate, recoveryRate)
	}

	// In Group 0: 8 data blocks, all 8 successfully delivered/recovered.
	// In Group 1: 8 data blocks, 3 lost permanently, 5 delivered.
	// Total data blocks in other groups (2-9): 8 groups * 8 data blocks = 64 data blocks.
	// Overall total data blocks evaluated = 8 (Group 0) + 8 (Group 1) + 64 = 80 data blocks.
	// Lost data blocks = 3.
	// Expected final loss rate = 3 / 80 = 3.75%
	expectedFinalLossRate := 3.75
	if finalLossRate != expectedFinalLossRate {
		t.Errorf("Expected final loss rate %.2f%%, got %.2f%%", expectedFinalLossRate, finalLossRate)
	}
}
