package fec

import (
	"fmt"
	"strings"
	"sync"
)

// VirtualGroupState represents the virtual simulation state of an FEC group.
type VirtualGroupState struct {
	RecvCount int
	Received  []bool
	Evaluated bool
}

// VirtualFEC simulates a single FEC parameter combination (k, n).
type VirtualFEC struct {
	k, n             int
	groups           map[uint32]*VirtualGroupState
	recoveredPackets int64
	lostPackets      int64
	deliveredPackets int64
	maxGroupHistory  uint32
	lastGroupID      uint32
}

// NewVirtualFEC creates a new VirtualFEC simulator.
func NewVirtualFEC(k, n int) *VirtualFEC {
	return &VirtualFEC{
		k:               k,
		n:               n,
		groups:          make(map[uint32]*VirtualGroupState),
		maxGroupHistory: 100,
	}
}

// RecordPacket records a packet reception or loss events.
func (vf *VirtualFEC) RecordPacket(seqNum uint32, isLost bool) {
	n := uint32(vf.n)
	groupID := seqNum / n
	index := int(seqNum % n)

	g, exists := vf.groups[groupID]
	if !exists {
		g = &VirtualGroupState{
			Received: make([]bool, vf.n),
		}
		vf.groups[groupID] = g

		// Clean up very old groups to prevent memory leaks
		if groupID > vf.lastGroupID {
			vf.lastGroupID = groupID
			vf.cleanupOldGroups(groupID)
		}
	}

	if !g.Received[index] {
		g.Received[index] = !isLost
		if !isLost {
			g.RecvCount++
		}
	}
}

func (vf *VirtualFEC) cleanupOldGroups(currentGroupID uint32) {
	cutoff := uint32(0)
	if currentGroupID > vf.maxGroupHistory {
		cutoff = currentGroupID - vf.maxGroupHistory
	}

	for id, g := range vf.groups {
		if id < cutoff && !g.Evaluated {
			vf.finalizeGroup(g)
			delete(vf.groups, id)
		}
	}
}

func (vf *VirtualFEC) finalizeGroup(g *VirtualGroupState) {
	g.Evaluated = true

	groupDelivered := 0
	groupLost := 0

	for i := 0; i < vf.k; i++ {
		if g.Received[i] {
			groupDelivered++
		} else {
			groupLost++
		}
	}

	vf.deliveredPackets += int64(groupDelivered)

	if groupLost > 0 {
		// If there is a missing data packet, check if RS recovery is possible
		// If total received blocks in the group is at least K, it can be fully recovered
		if g.RecvCount >= vf.k {
			vf.recoveredPackets += int64(groupLost)
		} else {
			vf.lostPackets += int64(groupLost)
		}
	}
}

// FinalizeAll processes remaining active groups in the map.
func (vf *VirtualFEC) FinalizeAll() {
	for _, g := range vf.groups {
		if !g.Evaluated {
			vf.finalizeGroup(g)
		}
	}
}

// Metrics calculates overhead, recovery rate, and final loss rate.
func (vf *VirtualFEC) Metrics() (overhead float64, recoveryRate float64, finalLossRate float64) {
	overhead = float64(vf.n-vf.k) / float64(vf.k) * 100.0

	totalNeededRecovery := vf.recoveredPackets + vf.lostPackets
	if totalNeededRecovery == 0 {
		recoveryRate = 100.0
	} else {
		recoveryRate = float64(vf.recoveredPackets) / float64(totalNeededRecovery) * 100.0
	}

	totalDataPackets := vf.deliveredPackets + vf.recoveredPackets + vf.lostPackets
	if totalDataPackets == 0 {
		finalLossRate = 0.0
	} else {
		finalLossRate = float64(vf.lostPackets) / float64(totalDataPackets) * 100.0
	}

	return
}

// FECAnalyzer manages multiple VirtualFEC instances to evaluate optimal parameters.
type FECAnalyzer struct {
	mu      sync.Mutex
	sims    []*VirtualFEC
	enabled bool
	lastSeq uint32
	hasSeq  bool
}

// NewFECAnalyzer creates a new FECAnalyzer.
func NewFECAnalyzer(enabled bool) *FECAnalyzer {
	if !enabled {
		return &FECAnalyzer{enabled: false}
	}
	return &FECAnalyzer{
		enabled: true,
		sims: []*VirtualFEC{
			NewVirtualFEC(8, 9),   // 12.5% overhead
			NewVirtualFEC(8, 10),  // 25.0% overhead (Standard)
			NewVirtualFEC(16, 20), // 25.0% overhead (Burst-resistant, Higher Latency)
			NewVirtualFEC(4, 6),   // 50.0% overhead (Low Latency, High-loss resistant)
		},
	}
}

// RecordPacket updates the passive simulation matrix upon packet sequence numbers.
func (fa *FECAnalyzer) RecordPacket(seqNum uint32) {
	if !fa.enabled {
		return
	}
	fa.mu.Lock()
	defer fa.mu.Unlock()

	if !fa.hasSeq {
		fa.lastSeq = seqNum
		fa.hasSeq = true
		for _, sim := range fa.sims {
			sim.RecordPacket(seqNum, false)
		}
		return
	}

	// Detect packet drops (gaps in sequence numbers)
	if seqNum > fa.lastSeq+1 {
		lostCount := seqNum - fa.lastSeq - 1
		// Skip abnormal jumps to avoid corrupting simulation history (e.g. initial re-connections)
		if lostCount < 1000 {
			for s := fa.lastSeq + 1; s < seqNum; s++ {
				for _, sim := range fa.sims {
					sim.RecordPacket(s, true)
				}
			}
		}
	}

	for _, sim := range fa.sims {
		sim.RecordPacket(seqNum, false)
	}
	fa.lastSeq = seqNum
}

// DumpReport generates a formatted table of all simulation parameters and recovery results.
func (fa *FECAnalyzer) DumpReport() string {
	if !fa.enabled {
		return ""
	}
	fa.mu.Lock()
	defer fa.mu.Unlock()

	// Finalize all remaining groups
	for _, sim := range fa.sims {
		sim.FinalizeAll()
	}

	var sb strings.Builder
	sb.WriteString("\n=== FEC Optimization Analysis Matrix ===\n")
	sb.WriteString("FEC (k, n)   Overhead %   Recovery Rate   Final Loss %   Verdict\n")
	sb.WriteString("-----------------------------------------------------------------\n")

	bestK, bestN := 0, 0
	var bestOverhead float64 = 999.0

	for _, sim := range fa.sims {
		overhead, recoveryRate, finalLossRate := sim.Metrics()

		verdict := "POOR (Loss remains)"
		if finalLossRate == 0.0 {
			verdict = "EXCELLENT (100% Recovered)"
			if overhead < bestOverhead {
				bestOverhead = overhead
				bestK = sim.k
				bestN = sim.n
			}
		} else if finalLossRate < 0.1 {
			verdict = "GOOD (Near 100% Recovery)"
			if overhead < bestOverhead {
				bestOverhead = overhead
				bestK = sim.k
				bestN = sim.n
			}
		}

		sb.WriteString(fmt.Sprintf("(%-2d, %-2d)      %-10.1f%%   %-13.2f%%   %-12.4f%%   %s\n",
			sim.k, sim.n, overhead, recoveryRate, finalLossRate, verdict))
	}
	sb.WriteString("-----------------------------------------------------------------\n")
	if bestK > 0 {
		sb.WriteString(fmt.Sprintf("Recommended Parameters: FEC (k=%d, n=%d) at %.1f%% overhead\n", bestK, bestN, bestOverhead))
	} else {
		sb.WriteString("Recommended Parameters: None of the tested FEC setups could fully recover. Consider FEC (4, 6) or wired connections.\n")
	}
	sb.WriteString("========================================")

	return sb.String()
}
