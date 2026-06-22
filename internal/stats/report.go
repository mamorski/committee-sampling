package stats

import (
	"time"

	"github.com/mamorski/committee-sampling/internal/common"
)

// Report is the per-node statistics document written as JSON at shutdown. It
// replaces the previous approach of reconstructing stats by scanning the whole
// log file: the analysis notebook loads one of these per node directly.
type Report struct {
	NodeID      string                     `json:"node_id"`
	SID         string                     `json:"sid"`
	GeneratedAt time.Time                  `json:"generated_at"`
	Protocols   map[string]*ProtocolReport `json:"protocols"`
	Bytes       ByteReport                 `json:"bytes"`
	Neighbors   []string                   `json:"neighbors"`
	Committee   []CommitteeMember          `json:"committee"`
	PeerDrops   []PeerDropReport           `json:"peer_drops"`
}

// ProtocolReport holds per-round message counts plus the synchronization and
// timing measurements for one protocol instance.
type ProtocolReport struct {
	Step          string `json:"step"`
	TotalMessages []int  `json:"total_messages"` // indexed by round
	ValidMessages []int  `json:"valid_messages"` // indexed by round
	// LateMessages counts messages whose round was below the protocol's current
	// round on arrival (a synchronization issue). MaxLateness is the largest
	// such gap, LateByRound the count per message round.
	LateMessages int         `json:"late_messages"`
	MaxLateness  int         `json:"max_lateness"`
	LateByRound  map[int]int `json:"late_by_round,omitempty"`
	// ArrivalLag measures, per round, how long after the round's start tick
	// messages arrived (feeds WAN round-length estimates).
	ArrivalLag []RoundLag `json:"arrival_lag"`
}

// RoundLag is the arrival-lag distribution for a single round, in nanoseconds.
type RoundLag struct {
	Round  int   `json:"round"`
	Count  int   `json:"count"`
	MeanNs int64 `json:"mean_ns"`
	MaxNs  int64 `json:"max_ns"`
	LastNs int64 `json:"last_ns"`
}

// ByteReport is the communication-volume breakdown for the node.
type ByteReport struct {
	Summary    common.ByteSummary    `json:"summary"`
	ByProtocol map[string]ByteCounts `json:"by_protocol"`
	PerRound   []ByteRound           `json:"per_round"`
}

// ByteCounts is cumulative in/out bytes for one protocol.
type ByteCounts struct {
	In  int64 `json:"in"`
	Out int64 `json:"out"`
}

// ByteRound is the per-protocol byte delta accrued within one round window.
type ByteRound struct {
	Step       string `json:"step"`
	Round      int    `json:"round"`
	ProtocolID string `json:"protocol_id"`
	InDelta    int64  `json:"in_delta"`
	OutDelta   int64  `json:"out_delta"`
}

// CommitteeMember is one elected committee member.
type CommitteeMember struct {
	ID    string `json:"id"`
	Grade int    `json:"grade"`
}

// PeerDropReport is one simulated peer-drop event this node initiated.
type PeerDropReport struct {
	PeerID string `json:"peer_id"`
	Phase  string `json:"phase"`
	Round  int    `json:"round"`
}
