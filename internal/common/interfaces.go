package common

import (
	"time"

	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

type FilterF func(string, string, []byte, []byte, *pb.AuxKeyMessage) bool

type FilterTagF func(string, string, []byte, []byte, *pb.Aux) bool

type GradeFunc func(string, []byte, []byte, *pb.AuxKeyMessage, float64) int

// Step represents a synchronization step in the protocol
type Step string

const (
	GraphDiscovery Step = "GraphDiscovery"
	Network        Step = "Network"
	ExPostMDAG     Step = "ExPostMDAG"
	ExAnteMDAG     Step = "ExAnteMDAG"
	ExPostVerify   Step = "ExPostVerify"
	ExAnteVerify   Step = "ExAnteVerify"
)

// Synchronizer defines the interface for round synchronization across protocol modules
type Synchronizer interface {
	WaitForRound(step Step, round int) (<-chan struct{}, error)
	// TotalRounds returns the number of scheduled ticks for a step.
	TotalRounds(step Step) (int, error)
}

// ByteSummary is the end-of-run application-vs-overhead byte breakdown for a node.
type ByteSummary struct {
	AppIn       int64
	AppOut      int64
	OverheadIn  int64
	OverheadOut int64
	TotalIn     int64
	TotalOut    int64
}

// StatsRecorder is the sink every protocol/component reports statistics to. The
// stats daemon implements it; protocols hold this interface (never the daemon)
// so they stay decoupled and testable. All methods must be safe for concurrent
// use and non-blocking enough for hot message paths.
type StatsRecorder interface {
	// RecordReceived counts a received message for (proto, step, round) and
	// stamps its arrival time (used for arrival-lag and late-message detection).
	RecordReceived(proto string, step Step, round int, arrival time.Time)
	// RecordValid counts a message that passed all validation for (proto, step, round).
	RecordValid(proto string, step Step, round int)
	// RecordBytesPerRound records the per-protocol byte delta accrued in a round window.
	RecordBytesPerRound(step Step, round int, protocolID string, inDelta, outDelta int64)
	// RecordBytesFinal records the end-of-run byte totals and per-protocol breakdown.
	RecordBytesFinal(summary ByteSummary, perProtocol map[string][2]int64)
	// RecordNeighbors records this node's final neighbor list.
	RecordNeighbors(neighbors []string)
	// RecordCommittee records the elected committee.
	RecordCommittee(members []*CommitteeOutput)
	// RecordPeerDrop records a simulated peer-drop event initiated by this node.
	RecordPeerDrop(peerID, phase string, round int)
}

// NoopRecorder is a StatsRecorder that discards everything. Constructors
// substitute it when no daemon is injected (e.g. unit tests) so hot paths can
// call the recorder unconditionally without nil checks.
type NoopRecorder struct{}

func (NoopRecorder) RecordReceived(string, Step, int, time.Time)        {}
func (NoopRecorder) RecordValid(string, Step, int)                      {}
func (NoopRecorder) RecordBytesPerRound(Step, int, string, int64, int64) {}
func (NoopRecorder) RecordBytesFinal(ByteSummary, map[string][2]int64)  {}
func (NoopRecorder) RecordNeighbors([]string)                          {}
func (NoopRecorder) RecordCommittee([]*CommitteeOutput)                {}
func (NoopRecorder) RecordPeerDrop(string, string, int)                {}
