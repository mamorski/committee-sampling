package common

import (
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
// App*/Overhead*/Total* are libp2p wire bytes (App* = app-protocol streams,
// Overhead* = discovery/connection protocols). Payload*/Envelope* are summed across
// app-protocol streams only: Payload* is the protocol's own message size, Envelope*
// the marshaled ProtocolMessage (payload + signature + MessageData). Thus
// Envelope-Payload = app wrapping overhead, App(wire)-Envelope = libp2p stream framing.
type ByteSummary struct {
	AppIn       int64 `json:"app_in"`
	AppOut      int64 `json:"app_out"`
	OverheadIn  int64 `json:"overhead_in"`
	OverheadOut int64 `json:"overhead_out"`
	TotalIn     int64 `json:"total_in"`
	TotalOut    int64 `json:"total_out"`
	PayloadIn   int64 `json:"payload_in"`
	PayloadOut  int64 `json:"payload_out"`
	EnvelopeIn  int64 `json:"envelope_in"`
	EnvelopeOut int64 `json:"envelope_out"`
}

// RoundByteDelta carries one protocol's byte deltas accrued within a round window.
// Wire* are libp2p BandwidthCounter deltas; Env* are marshaled ProtocolMessage sizes;
// Payload* are the protocol's own message sizes. Per direction Payload <= Env <= Wire.
type RoundByteDelta struct {
	WireIn, WireOut       int64
	EnvIn, EnvOut         int64
	PayloadIn, PayloadOut int64
}

// ProtoByteTotals is the cumulative wire/envelope/payload totals for one protocol.
type ProtoByteTotals struct {
	WireIn, WireOut       int64
	EnvIn, EnvOut         int64
	PayloadIn, PayloadOut int64
}

// StatsRecorder is the sink every protocol/component reports statistics to. The
// stats daemon implements it; protocols hold this interface (never the daemon)
// so they stay decoupled and testable. All methods must be safe for concurrent
// use and non-blocking enough for hot message paths.
type StatsRecorder interface {
	// RecordRoundStats flushes the pre-aggregated counters for one round.
	// Protocols accumulate these locally (under their own mutex) and call once
	// per round, reducing stats-daemon lock acquisitions from O(messages) to
	// O(rounds).
	RecordRoundStats(proto string, step Step, round int,
		total, valid, lateCount, maxLateness int,
		lagSumNs, lagMaxNs, lagLastNs int64, lagCount int)
	// RecordBytesPerRound records the per-protocol byte delta accrued in a round window.
	RecordBytesPerRound(step Step, round int, protocolID string, d RoundByteDelta)
	// RecordBytesFinal records the end-of-run byte totals and per-protocol breakdown.
	RecordBytesFinal(summary ByteSummary, perProtocol map[string]ProtoByteTotals)
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

func (NoopRecorder) RecordRoundStats(string, Step, int, int, int, int, int, int64, int64, int64, int) {
}
func (NoopRecorder) RecordBytesPerRound(Step, int, string, RoundByteDelta)        {}
func (NoopRecorder) RecordBytesFinal(ByteSummary, map[string]ProtoByteTotals)     {}
func (NoopRecorder) RecordNeighbors([]string)                                     {}
func (NoopRecorder) RecordCommittee([]*CommitteeOutput)                           {}
func (NoopRecorder) RecordPeerDrop(string, string, int)                           {}
