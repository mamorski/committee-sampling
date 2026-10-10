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
