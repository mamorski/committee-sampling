package common

import pb "github.com/mamorski/committee-sampling/pkg/proto"

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
}
