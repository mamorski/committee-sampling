package common

type FilterF func(string, string, []byte, []byte, *AuxKey) bool

type FilterTagF func(string, string, []byte, []byte, *AuxTag) bool

type GradeFunc func(string, []byte, []byte, *AuxKey, float64) int

// Step represents a synchronization step in the protocol
type Step string

const (
	ExPostMDAG   Step = "ExPostMDAG"
	ExAnteMDAG   Step = "ExAnteMDAG"
	ExPostVerify Step = "ExPostVerify"
	ExAnteVerify Step = "ExAnteVerify"
)

// Synchronizer defines the interface for round synchronization across protocol modules
type Synchronizer interface {
	WaitForRound(step Step, round int) (<-chan struct{}, error)
}
