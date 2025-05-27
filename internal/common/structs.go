package common

import (
	pb "github.com/mamorski/committee-sampling/pkg/proto"
)

type RBExpProof struct {
	PiRP     []byte
	SigmaExp [][][]byte
	SigmaExa [][][]byte
}

// AuxKey holds the auxiliary public values used in the RB-ExP verification.
type AuxKey struct {
	// Output and proof from the VRF evaluation in the committee-election phase.
	PhiVRF []byte
	PiVRF  []byte

	// VDF values from the initialization phase.
	PhiVDF []byte
	PiVDF  []byte
}

func (a *AuxKey) ToProto() *pb.AuxData {
	return &pb.AuxData{
		PhiVrf: a.PhiVRF,
		PiVrf:  a.PiVRF,
		PhiVdf: a.PhiVDF,
		PiVdf:  a.PiVDF,
	}
}

// RBExpOutput represents one output element returned by RBExp.Ver.
// Each output corresponds to a candidate (or committee member) along with an associated grade.
type RBExpOutput struct {
	SID       string
	VK        []byte
	Grade     int
	AuxTag    *AuxTag
	Challenge []byte
}

type AuxTag struct {
	PiRP   []byte
	AuxKey *AuxKey
}

type O struct {
	VK        []byte
	Challenge []byte
	Aux       *AuxTag
	Grade     int
}

type FSigmaExp struct {
	Challenge []byte
	Sigma     [][][]byte
}

type FSigmaRBExp struct {
	PiRP     []byte
	SigmaExp [][][]byte
	SigmaExa [][][]byte
}

type Key struct {
	VK string
	Ch string
}
