package common

type RBExpProof struct {
	PiRP     []byte
	SigmaExp [][][]byte
	SigmaExa [][][]byte
}

type AuxKey struct {
	PhiVRF []byte
	PiVRF  []byte
	PhiVDF []byte
	PiVDF  []byte
}

type RBExpOutput struct {
	SID       string
	VK        []byte
	Grade     int
	AuxKey    *AuxKey
	Challenge []byte
}

type AuxTag struct {
	PiRP   []byte
	AuxKey *AuxKey
}

type O struct {
	VK        []byte
	Challenge []byte
	Aux       *AuxKey
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
