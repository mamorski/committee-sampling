package gce

import (
	"crypto/sha256"
	"errors"

	"github.com/mamorski/committee-sampling/internal/common"
)

type VRF interface {
	Generate(lambda int) (sk []byte, vk []byte, err error)
	Eval(message, sk []byte) (output []byte, proof []byte, err error)
}

type VDF interface {
	Setup(lambda, delta int) (vdfVk []byte, err error)
	Eval(message, vk []byte, delay int) (phiVDF []byte, piVDF []byte, err error)
}

type RBExp interface {
	Generate(sid string, vk []byte) (challenge []byte, proof *common.RBExpProof, err error)
	Verify(sid string,
		vk,
		ch []byte,
		proof *common.RBExpProof,
		auxKey *common.AuxKey,
		auxLocal float64) ([]*common.RBExpOutput, error)
}

// LocalState holds the party’s local state after the initialization phase.
type LocalState struct {
	VRFSecret []byte
	VRFPublic []byte

	Challenge  []byte
	RBExpProof *common.RBExpProof

	PhiVDF []byte
	PiVDF  []byte
}

// CommitteeOutput is the final output for an elected candidate: a pair (id||vk, grade).
type CommitteeOutput struct {
	IdentityData []byte
	Grade        int
}

// HashData concatenates all input byte slices and returns their SHA-256 hash.
func HashData(data ...[]byte) []byte {
	h := sha256.New()
	for _, d := range data {
		h.Write(d)
	}
	return h.Sum(nil)
}

// Initialize executes the initialization phase of GCE.
// Inputs:
//   - id: the party’s identifier (e.g. its network or application id).
//   - sid: the session identifier.
//   - vrf: an implementation of the VRF interface.
//   - rbexp: an implementation of the RBExp interface.
//   - vdf: an implementation of the VDF interface.
//   - vdfVk: the verification key for the VDF.
//   - delay: the VDF delay parameter.
//
// Returns the party’s LocalState or an error.
func Initialize(id string, sid string, vrf VRF, rbexp RBExp, vdf VDF, delay int, lambda int) (*LocalState, error) {
	// Step 1: Sample a VRF key pair.
	sk, vk, err := vrf.Generate(lambda)
	if err != nil {
		return nil, err
	}

	// Step 2: Run the resource-bounded ex-post generation.
	identityData := append([]byte(id), vk...)
	challenge, rbExpProof, err := rbexp.Generate(sid, identityData)
	if err != nil {
		return nil, err
	}

	// Step 3: Start the VDF evaluation.
	vdfVk, err := vdf.Setup(lambda, delay)
	if err != nil {
		return nil, err
	}
	vdfInput := HashData([]byte(id), vk, challenge)
	phiVDF, piVDF, err := vdf.Eval(vdfInput, vdfVk, delay)
	if err != nil {
		return nil, err
	}

	state := &LocalState{
		VRFSecret:  sk,
		VRFPublic:  vk,
		Challenge:  challenge,
		RBExpProof: rbExpProof,
		PhiVDF:     phiVDF,
		PiVDF:      piVDF,
	}
	return state, nil
}

// CommitteeElection executes the committee-election phase of ΠGCE.
// Inputs:
//   - id: the party’s identifier.
//   - sid: the session identifier.
//   - state: the LocalState output from the initialization phase.
//   - weight: the party’s local estimation (W_i) of the expected total RP weight.
//   - vrf: an implementation of the VRF interface (for evaluation).
//   - rbexp: an implementation of the RBExp interface (for verification).
//
// Returns a slice of CommitteeOutput representing elected committee members.
func CommitteeElection(
	id string, sid string, state *LocalState, weight float64, vrf VRF, rbexp RBExp) ([]CommitteeOutput, error) {

	if state == nil || len(state.VRFSecret) == 0 {
		return nil, errors.New("invalid local state")
	}

	// Step 1: Compute the VRF evaluation.
	hashInput := HashData(state.PhiVDF, []byte(sid))
	phiVrf, piVrf, err := vrf.Eval(hashInput, state.VRFSecret)
	if err != nil {
		return nil, err
	}

	auxKey := &common.AuxKey{
		PhiVRF: phiVrf,
		PiVRF:  piVrf,
		PhiVDF: state.PhiVDF,
		PiVDF:  state.PiVDF,
	}

	// Step 2: Run RB-ExP.Verify.
	identityData := append([]byte(id), state.VRFPublic...)
	outputs, err := rbexp.Verify(sid, identityData, state.Challenge, state.RBExpProof, auxKey, weight)
	if err != nil {
		return nil, err
	}

	// Step 3: Process the outputs.
	var committee []CommitteeOutput
	for _, out := range outputs {
		committee = append(committee, CommitteeOutput{
			IdentityData: out.VK,
			Grade:        out.Grade,
		})
	}

	return committee, nil
}
