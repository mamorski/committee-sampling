package gce

import (
	"crypto/sha256"
	"errors"

	"github.com/mamorski/committee-sampling/internal/common"
	"go.uber.org/zap"
)

type VRF interface {
	Generate(lambda int) (sk []byte, vk []byte, err error)
	Eval(message, sk []byte) (output []byte, proof []byte, err error)
}

type VDF interface {
	Eval(message, vk []byte, delay int) (phiVDF []byte, piVDF []byte, err error)
}

type RBExp interface {
	Generate(sid string, vk []byte) (challenge []byte, proof *common.RBExpProof, err error)
	Verify(sid string, vk, ch []byte, proof *common.RBExpProof, auxKey *common.AuxKey, auxLocal float64) ([]*common.CommitteeOutput, error)
}

type Election struct {
	logger *zap.Logger
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

// HashData concatenates all input byte slices and returns their SHA-256 hash.
func HashData(data ...[]byte) []byte {
	h := sha256.New()
	for _, d := range data {
		h.Write(d)
	}
	return h.Sum(nil)
}

// New creates a new instance of the Election struct with a logger.
func New(logger *zap.Logger) *Election {
	return &Election{
		logger: logger.Named("election"),
	}
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
func (e *Election) Initialize(id string, sid string, vrf VRF, rbexp RBExp, vdf VDF, delay int, lambda int) (*LocalState, error) {
	// Step 1: Sample a VRF key pair.
	sk, vk, err := vrf.Generate(lambda)
	if err != nil {
		return nil, err
	}

	// Step 2: Run the resource-bounded ex-post generation.
	challenge, rbExpProof, err := rbexp.Generate(sid, vk)
	if err != nil {
		return nil, err
	}

	vdfInput := HashData([]byte(id), vk, challenge)
	phiVDF, piVDF, err := vdf.Eval(vdfInput, vk, delay)
	e.logger.Debug("VDF eval completed",
		zap.String("node_id", id),
		zap.String("sid", sid),
		zap.Binary("vk", vk),
		zap.Binary("challenge", challenge),
		zap.Binary("phi_vdf", phiVDF),
		zap.Binary("pi_vdf", piVDF),
		zap.Binary("VDF Input", vdfInput),
	)
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
func (e *Election) CommitteeElection(
	sid string,
	state *LocalState,
	weight float64,
	vrf VRF,
	rbexp RBExp) ([]*common.CommitteeOutput, error) {

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
	outputs, err := rbexp.Verify(sid, state.VRFPublic, state.Challenge, state.RBExpProof, auxKey, weight)
	if err != nil {
		e.logger.Error("RBExp verification failed")
		return nil, err
	}

	return outputs, err
}
