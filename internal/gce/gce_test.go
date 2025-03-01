package gce

import (
	"bytes"
	"errors"
	"strconv"
	"testing"

	"github.com/mamorski/committee-sampling/internal/common"
)

type mockVRF struct{}

func (m *mockVRF) Gen(lambda int) (sk []byte, vk []byte, err error) {
	if lambda == 0 {
		return nil, nil, errors.New("VRF gen error")
	}
	return []byte("sk_" + strconv.Itoa(lambda)), []byte("vk_" + strconv.Itoa(lambda)), nil
}

func (m *mockVRF) Eval(message, sk []byte) (output []byte, proof []byte, err error) {
	if string(sk) == "fail_secret" {
		return nil, nil, errors.New("VRF eval error")
	}
	return []byte("phiVRF_" + string(message)), []byte("piVRF_" + string(message)), nil
}

type mockVDF struct{}

func (m *mockVDF) Setup(_ int, delta int) (vdfVk []byte, err error) {
	if delta == 0 {
		return nil, errors.New("VDF setup error")
	}
	return []byte("vdfVk_" + strconv.Itoa(delta)), nil
}

func (m *mockVDF) Eval(message, _ []byte, delay int) (phiVDF []byte, piVDF []byte, err error) {
	if delay == -1 {
		return nil, nil, errors.New("VDF eval error")
	}
	return []byte("phiVDF_" + string(message)), []byte("piVDF_" + string(message)), nil
}

type mockRBExp struct{}

func (m *mockRBExp) Gen(sid string, vk []byte) (challenge []byte, proof *common.RBExpProof, err error) {
	if sid == "fail_rbexp_gen" {
		return nil, nil, errors.New("RBExp gen error")
	}
	challenge = []byte("challenge_" + sid + "_" + string(vk))
	proof = &common.RBExpProof{
		PiRP:     []byte("piRP_" + string(vk)),
		SigmaExp: [][][]byte{{[]byte("sigmaExp_" + sid)}},
		SigmaExa: [][][]byte{{[]byte("sigmaExa_" + sid)}},
	}
	return challenge, proof, nil
}

func (m *mockRBExp) Ver(sid string, vk, _ []byte, _ *common.RBExpProof, auxKey *common.AuxKey, _ float64) ([]*common.RBExpOutput, error) {
	if sid == "fail_ver" {
		return nil, errors.New("RBExp ver error")
	}
	output := &common.RBExpOutput{
		SID:       sid,
		VK:        vk,
		Challenge: []byte("ver_challenge_" + sid),
		AuxKey:    auxKey,
		Grade:     1,
	}
	return []*common.RBExpOutput{output}, nil
}

func TestInitialize_Success(t *testing.T) {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := 10

	vrf := &mockVRF{}
	vdf := &mockVDF{}
	rbexp := &mockRBExp{}

	state, err := Initialize(id, sid, vrf, rbexp, vdf, delay, lambda)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	if len(state.VRFSecret) == 0 {
		t.Error("VRFSecret is empty")
	}
	if len(state.VRFPublic) == 0 {
		t.Error("VRFPublic is empty")
	}
	if len(state.Challenge) == 0 {
		t.Error("Challenge is empty")
	}
	if state.RBExpProof == nil {
		t.Error("RBExpProof is nil")
	}
	if len(state.PhiVDF) == 0 {
		t.Error("PhiVDF is empty")
	}
	if len(state.PiVDF) == 0 {
		t.Error("PiVDF is empty")
	}

	expectedIdentityData := append([]byte(id), []byte("vk_"+strconv.Itoa(lambda))...)
	expectedPrefix := "challenge_" + sid + "_" + string(expectedIdentityData)
	if !bytes.HasPrefix(state.Challenge, []byte(expectedPrefix)) {
		t.Errorf("unexpected challenge, got %s, expected prefix %s", state.Challenge, expectedPrefix)
	}
}

func TestInitialize_ErrorVRFGen(t *testing.T) {
	id := "test_id"
	sid := "test_session"
	lambda := 0
	delay := 10

	vrf := &mockVRF{}
	rbexp := &mockRBExp{}
	vdf := &mockVDF{}

	state, err := Initialize(id, sid, vrf, rbexp, vdf, delay, lambda)
	if err == nil || err.Error() != "VRF gen error" {
		t.Errorf("Expected error 'VRF gen error', got: %v", err)
	}
	if state != nil {
		t.Error("Expected nil state on VRF gen error")
	}
}

func TestInitialize_ErrorRBExpGen(t *testing.T) {
	id := "test_id"
	sid := "fail_rbexp_gen"
	lambda := 128
	delay := 10

	vrf := &mockVRF{}
	rbexp := &mockRBExp{}
	vdf := &mockVDF{}

	state, err := Initialize(id, sid, vrf, rbexp, vdf, delay, lambda)
	if err == nil || err.Error() != "RBExp gen error" {
		t.Errorf("Expected error 'RBExp gen error', got: %v", err)
	}
	if state != nil {
		t.Error("Expected nil state on RBExp gen error")
	}
}

func TestInitialize_ErrorVDFSetup(t *testing.T) {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := 0

	vrf := &mockVRF{}
	rbexp := &mockRBExp{}
	vdf := &mockVDF{}

	state, err := Initialize(id, sid, vrf, rbexp, vdf, delay, lambda)
	if err == nil || err.Error() != "VDF setup error" {
		t.Errorf("Expected error 'VDF setup error', got: %v", err)
	}
	if state != nil {
		t.Error("Expected nil state on VDF setup error")
	}
}

func TestInitialize_ErrorVDFEval(t *testing.T) {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := -1

	vrf := &mockVRF{}
	rbexp := &mockRBExp{}
	vdf := &mockVDF{}

	state, err := Initialize(id, sid, vrf, rbexp, vdf, delay, lambda)
	if err == nil || err.Error() != "VDF eval error" {
		t.Errorf("Expected error 'VDF eval error', got: %v", err)
	}
	if state != nil {
		t.Error("Expected nil state on VDF eval error")
	}
}

func TestCommitteeElection_Success(t *testing.T) {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := 10
	weight := 100.0

	vrf := &mockVRF{}
	vdf := &mockVDF{}
	rbexp := &mockRBExp{}

	state, err := Initialize(id, sid, vrf, rbexp, vdf, delay, lambda)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	committee, err := CommitteeElection(id, sid, state, weight, vrf, rbexp)
	if err != nil {
		t.Fatalf("CommitteeElection failed: %v", err)
	}
	if len(committee) == 0 {
		t.Fatal("CommitteeElection returned empty committee")
	}

	expectedIdentityData := append([]byte(id), state.VRFPublic...)
	if !bytes.Equal(committee[0].IdentityData, expectedIdentityData) {
		t.Errorf("Identity data mismatch: got %s, expected %s", committee[0].IdentityData, expectedIdentityData)
	}
	if committee[0].Grade != 1 {
		t.Errorf("Candidate grade mismatch: got %d, expected %d", committee[0].Grade, 1)
	}
}

func TestCommitteeElection_NilState(t *testing.T) {
	vrf := &mockVRF{}
	rbexp := &mockRBExp{}

	_, err := CommitteeElection("test_id", "test_session", nil, 100.0, vrf, rbexp)
	if err == nil {
		t.Fatal("CommitteeElection expected to fail with nil state, but it succeeded")
	}
}

func TestCommitteeElection_ErrorVRFEval(t *testing.T) {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := 10
	weight := 100.0

	vrf := &mockVRF{}
	vdf := &mockVDF{}
	rbexp := &mockRBExp{}

	state, err := Initialize(id, sid, vrf, rbexp, vdf, delay, lambda)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	state.VRFSecret = []byte("fail_secret")

	_, err = CommitteeElection(id, sid, state, weight, vrf, rbexp)
	if err == nil || err.Error() != "VRF eval error" {
		t.Errorf("Expected error 'VRF eval error', got: %v", err)
	}
}

func TestCommitteeElection_ErrorRBExpVer(t *testing.T) {
	id := "test_id"
	sid := "fail_ver"
	lambda := 128
	delay := 10
	weight := 100.0

	vrf := &mockVRF{}
	vdf := &mockVDF{}
	rbexp := &mockRBExp{}

	state, err := Initialize(id, "test_session", vrf, rbexp, vdf, delay, lambda)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}
	_, err = CommitteeElection(id, sid, state, weight, vrf, rbexp)
	if err == nil || err.Error() != "RBExp ver error" {
		t.Errorf("Expected error 'RBExp ver error', got: %v", err)
	}
}
