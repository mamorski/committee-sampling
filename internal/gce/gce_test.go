package gce

import (
	"bytes"
	"testing"

	"github.com/mamorski/committee-sampling/internal/common"
)

// mockVRF implements the VRF interface for testing.
type mockVRF struct{}

func (m *mockVRF) Gen(_ int) (sk []byte, vk []byte, err error) {
	return []byte("fake_sk"), []byte("fake_vk"), nil
}

func (m *mockVRF) Eval(message, _ []byte) (output []byte, proof []byte, err error) {
	// For testing, the VRF output is "phiVRF_" concatenated with the message.
	// Similarly, the proof is "piVRF_" concatenated with the message.
	return append([]byte("phiVRF_"), message...), append([]byte("piVRF_"), message...), nil
}

// mockVDF implements the VDF interface for testing.
type mockVDF struct{}

func (f *mockVDF) Setup(_, _ int) (vdfVk []byte, err error) {
	// Return a fixed VDF verification key.
	return []byte("fake_vdfVk"), nil
}

func (f *mockVDF) Eval(message, _ []byte, _ int) (phiVDF []byte, piVDF []byte, err error) {
	// Return outputs based on the input message.
	return append([]byte("phiVDF_"), message...), append([]byte("piVDF_"), message...), nil
}

// mockRBExp implements the RBExp interface for testing.
type mockRBExp struct{}

func (f *mockRBExp) Gen(sid string, vk []byte) (challenge []byte, proof *common.RBExpProof, err error) {
	// The challenge is built from the session id and the provided data.
	challenge = []byte("challenge_" + sid + "_" + string(vk))
	proof = &common.RBExpProof{
		PiRP: []byte("fake_piRP"),
		SigmaExp: [][][]byte{
			{
				{'f', 'a', 'k', 'e'},
				{'s', 'i', 'g', 'm', 'a'},
			},
		},
		SigmaExa: [][][]byte{
			{
				{'f', 'a', 'k', 'e'},
				{'s', 'i', 'g', 'm', 'a'},
				{'e', 'x', 'a'},
			},
		},
	}
	return challenge, proof, nil
}

func (f *mockRBExp) Ver(
	sid string, vk, _ []byte, _ *common.RBExpProof, auxKey *common.AuxKey, _ float64) ([]*common.RBExpOutput, error) {
	// For testing, simply return a single candidate whose identity is the provided identityData
	// and assign a fixed grade.
	output := &common.RBExpOutput{
		SID:       sid,
		VK:        vk,
		Challenge: []byte("ver_challenge"),
		AuxKey:    auxKey,
		Grade:     1,
	}
	return []*common.RBExpOutput{output}, nil
}

// TestInitialize tests the Initialize function.
func TestInitialize(t *testing.T) {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := 10

	// Instantiate fake implementations.
	vrf := &mockVRF{}
	vdf := &mockVDF{}
	rbexp := &mockRBExp{}

	state, err := Initialize(id, sid, vrf, rbexp, vdf, delay, lambda)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Check that required fields in the local state are non-empty.
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

	// Verify that the challenge has the expected prefix.
	// Identity data is computed as id || VRFPublic.
	expectedIdentityData := append([]byte(id), []byte("fake_vk")...)
	expectedPrefix := "challenge_" + sid + "_" + string(expectedIdentityData)
	if !bytes.HasPrefix(state.Challenge, []byte(expectedPrefix)) {
		t.Errorf("unexpected challenge, got %s, expected prefix %s", state.Challenge, expectedPrefix)
	}
}

// TestCommitteeElection tests the CommitteeElection function.
func TestCommitteeElection(t *testing.T) {
	id := "test_id"
	sid := "test_session"
	lambda := 128
	delay := 10
	weight := 100.0

	// Instantiate fake implementations.
	vrf := &mockVRF{}
	vdf := &mockVDF{}
	rbexp := &mockRBExp{}

	// First, run the initialization phase.
	state, err := Initialize(id, sid, vrf, rbexp, vdf, delay, lambda)
	if err != nil {
		t.Fatalf("Initialize failed: %v", err)
	}

	// Now, run the committee-election phase.
	committee, err := CommitteeElection(id, sid, state, weight, vrf, rbexp)
	if err != nil {
		t.Fatalf("CommitteeElection failed: %v", err)
	}
	if len(committee) == 0 {
		t.Fatal("CommitteeElection returned empty committee")
	}

	// Verify that the candidate's identity data is equal to id || VRFPublic.
	expectedIdentityData := append([]byte(id), state.VRFPublic...)
	if !bytes.Equal(committee[0].IdentityData, expectedIdentityData) {
		t.Errorf("Identity data mismatch: got %s, expected %s",
			committee[0].IdentityData, expectedIdentityData)
	}

	// Verify that the candidate's grade is as expected.
	if committee[0].Grade != 1 {
		t.Errorf("Candidate grade mismatch: got %d, expected %d", committee[0].Grade, 1)
	}
}

// TestCommitteeElectionNilState tests that CommitteeElection returns an error if given a nil state.
func TestCommitteeElectionNilState(t *testing.T) {
	vrf := &mockVRF{}
	rbexp := &mockRBExp{}

	_, err := CommitteeElection("test_id", "test_session", nil, 100.0, vrf, rbexp)
	if err == nil {
		t.Fatal("CommitteeElection expected to fail with nil state, but it succeeded")
	}
}
